"""
Nodus Edge Processing Pipeline

Core pipeline that processes SDRTrunk recordings:
1. Parse recording metadata
2. Find associated event logs (rich P25 data)
3. Transcribe audio (optional)
4. Build TranscriptSegment.v1
5. Emit to output directory
"""

import re
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any
from uuid import uuid4

import structlog

from . import __version__
from .config import settings
from .hallucination_filter import evaluate_transcription, is_whisper_hallucination
from .transcription.audit_log import audit_log
from .schema import (
    TranscriptSegmentV1,
    RFChannel,
    AudioMetadata,
    P25Metadata,
    LRRPData,
    ARSData,
    NetworkData,
    CallEvent,
    Transcription,
    TalkgroupMetadata,
)
from .ingestion.parser import RecordingParser
from .ingestion.tr_schema import TRCallJSON
from .ingestion.tr_mapper import map_tr_to_segment
from .transcription.whisper_client import WhisperClient
from .forwarding.emitter import SegmentEmitter
from .forwarding.synapse_publisher import SynapsePublisher
from .forwarding.diagnostic_mqtt import publish_diagnostic
from .orion_lookup import ORIONLookup

logger = structlog.get_logger(__name__)


class EdgePipeline:
    """
    Main processing pipeline for Nodus Edge.

    Processes individual call recordings through:
    ingest -> enrich with event log data -> transcribe -> emit
    """

    def __init__(
        self,
        node_id: Optional[str] = None,
        transcription_enabled: Optional[bool] = None,
    ):
        self.node_id = node_id or settings.node_id
        self.transcription_enabled = (
            transcription_enabled
            if transcription_enabled is not None
            else settings.transcription_enabled
        )

        self.parser = RecordingParser()
        self.emitter = SegmentEmitter()
        self.synapse_publisher = SynapsePublisher()
        self.orion_lookup = ORIONLookup()

        logger.info(
            "ORION lookup initialized",
            talkgroups=self.orion_lookup.talkgroup_count,
            sites=self.orion_lookup.site_count,
        )

        # Initialize Whisper client if transcription enabled
        self.whisper: Optional[WhisperClient] = None
        if self.transcription_enabled:
            self.whisper = WhisperClient()
            if self.whisper.health_check():
                logger.info(
                    "Whisper transcription enabled",
                    url=settings.whisper_api_url,
                )
            else:
                logger.warning(
                    "Whisper service not available, transcription disabled",
                    url=settings.whisper_api_url,
                )
                self.whisper = None

        # Log Synapse integration status
        if self.synapse_publisher.enabled:
            logger.info(
                "Synapse integration enabled",
                endpoint=settings.synapse_endpoint,
            )

        # Stats
        self._processed_count = 0
        self._transcribed_count = 0
        self._filtered_count = 0
        self._encrypted_count = 0
        self._error_count = 0
        self._transcription_failed_count = 0
        self._synapse_published_count = 0

    def _build_p25_whisper_prompt(
        self, talkgroup_metadata: Optional[TalkgroupMetadata]
    ) -> Optional[str]:
        """
        Build cadence-based initial_prompt for P25 dispatch transcription.

        Layer 1: Example dispatch conversation that teaches Whisper the
                 rhythm of radio traffic (not a word list, a cadence).
        Layer 2: System/county context from ORION talkgroup metadata.
        """
        parts = []

        if talkgroup_metadata:
            svc = talkgroup_metadata.service_type

            # Layer 1: Service-specific cadence example
            if talkgroup_metadata.is_dispatch:
                if svc == "fire":
                    parts.append(
                        "Fire dispatch radio. "
                        "Engine 5, respond to 1234 Main Street for a structure fire. "
                        "Engine 5 copy, en route. "
                    )
                elif svc == "ems":
                    parts.append(
                        "EMS dispatch radio. "
                        "Medic 3, respond to 910 Elm Street for a medical emergency. "
                        "Medic 3 copy, responding. "
                    )
                elif svc == "multi":
                    parts.append(
                        "Multi-agency dispatch radio. "
                        "Engine 5 and Medic 3, respond to 1234 Main Street. "
                        "Engine 5 copy. Medic 3 copy, en route. "
                    )
                else:
                    # law or unknown dispatch
                    parts.append(
                        "Police dispatch radio. "
                        "Unit 42, respond to 5678 Oak Avenue, report of a disturbance. "
                        "10-4, en route. "
                    )
            elif talkgroup_metadata.is_tactical:
                if svc == "fire":
                    parts.append(
                        "Fire tactical radio. "
                        "Command to all units, second floor is clear, move to third. Copy. "
                    )
                else:
                    parts.append(
                        "Tactical radio. "
                        "Unit 42, subject is northbound on 72nd Street, copy. 10-4. "
                    )
            else:
                parts.append("Public safety radio. ")

            # Layer 2: Agency/system context from ORION
            if talkgroup_metadata.category:
                parts.append(f"{talkgroup_metadata.category}. ")
            if talkgroup_metadata.alpha_tag:
                parts.append(f"{talkgroup_metadata.alpha_tag}. ")
        else:
            # No ORION metadata — generic public safety cadence
            parts.append(
                "Public safety radio. "
                "Dispatch to unit, respond to an address. 10-4, en route. "
            )

        return "".join(parts) if parts else None

    def process_recording(self, recording_path: Path) -> Optional[TranscriptSegmentV1]:
        """
        Process a single recording file through the full pipeline.

        Returns the emitted segment, or None on error.
        """
        logger.debug("Processing recording", path=recording_path.name)

        # Parse recording metadata
        metadata = self.parser.parse_call_recording(recording_path)
        if not metadata:
            # Try baseband pattern
            metadata = self.parser.parse_baseband_recording(recording_path)

        if not metadata:
            logger.warning("Could not parse recording filename", path=recording_path.name)
            self._error_count += 1
            return None

        # Find and parse associated event logs
        call_events = self._find_call_events(metadata)

        # Build P25 metadata from events
        p25_metadata = self._build_p25_metadata(metadata, call_events)

        # Enrich talkgroup data from ORION lookup
        talkgroup_id = metadata.get("talkgroup_id")
        talkgroup_metadata = None
        if talkgroup_id:
            try:
                tgid = int(talkgroup_id)
                tg_info = self.orion_lookup.get_talkgroup(tgid)
                if tg_info:
                    talkgroup_metadata = TalkgroupMetadata(
                        tgid=tg_info.tgid,
                        alpha_tag=tg_info.alpha_tag,
                        description=tg_info.description,
                        tag=tg_info.tag,
                        category=tg_info.category,
                        mode=tg_info.mode,
                        encrypted=tg_info.encrypted,
                        service_type=tg_info.service_type,
                        is_dispatch=tg_info.is_dispatch,
                        is_tactical=tg_info.is_tactical,
                    )
            except (ValueError, TypeError):
                pass

        # Check if encrypted - use talkgroup metadata as authoritative source if available
        is_encrypted = False
        if talkgroup_metadata:
            is_encrypted = talkgroup_metadata.encrypted
        elif p25_metadata:
            is_encrypted = p25_metadata.encrypted

        if is_encrypted:
            self._encrypted_count += 1
            logger.debug("Encrypted call detected", path=recording_path.name)

        # Transcribe audio (skip if encrypted or no speech expected)
        transcription = None
        if self.whisper and not is_encrypted:
            initial_prompt = self._build_p25_whisper_prompt(talkgroup_metadata)
            transcription = self.whisper.transcribe(
                recording_path,
                initial_prompt=initial_prompt,
            )
            if transcription and transcription.text:
                # Filter Whisper hallucinations before further processing
                if settings.p25_hallucination_filter_enabled:
                    passes, quality_score, reason = evaluate_transcription(transcription)

                    # Shadow mode: also run legacy filter for comparison
                    legacy_decision = None
                    legacy_reason = None
                    if settings.quality_gate_enabled and not settings.quality_gate_primary:
                        legacy_is_hall, legacy_reason = is_whisper_hallucination(transcription.text)
                        legacy_decision = legacy_is_hall

                    # Log to audit
                    audit_log.log_transcription(
                        modality="p25",
                        text=transcription.text,
                        confidence=transcription.confidence,
                        max_no_speech_prob=transcription.max_no_speech_prob,
                        max_compression_ratio=transcription.max_compression_ratio,
                        min_segment_confidence=transcription.min_confidence,
                        quality_score=quality_score,
                        outcome="passed" if passes
                            else "rejected_quality" if "quality" in reason
                            else "rejected_structural",
                        rejection_reason=reason if not passes else None,
                        duration_seconds=transcription.duration_seconds,
                        legacy_decision=legacy_decision,
                        legacy_reason=legacy_reason,
                    )

                    if not passes:
                        logger.info(
                            "Filtered Whisper hallucination",
                            text=transcription.text.strip()[:80],
                            reason=reason,
                            quality_score=round(quality_score, 3),
                            path=recording_path.name,
                        )
                        self._filtered_count += 1
                        transcription = None

            if transcription and transcription.text:
                self._transcribed_count += 1
                # Update audio duration from transcription
                if transcription.duration_seconds:
                    metadata["duration_seconds"] = transcription.duration_seconds

        # No transcription on a non-encrypted call — nothing to publish
        if not is_encrypted and (not transcription or not transcription.text):
            self._transcription_failed_count += 1
            logger.warning(
                "Segment dropped: no transcription",
                frequency_hz=metadata.get("frequency_hz"),
                talkgroup=metadata.get("talkgroup"),
                path=recording_path.name,
                whisper_available=self.whisper is not None,
            )
            publish_diagnostic(self.node_id, "transcription_failed", {
                "mode": "p25",
                "frequency_hz": metadata.get("frequency_hz"),
                "talkgroup": metadata.get("talkgroup"),
                "filename": recording_path.name,
                "whisper_available": self.whisper is not None,
            })
            return None

        # Calculate overall confidence
        confidence = self._calculate_confidence(transcription, p25_metadata)

        # Extract LRRP, ARS, Network data from events
        lrrp_data, ars_data, network_data = self._extract_supplemental_data(call_events)

        # Build RF channel with enriched talkgroup data
        rf_channel = self.parser.build_rf_channel(metadata, call_events)
        if talkgroup_metadata:
            rf_channel.talkgroup = talkgroup_metadata
            # Also set talkgroup_name from enriched data if not already set
            if not rf_channel.talkgroup_name:
                rf_channel.talkgroup_name = talkgroup_metadata.alpha_tag

        # Build the segment
        segment = TranscriptSegmentV1(
            segment_id=uuid4(),
            source_node_id=self.node_id,
            source_node_version=__version__,
            metro=settings.metro or None,
            timestamp=metadata["timestamp"],
            rf_channel=rf_channel,
            source_radio_id=metadata.get("source_radio_id"),
            destination_radio_id=metadata.get("destination_radio_id"),
            audio=self.parser.build_audio_metadata(metadata),
            p25=p25_metadata,
            call_events=call_events,
            lrrp=lrrp_data,
            ars=ars_data,
            network=network_data,
            transcription=transcription,
            confidence=confidence,
            source_files={
                "audio": str(recording_path),
            },
        )

        # Emit to output (local file)
        output_path = self.emitter.emit(segment)
        self._processed_count += 1

        # Publish to Synapse if configured
        synapse_success = False
        if self.synapse_publisher.enabled:
            synapse_success = self.synapse_publisher.publish(segment)
            if synapse_success:
                self._synapse_published_count += 1

        logger.info(
            "Recording processed",
            path=recording_path.name,
            segment_id=str(segment.segment_id),
            talkgroup=talkgroup_metadata.alpha_tag if talkgroup_metadata else talkgroup_id,
            encrypted=is_encrypted,
            transcribed=transcription is not None and bool(transcription.text),
            events=len(call_events),
            synapse=synapse_success if self.synapse_publisher.enabled else "disabled",
        )

        return segment

    def process_tr_recording(
        self,
        json_path: Path,
        audio_path: Path,
        call_data: TRCallJSON,
    ) -> Optional[TranscriptSegmentV1]:
        """
        Process a Trunk Recorder call through the pipeline.

        Uses TR's JSON metadata instead of SDRTrunk filename parsing,
        but shares ORION enrichment, Whisper transcription, and emission.
        """
        logger.debug(
            "Processing TR call",
            talkgroup=call_data.talkgroup,
            freq=call_data.freq,
            duration=call_data.call_length,
        )

        # Enrich talkgroup data from ORION lookup
        talkgroup_metadata = None
        try:
            tg_info = self.orion_lookup.get_talkgroup(call_data.talkgroup)
            if tg_info:
                talkgroup_metadata = TalkgroupMetadata(
                    tgid=tg_info.tgid,
                    alpha_tag=tg_info.alpha_tag,
                    description=tg_info.description,
                    tag=tg_info.tag,
                    category=tg_info.category,
                    mode=tg_info.mode,
                    encrypted=tg_info.encrypted,
                    service_type=tg_info.service_type,
                    is_dispatch=tg_info.is_dispatch,
                    is_tactical=tg_info.is_tactical,
                )
        except (ValueError, TypeError):
            pass

        # Check encryption - ORION metadata is authoritative
        is_encrypted = False
        if talkgroup_metadata:
            is_encrypted = talkgroup_metadata.encrypted
        elif call_data.encrypted:
            is_encrypted = True

        if is_encrypted:
            self._encrypted_count += 1
            # Propagate ORION-resolved encryption to call_data for downstream mapping
            call_data.encrypted = 1

        # Transcribe audio (skip if encrypted)
        transcription = None
        if self.whisper and not is_encrypted and audio_path.exists():
            initial_prompt = self._build_p25_whisper_prompt(talkgroup_metadata)
            transcription = self.whisper.transcribe(
                audio_path,
                initial_prompt=initial_prompt,
            )
            if transcription and transcription.text:
                # Filter Whisper hallucinations before further processing
                if settings.p25_hallucination_filter_enabled:
                    passes, quality_score, reason = evaluate_transcription(transcription)

                    # Shadow mode: also run legacy filter for comparison
                    legacy_decision = None
                    legacy_reason = None
                    if settings.quality_gate_enabled and not settings.quality_gate_primary:
                        legacy_is_hall, legacy_reason = is_whisper_hallucination(transcription.text)
                        legacy_decision = legacy_is_hall

                    audit_log.log_transcription(
                        modality="p25",
                        text=transcription.text,
                        confidence=transcription.confidence,
                        max_no_speech_prob=transcription.max_no_speech_prob,
                        max_compression_ratio=transcription.max_compression_ratio,
                        min_segment_confidence=transcription.min_confidence,
                        quality_score=quality_score,
                        outcome="passed" if passes
                            else "rejected_quality" if "quality" in reason
                            else "rejected_structural",
                        rejection_reason=reason if not passes else None,
                        duration_seconds=transcription.duration_seconds,
                        legacy_decision=legacy_decision,
                        legacy_reason=legacy_reason,
                    )

                    if not passes:
                        logger.info(
                            "Filtered Whisper hallucination",
                            text=transcription.text.strip()[:80],
                            reason=reason,
                            quality_score=round(quality_score, 3),
                            path=audio_path.name,
                        )
                        self._filtered_count += 1
                        transcription = None

            if transcription and transcription.text:
                self._transcribed_count += 1

        # No transcription on a non-encrypted call — nothing to publish
        if not is_encrypted and (not transcription or not transcription.text):
            self._transcription_failed_count += 1
            logger.warning(
                "Segment dropped: no transcription",
                talkgroup=call_data.talkgroup,
                system=call_data.short_name,
                path=audio_path.name if audio_path else json_path.name,
                whisper_available=self.whisper is not None,
            )
            publish_diagnostic(self.node_id, "transcription_failed", {
                "mode": "p25",
                "talkgroup": call_data.talkgroup,
                "system": call_data.short_name,
                "filename": audio_path.name if audio_path else json_path.name,
                "whisper_available": self.whisper is not None,
            })
            return None

        # Map to TranscriptSegmentV1 with enriched data
        segment = map_tr_to_segment(
            call=call_data,
            json_path=json_path,
            audio_path=audio_path,
            transcription=transcription,
            talkgroup_metadata=talkgroup_metadata,
            node_id=self.node_id,
            metro=settings.metro or None,
        )

        # Emit to output (local file)
        output_path = self.emitter.emit(segment)
        self._processed_count += 1

        # Publish to Synapse if configured
        synapse_success = False
        if self.synapse_publisher.enabled:
            synapse_success = self.synapse_publisher.publish(segment)
            if synapse_success:
                self._synapse_published_count += 1

        logger.info(
            "TR call processed",
            path=audio_path.name,
            segment_id=str(segment.segment_id),
            talkgroup=talkgroup_metadata.alpha_tag if talkgroup_metadata else str(call_data.talkgroup),
            encrypted=is_encrypted,
            transcribed=transcription is not None and bool(transcription.text),
            events=len(segment.call_events),
            synapse=synapse_success if self.synapse_publisher.enabled else "disabled",
        )

        return segment

    def _find_call_events(self, metadata: Dict[str, Any]) -> List[CallEvent]:
        """
        Find event log associated with this recording and extract events.

        Matches based on timestamp and talkgroup.
        """
        events: List[CallEvent] = []

        # Build search parameters from recording metadata
        timestamp = metadata.get("timestamp")
        talkgroup_id = metadata.get("talkgroup_id")
        source_radio_id = metadata.get("source_radio_id")

        if not timestamp:
            return events

        # Look for event logs in the event_logs directory
        event_logs_dir = settings.event_logs_path
        if not event_logs_dir.exists():
            return events

        # Find logs from around the same time
        date_str = timestamp.strftime("%Y%m%d")

        for log_path in event_logs_dir.glob(f"{date_str}_*_call_events.log"):
            log_events = self.parser.parse_call_events_file(log_path)

            # Filter events matching our recording
            for event in log_events:
                # Match by timestamp proximity (within 30 seconds)
                time_diff = abs((event.timestamp - timestamp).total_seconds())
                if time_diff > 30:
                    continue

                # Match by talkgroup if we have it
                if talkgroup_id and event.destination_talkgroup_id:
                    if event.destination_talkgroup_id == talkgroup_id:
                        events.append(event)
                # Match by source radio
                elif source_radio_id and event.source_radio_id:
                    if event.source_radio_id == source_radio_id:
                        events.append(event)

        # Sort by timestamp
        events.sort(key=lambda e: e.timestamp)

        return events

    def _build_p25_metadata(
        self,
        metadata: Dict[str, Any],
        events: List[CallEvent],
    ) -> Optional[P25Metadata]:
        """Build P25 metadata from recording and events."""
        if not events:
            return None

        # Get first event with protocol data
        for event in events:
            if event.protocol or event.event_type:
                return P25Metadata(
                    protocol=event.protocol,
                    event_type=event.event_type,
                    event_id=event.event_id,
                    phase=event.phase,
                    encrypted=event.encrypted,
                    priority=event.priority,
                    grant_type=event.grant_type,
                )

        return None

    def _extract_supplemental_data(
        self, events: List[CallEvent]
    ) -> tuple[Optional[LRRPData], Optional[ARSData], Optional[NetworkData]]:
        """Extract LRRP, ARS, and Network data from events."""
        lrrp = None
        ars = None
        network = None

        for event in events:
            if event.lrrp and not lrrp:
                lrrp = event.lrrp
            if event.ars and not ars:
                ars = event.ars
            if event.network and not network:
                network = event.network

        return lrrp, ars, network

    def _calculate_confidence(
        self,
        transcription: Optional[Transcription],
        p25_metadata: Optional[P25Metadata],
    ) -> float:
        """
        Calculate overall segment confidence.

        Based on:
        - Transcription confidence (if available)
        - Encryption status (encrypted = 0 confidence for transcription)
        - Signal quality indicators (future enhancement)
        """
        if p25_metadata and p25_metadata.encrypted:
            # Encrypted calls get a confidence based on signal, not content
            return 0.5  # We're confident about the metadata, not the content

        if transcription and transcription.confidence:
            return transcription.confidence

        # Default confidence for non-transcribed, non-encrypted
        return 0.7

    def process_encrypted_call(self, call_data: Dict[str, Any]) -> Optional[TranscriptSegmentV1]:
        """
        Process an encrypted call from event log (no audio recording).

        Creates a TranscriptSegment with P25 metadata but no audio/transcription.
        This allows encrypted OPD traffic to appear in Diagnostics.

        Args:
            call_data: Dict with encrypted call metadata from parser.extract_encrypted_calls()

        Returns:
            The emitted segment, or None on error.
        """
        talkgroup_id = call_data.get("talkgroup_id")
        timestamp = call_data.get("timestamp")

        if not talkgroup_id or not timestamp:
            logger.warning("Encrypted call missing required fields", data=call_data)
            return None

        logger.debug(
            "Processing encrypted call",
            talkgroup=talkgroup_id,
            timestamp=timestamp.isoformat(),
        )

        # Enrich talkgroup data from ORION lookup
        talkgroup_metadata = None
        try:
            tgid = int(talkgroup_id)
            tg_info = self.orion_lookup.get_talkgroup(tgid)
            if tg_info:
                talkgroup_metadata = TalkgroupMetadata(
                    tgid=tg_info.tgid,
                    alpha_tag=tg_info.alpha_tag,
                    description=tg_info.description,
                    tag=tg_info.tag,
                    category=tg_info.category,
                    mode=tg_info.mode,
                    encrypted=True,  # Override - we know it's encrypted
                    service_type=tg_info.service_type,
                    is_dispatch=tg_info.is_dispatch,
                    is_tactical=tg_info.is_tactical,
                )
        except (ValueError, TypeError):
            pass

        # Build RF channel
        rf_channel = RFChannel(
            frequency_hz=call_data.get("frequency_hz") or 0,
            channel_number=call_data.get("channel_number"),
            talkgroup_id=talkgroup_id,
            talkgroup_name=call_data.get("talkgroup_name") or (
                talkgroup_metadata.alpha_tag if talkgroup_metadata else None
            ),
            talkgroup=talkgroup_metadata,
            system_name="ORION",
            channel_type="T-Control",
            timeslot=call_data.get("timeslot"),
        )

        # Build P25 metadata
        p25_metadata = P25Metadata(
            protocol=call_data.get("protocol", "APCO-25"),
            event_type=call_data.get("event_type", "Encrypted Group Call"),
            event_id=call_data.get("event_id"),
            phase=call_data.get("phase"),
            encrypted=True,
            priority=call_data.get("priority"),
            grant_type=call_data.get("grant_type"),
        )

        # Build audio metadata (placeholder - no actual audio)
        audio_metadata = AudioMetadata(
            filename=f"encrypted_{talkgroup_id}_{timestamp.strftime('%Y%m%d_%H%M%S')}.encrypted",
            filepath=None,
            duration_seconds=None,
            file_size_bytes=0,
            format="encrypted",
        )

        # Build a single CallEvent for the encrypted call
        call_event = CallEvent(
            timestamp=timestamp,
            event_type=call_data.get("event_type", "Encrypted Group Call"),
            protocol=call_data.get("protocol", "APCO-25"),
            source_radio_id=call_data.get("source_radio_id"),
            destination_talkgroup_id=talkgroup_id,
            destination_talkgroup_name=call_data.get("talkgroup_name"),
            frequency_hz=call_data.get("frequency_hz"),
            timeslot=call_data.get("timeslot"),
            phase=call_data.get("phase"),
            encrypted=True,
            priority=call_data.get("priority"),
            grant_type=call_data.get("grant_type"),
            event_id=call_data.get("event_id"),
            raw_details=call_data.get("raw_details"),
        )

        # Build the segment
        segment = TranscriptSegmentV1(
            segment_id=uuid4(),
            source_node_id=self.node_id,
            source_node_version=__version__,
            metro=settings.metro or None,
            timestamp=timestamp,
            rf_channel=rf_channel,
            source_radio_id=call_data.get("source_radio_id"),
            audio=audio_metadata,
            p25=p25_metadata,
            call_events=[call_event],
            transcription=None,  # No transcription for encrypted calls
            confidence=0.5,  # Confident about metadata, not content
            source_files={
                "event_log": call_data.get("source_file", "unknown"),
            },
        )

        # Emit to output (local file)
        output_path = self.emitter.emit(segment)
        self._processed_count += 1
        self._encrypted_count += 1

        # Publish to Synapse if configured
        synapse_success = False
        if self.synapse_publisher.enabled:
            synapse_success = self.synapse_publisher.publish(segment)
            if synapse_success:
                self._synapse_published_count += 1

        logger.info(
            "Encrypted call processed",
            segment_id=str(segment.segment_id),
            talkgroup=talkgroup_metadata.alpha_tag if talkgroup_metadata else talkgroup_id,
            radio_id=call_data.get("source_radio_id"),
            synapse=synapse_success if self.synapse_publisher.enabled else "disabled",
        )

        return segment

    def scan_event_logs_for_encrypted(
        self,
        since_timestamp: Optional[datetime] = None,
    ) -> int:
        """
        Scan event logs for encrypted calls and process them.

        Args:
            since_timestamp: Only process calls after this timestamp

        Returns:
            Count of encrypted calls processed
        """
        event_logs_dir = settings.event_logs_path
        if not event_logs_dir.exists():
            logger.warning("Event logs directory not found", path=str(event_logs_dir))
            return 0

        processed_count = 0

        # Find recent event log files
        for log_path in sorted(
            event_logs_dir.glob("*_call_events.log"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,  # Newest first
        ):
            # Extract encrypted calls from this log
            encrypted_calls = self.parser.extract_encrypted_calls(
                log_path,
                since_timestamp=since_timestamp,
            )

            for call_data in encrypted_calls:
                try:
                    segment = self.process_encrypted_call(call_data)
                    if segment:
                        processed_count += 1
                except Exception as e:
                    logger.error(
                        "Error processing encrypted call",
                        error=str(e),
                        talkgroup=call_data.get("talkgroup_id"),
                    )

            # Limit to recent logs only (last hour of files)
            try:
                import time
                if time.time() - log_path.stat().st_mtime > 3600:
                    break
            except OSError:
                pass

        if processed_count > 0:
            logger.info(
                "Encrypted calls scan complete",
                processed=processed_count,
            )

        return processed_count

    def get_stats(self) -> Dict[str, Any]:
        """Get pipeline statistics."""
        return {
            "node_id": self.node_id,
            "processed_count": self._processed_count,
            "transcribed_count": self._transcribed_count,
            "filtered_count": self._filtered_count,
            "encrypted_count": self._encrypted_count,
            "error_count": self._error_count,
            "transcription_failed_count": self._transcription_failed_count,
            "synapse_published_count": self._synapse_published_count,
            "transcription_enabled": self.transcription_enabled,
            "whisper_available": self.whisper is not None,
            "emitter": self.emitter.get_stats(),
            "synapse": self.synapse_publisher.get_stats(),
        }
