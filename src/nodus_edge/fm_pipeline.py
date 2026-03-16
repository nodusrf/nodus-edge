"""
FM Ham Radio Processing Pipeline for Nodus Edge.

Processes FM ham radio recordings:
1. Parse recording metadata (frequency, timestamp)
2. Transcribe audio
3. Extract callsigns from transcript
4. Build FMTranscriptSegment.v1
5. Emit to output directory
"""

import base64
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, Any, List, Optional, Tuple
from uuid import uuid4

import structlog

from . import __version__
from .config import settings
from .hallucination_filter import evaluate_transcription, is_whisper_hallucination, truncate_tail_loop
from .transcription.audit_log import audit_log
from .schema import (
    FMTranscriptSegmentV1,
    FMRFChannel,
    AudioMetadata,
    Transcription,
)
from .ingestion.fm_parser import FMRecordingParser
from .ingestion.morse_decoder import detect_and_decode_morse, MorseResult
from .transcription.whisper_client import WhisperClient
from .forwarding.emitter import SegmentEmitter
from .forwarding.synapse_publisher import SynapsePublisher
from .forwarding.diagnostic_mqtt import publish_diagnostic
from .data.ham_data import get_repeater_db, get_callsign_db
from .data.operator_cache import OperatorCache
from .utils import levenshtein_distance
from .validation import validate_fm_segment

logger = structlog.get_logger(__name__)


def _text_similarity(a: str, b: str) -> float:
    """Word-level Jaccard similarity (0.0 to 1.0)."""
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    wa, wb = set(a.lower().split()), set(b.lower().split())
    union = wa | wb
    return len(wa & wb) / len(union) if union else 1.0


class FMPipeline:
    """
    Processing pipeline for FM ham radio recordings.

    Simpler than P25 pipeline - no event logs, ORION lookup, or P25 metadata.
    Extracts callsigns from transcripts for actor identification.
    """

    # Hyphen-spelled callsign pattern: W-0-W-Y-V, K-A-1-A-B-C, etc.
    _SPELLED_CALLSIGN_RE = re.compile(r'[A-Z0-9](?:-[A-Z0-9]){3,5}', re.IGNORECASE)

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

        self.parser = FMRecordingParser()
        self.emitter = SegmentEmitter()
        self.synapse_publisher = SynapsePublisher()

        # Initialize Whisper client if transcription enabled
        self.whisper: Optional[WhisperClient] = None
        self._whisper_available = False
        self._last_whisper_retry_at: float = 0.0
        self._whisper_retry_interval = settings.fm_whisper_retry_interval_seconds

        if self.transcription_enabled:
            self.whisper = WhisperClient()
            if self.whisper.health_check():
                self._whisper_available = True
                logger.info(
                    "Whisper transcription enabled for FM",
                    url=settings.whisper_api_url,
                )
            else:
                logger.warning(
                    "Whisper service not available — will retry periodically",
                    url=settings.whisper_api_url,
                    retry_interval_seconds=self._whisper_retry_interval,
                )

        # Log Synapse integration status
        if self.synapse_publisher.enabled:
            logger.info(
                "Synapse integration enabled for FM",
                endpoint=settings.synapse_endpoint,
            )

        # Load offline ham data
        self._repeater_db = get_repeater_db()
        self._callsign_db = get_callsign_db()
        if self._repeater_db.load():
            logger.info(
                "Repeater database loaded",
                count=len(self._repeater_db.get_all_frequencies()),
            )
        if self._callsign_db.load():
            logger.info("Callsign database loaded")

        # Operator cache for Whisper prompt enrichment
        self._operator_cache = OperatorCache(
            cache_dir=settings.fm_capture_dir,
            filename=settings.fm_operator_cache_file,
        )

        # Morse detection config
        if settings.fm_morse_detection_enabled:
            logger.info(
                "Morse code detection enabled",
                min_snr_db=settings.fm_morse_min_snr_db,
                tone_range=f"{settings.fm_morse_tone_range_low_hz}-{settings.fm_morse_tone_range_high_hz} Hz",
            )

        # Segment callbacks (e.g., dashboard store)
        self._segment_callbacks: List[Callable] = []

        # Per-segment validation warning tracker (rolling 1-hour window)
        # {code: [(timestamp, count_delta), ...]}
        self._segment_warnings: Dict[str, List[float]] = {}
        self._segment_warning_window = 3600  # 1 hour in seconds

        # Stats
        self._processed_count = 0
        self._transcribed_count = 0
        self._filtered_count = 0
        self._beacon_count = 0
        self._kerchunk_count = 0
        self._bleedover_count = 0
        self._morse_count = 0
        self._error_count = 0
        self._transcription_failed_count = 0
        self._synapse_published_count = 0
        self._callsigns_extracted = 0

        # Shadow Whisper client for model comparison (fire-and-forget)
        self._shadow_whisper: Optional[WhisperClient] = None
        self._shadow_executor: Optional[ThreadPoolExecutor] = None
        self._shadow_available = False
        self._shadow_count = 0
        self._shadow_error_count = 0

        if settings.shadow_whisper_enabled and self.transcription_enabled:
            self._shadow_whisper = WhisperClient(
                base_url=settings.shadow_whisper_api_url,
                timeout=settings.shadow_whisper_timeout_seconds,
            )
            if self._shadow_whisper.health_check():
                self._shadow_available = True
                self._shadow_executor = ThreadPoolExecutor(
                    max_workers=1,
                    thread_name_prefix="shadow-whisper",
                )
                logger.info(
                    "Shadow Whisper enabled for model comparison",
                    primary_url=settings.whisper_api_url,
                    shadow_url=settings.shadow_whisper_api_url,
                )
            else:
                logger.warning(
                    "Shadow Whisper service not available",
                    url=settings.shadow_whisper_api_url,
                )

    def process_recording(
        self,
        recording_path: Path,
        frequency_hz: Optional[int] = None,
        signal_db: Optional[float] = None,
    ) -> Optional[FMTranscriptSegmentV1]:
        """
        Process a single FM recording file through the full pipeline.

        Args:
            recording_path: Path to the WAV file
            frequency_hz: Optional frequency override (from scanner)
            signal_db: Optional pre-normalization signal strength in dB

        Returns:
            The emitted segment, or None on error.
        """
        logger.debug("Processing FM recording", path=recording_path.name)

        # Parse recording metadata from filename
        metadata = self.parser.parse_fm_recording(recording_path)
        if not metadata:
            logger.warning(
                "Could not parse FM recording filename",
                path=recording_path.name,
            )
            self._error_count += 1
            audit_log.log_transcription(
                modality="fm",
                text="",
                outcome="filtered_error",
                rejection_reason="Could not parse recording filename",
                audio_filename=recording_path.name,
            )
            return None

        # Use frequency from scanner if provided (more reliable than filename)
        if frequency_hz:
            metadata["frequency_hz"] = frequency_hz

        # RF bleedover detection — reject abnormally strong signals (front-end overload)
        if (
            settings.fm_bleedover_detection_enabled
            and signal_db is not None
            and signal_db > settings.fm_bleedover_threshold_db
        ):
            self._bleedover_count += 1
            logger.info(
                "🔥 Bleedover suspected — signal too strong (front-end overload)",
                signal_db=signal_db,
                threshold_db=settings.fm_bleedover_threshold_db,
                frequency_hz=metadata["frequency_hz"],
                path=recording_path.name,
            )
            audit_log.log_transcription(
                modality="fm",
                text="",
                outcome="filtered_bleedover",
                rejection_reason=f"Signal strength {signal_db} dB exceeds bleedover threshold {settings.fm_bleedover_threshold_db} dB",
                audio_filename=recording_path.name,
            )
            publish_diagnostic(
                node_id=self.node_id,
                event_type="bleedover_detected",
                payload={
                    "label": "🔥 bleedover",
                    "signal_db": signal_db,
                    "threshold_db": settings.fm_bleedover_threshold_db,
                    "frequency_hz": metadata["frequency_hz"],
                    "action": settings.fm_bleedover_action,
                    "audio_filename": recording_path.name,
                },
            )
            if settings.fm_bleedover_action == "drop":
                return None

        # Morse code detection (runs before Whisper — fast, pure signal processing)
        morse_result: Optional[MorseResult] = None
        if settings.fm_morse_detection_enabled:
            try:
                morse_result = detect_and_decode_morse(
                    recording_path,
                    min_snr_db=settings.fm_morse_min_snr_db,
                    tone_low_hz=settings.fm_morse_tone_range_low_hz,
                    tone_high_hz=settings.fm_morse_tone_range_high_hz,
                )
            except Exception as e:
                logger.debug("Morse detection error", error=str(e), path=recording_path.name)
                morse_result = None

        # Transcribe audio
        transcription = None
        detected_callsigns = []
        signal_type = None

        # Periodic Whisper reconnection when service is down
        if self.whisper and not self._whisper_available:
            now = time.monotonic()
            if now - self._last_whisper_retry_at >= self._whisper_retry_interval:
                self._last_whisper_retry_at = now
                if self.whisper.health_check():
                    self._whisper_available = True
                    logger.info(
                        "Whisper service recovered",
                        url=settings.whisper_api_url,
                    )

        # Build Whisper initial prompt for ham radio vocabulary priming
        initial_prompt = None
        if settings.fm_whisper_prompt_enabled and settings.mode == "fm":
            initial_prompt = self._build_whisper_prompt(metadata["frequency_hz"])

        whisper_is_hallucination = False
        quality_score = 0.0
        rejection_reason = ""
        if self.whisper and self._whisper_available:
            transcription = self.whisper.transcribe(
                recording_path,
                initial_prompt=initial_prompt,
            )
            if transcription is None:
                # Transcription call failed — check if service went down
                if not self.whisper.health_check():
                    self._whisper_available = False
                    logger.warning(
                        "Whisper service went down — will retry periodically",
                        url=settings.whisper_api_url,
                        retry_interval_seconds=self._whisper_retry_interval,
                    )
            if transcription and transcription.text:
                # Truncate mid-segment Whisper loops before evaluation
                if settings.fm_tail_loop_truncation_enabled:
                    truncated_text, was_truncated = truncate_tail_loop(transcription.text)
                    if was_truncated:
                        logger.info(
                            "Tail loop truncated",
                            original_len=len(transcription.text),
                            truncated_len=len(truncated_text),
                            path=recording_path.name,
                        )
                        transcription.text = truncated_text

                # Evaluate transcription quality
                if settings.fm_hallucination_filter_enabled:
                    passes, quality_score, rejection_reason = evaluate_transcription(transcription, initial_prompt=initial_prompt)

                    # Shadow mode: also run legacy filter for comparison logging
                    legacy_decision = None
                    legacy_reason = None
                    if settings.quality_gate_enabled and not settings.quality_gate_primary:
                        legacy_is_hall, legacy_reason = is_whisper_hallucination(transcription.text)
                        legacy_decision = legacy_is_hall

                    if not passes:
                        whisper_is_hallucination = True
                        logger.debug(
                            "Whisper hallucination detected",
                            text=transcription.text.strip()[:80],
                            reason=rejection_reason,
                            quality_score=round(quality_score, 3),
                            path=recording_path.name,
                        )

                    # Log to audit
                    audit_log.log_transcription(
                        modality="fm",
                        text=transcription.text,
                        confidence=transcription.confidence,
                        max_no_speech_prob=transcription.max_no_speech_prob,
                        max_compression_ratio=transcription.max_compression_ratio,
                        min_segment_confidence=transcription.min_confidence,
                        quality_score=quality_score,
                        outcome="rejected_quality" if whisper_is_hallucination and "quality" in rejection_reason
                            else "rejected_structural" if whisper_is_hallucination
                            else "passed",
                        rejection_reason=rejection_reason if whisper_is_hallucination else None,
                        frequency_hz=metadata.get("frequency_hz"),
                        duration_seconds=transcription.duration_seconds,
                        audio_filename=recording_path.name,
                        legacy_decision=legacy_decision,
                        legacy_reason=legacy_reason,
                    )

        # Shadow transcription: fire-and-forget to background thread
        if (self._shadow_whisper and self._shadow_available
                and self._shadow_executor
                and transcription and transcription.text):
            self._submit_shadow_transcription(
                recording_path=recording_path,
                initial_prompt=initial_prompt,
                frequency_hz=metadata["frequency_hz"],
                primary_transcription=transcription,
                primary_quality_score=quality_score,
                primary_is_hallucination=whisper_is_hallucination,
                primary_rejection_reason=rejection_reason,
            )

        # Decision matrix: combine Morse detection + Whisper result
        morse_detected = morse_result is not None and morse_result.detected

        # Reject low-confidence morse — treat as not detected
        if morse_detected and morse_result.confidence < settings.fm_morse_min_confidence:
            logger.info(
                "Morse rejected (low confidence)",
                confidence=round(morse_result.confidence, 2),
                text=morse_result.text[:40],
                path=recording_path.name,
            )
            morse_detected = False

        # Reject kerchunk courtesy tones — Morse decoder picks up repeater beeps
        # as single-letter characters (E=dit, T=dah). Real CW has multi-char words.
        if (settings.fm_kerchunk_filter_enabled
                and morse_detected
                and self._is_kerchunk_courtesy_tone(morse_result)):
            logger.info(
                "Kerchunk courtesy tone filtered",
                morse_text=morse_result.text,
                confidence=round(morse_result.confidence, 2),
                path=recording_path.name,
            )
            morse_detected = False
            self._kerchunk_count += 1
            audit_log.log_transcription(
                modality="fm",
                text=morse_result.text if morse_result else "",
                outcome="filtered_kerchunk",
                rejection_reason="Kerchunk courtesy tone (E/T only)",
                frequency_hz=metadata.get("frequency_hz"),
                audio_filename=recording_path.name,
            )

        if whisper_is_hallucination and not morse_detected:
            # No Morse, Whisper hallucinated → drop as before
            logger.info(
                "Filtered Whisper hallucination",
                text=transcription.text.strip()[:80] if transcription and transcription.text else "",
                path=recording_path.name,
            )
            self._filtered_count += 1
            return None

        if morse_detected and (whisper_is_hallucination or not (transcription and transcription.text)):
            # Morse detected, no useful speech → emit as Morse-only segment
            signal_type = "morse"
            self._morse_count += 1
            transcription = Transcription(
                engine="morse_decoder",
                model="goertzel",
                language="en",
                text=f"[CW] {morse_result.text}",
                confidence=morse_result.confidence,
                duration_seconds=metadata.get("duration_seconds"),
                transcribed_at=datetime.utcnow(),
            )
            # Extract callsigns from Morse-decoded text
            if settings.fm_extract_callsigns:
                detected_callsigns = self.parser.extract_callsigns(morse_result.text)
                if detected_callsigns:
                    self._callsigns_extracted += len(detected_callsigns)
            logger.info(
                "Morse-only segment",
                morse_text=morse_result.text,
                tone_hz=round(morse_result.tone_frequency_hz, 1),
                wpm=round(morse_result.wpm, 1),
                callsigns=detected_callsigns or None,
                path=recording_path.name,
            )
        elif morse_detected and transcription and transcription.text and not whisper_is_hallucination:
            # Both Morse and valid speech → voice+morse
            signal_type = "voice+morse"
            self._morse_count += 1
            self._transcribed_count += 1
            if transcription.duration_seconds:
                metadata["duration_seconds"] = transcription.duration_seconds
            # Extract callsigns from speech transcript
            if settings.fm_extract_callsigns:
                detected_callsigns = self.parser.extract_callsigns(transcription.text)
                # Also extract callsigns from Morse decode and merge
                morse_callsigns = self.parser.extract_callsigns(morse_result.text)
                all_callsigns = set(detected_callsigns) | set(morse_callsigns)
                detected_callsigns = sorted(all_callsigns)
                if detected_callsigns:
                    self._callsigns_extracted += len(detected_callsigns)
                    logger.debug("Callsigns extracted", callsigns=detected_callsigns)
            logger.info(
                "Voice+Morse segment",
                morse_text=morse_result.text,
                tone_hz=round(morse_result.tone_frequency_hz, 1),
                path=recording_path.name,
            )
        elif transcription and transcription.text and not whisper_is_hallucination:
            # Voice only, no Morse → normal flow
            signal_type = "voice"
            self._transcribed_count += 1
            if transcription.duration_seconds:
                metadata["duration_seconds"] = transcription.duration_seconds
            if settings.fm_extract_callsigns:
                detected_callsigns = self.parser.extract_callsigns(transcription.text)
                if detected_callsigns:
                    self._callsigns_extracted += len(detected_callsigns)
                    logger.debug("Callsigns extracted", callsigns=detected_callsigns)

        # No transcription, no morse — nothing to publish
        if signal_type is None:
            self._transcription_failed_count += 1
            logger.warning(
                "Segment dropped: no transcription",
                frequency_hz=metadata.get("frequency_hz"),
                path=recording_path.name,
                whisper_available=self._whisper_available,
            )
            publish_diagnostic(self.node_id, "transcription_failed", {
                "mode": "fm",
                "frequency_hz": metadata.get("frequency_hz"),
                "filename": recording_path.name,
                "whisper_available": self._whisper_available,
            })
            return None

        # Look up repeater info for frequency enrichment
        repeater_callsign = None
        repeater_info = self._repeater_db.lookup_frequency(metadata["frequency_hz"])
        if repeater_info:
            logger.debug(
                "Repeater identified",
                callsign=repeater_info.get("Callsign"),
                city=repeater_info.get("Nearest City"),
                pl=repeater_info.get("PL"),
            )
            # Store repeater callsign as channel metadata, NOT as a detected operator.
            # The repeater trustee callsign (e.g. K0USA) is not a conversation participant.
            rptr_callsign = repeater_info.get("Callsign", "").upper()
            if rptr_callsign:
                repeater_callsign = rptr_callsign

        # Correct callsigns that are close to the known repeater callsign
        # (catches single-character Whisper errors like K0BCC → K0BVC)
        if repeater_callsign and detected_callsigns:
            corrected = []
            for cs in detected_callsigns:
                if cs != repeater_callsign and levenshtein_distance(cs, repeater_callsign) <= 1:
                    logger.info(
                        "Corrected callsign via repeater match",
                        original=cs,
                        corrected=repeater_callsign,
                        frequency=self.parser.format_frequency(metadata["frequency_hz"]),
                    )
                    corrected.append(repeater_callsign)
                else:
                    corrected.append(cs)
            detected_callsigns = sorted(set(corrected))

        # Detect automated repeater beacons (voice IDs / time announcements)
        # Skip for CW-decoded segments — the whole point of CW decoding is to capture the beacon ID
        if (settings.fm_beacon_filter_enabled
                and repeater_callsign
                and signal_type != "morse"
                and transcription and transcription.text):
            is_beacon, beacon_heard = self._detect_repeater_beacon(
                transcription.text,
                repeater_callsign,
                detected_callsigns,
            )
            if is_beacon:
                freq_str = self.parser.format_frequency(metadata["frequency_hz"])
                logger.info(
                    "Repeater beacon detected",
                    repeater=repeater_callsign,
                    frequency=freq_str,
                    heard=beacon_heard,
                    path=recording_path.name,
                )
                self._beacon_count += 1
                audit_log.log_transcription(
                    modality="fm",
                    text=beacon_heard,
                    outcome="filtered_beacon",
                    rejection_reason=f"Repeater beacon ({repeater_callsign})",
                    frequency_hz=metadata.get("frequency_hz"),
                    duration_seconds=transcription.duration_seconds if transcription else None,
                    audio_filename=recording_path.name,
                )
                return None

        # Calculate confidence
        confidence = self._calculate_confidence(transcription)

        # Build RF channel
        rf_channel = self.parser.build_fm_rf_channel(metadata, signal_strength_db=signal_db)
        if repeater_callsign:
            rf_channel.repeater_callsign = repeater_callsign

        # Build audio metadata
        audio_meta = self.parser.build_audio_metadata(metadata)

        # Embed source MP3 as base64 for pipeline transport
        source_mp3 = self._find_source_mp3(recording_path)
        if source_mp3:
            try:
                mp3_bytes = source_mp3.read_bytes()
                audio_meta.audio_data_base64 = base64.b64encode(mp3_bytes).decode("ascii")
                audio_meta.filename = source_mp3.name
                audio_meta.format = "mp3"
                audio_meta.file_size_bytes = len(mp3_bytes)
            except Exception as e:
                logger.warning("Failed to embed MP3", error=str(e), mp3=source_mp3.name)

        segment = FMTranscriptSegmentV1(
            segment_id=uuid4(),
            source_node_id=self.node_id,
            source_node_version=__version__,
            metro=settings.metro or None,
            timestamp=metadata["timestamp"],
            rf_channel=rf_channel,
            audio=audio_meta,
            transcription=transcription,
            detected_callsigns=detected_callsigns,
            signal_type=signal_type,
            confidence=confidence,
            source_files={
                "audio": str(recording_path),
            },
        )

        # Validate segment and track warnings
        seg_warnings = validate_fm_segment(
            segment.model_dump(mode="json"),
            transcription_enabled=self.transcription_enabled,
        )
        if seg_warnings:
            now = time.time()
            for w in seg_warnings:
                if w.code not in self._segment_warnings:
                    self._segment_warnings[w.code] = []
                self._segment_warnings[w.code].append(now)

        # Emit to output (local file)
        output_path = self.emitter.emit_fm(segment)
        self._processed_count += 1

        # Publish to Synapse if configured
        synapse_success = False
        if self.synapse_publisher.enabled:
            synapse_success = self.synapse_publisher.publish_fm(segment)
            if synapse_success:
                self._synapse_published_count += 1

        # Notify segment callbacks (e.g., dashboard)
        if self._segment_callbacks:
            try:
                seg_dict = segment.model_dump(mode="json")
                for cb in self._segment_callbacks:
                    try:
                        cb(seg_dict)
                    except Exception as e:
                        logger.debug("Segment callback error", error=str(e))
            except Exception as e:
                logger.debug("Segment serialization error", error=str(e))

        # Periodic edge cleanup
        if self._processed_count % 50 == 0:
            self._cleanup_old_audio()

        logger.info(
            "FM recording processed",
            path=recording_path.name,
            segment_id=str(segment.segment_id),
            frequency=self.parser.format_frequency(metadata["frequency_hz"]),
            signal_type=signal_type,
            transcribed=transcription is not None and bool(transcription.text),
            callsigns=detected_callsigns or None,
            synapse=synapse_success if self.synapse_publisher.enabled else "disabled",
        )

        return segment

    @property
    def operator_cache(self) -> OperatorCache:
        """Expose operator cache for health server endpoint."""
        return self._operator_cache

    def register_segment_callback(self, callback: Callable) -> None:
        """Register a callback invoked with each emitted segment dict."""
        self._segment_callbacks.append(callback)

    def _build_whisper_prompt(self, frequency_hz: int) -> str:
        """
        Build a Whisper initial_prompt for ham radio callsign recognition.

        Layers:
        1. Generic base prompt (always) — teaches Whisper callsign patterns
        2. Repeater callsign for this frequency (from local DB)
        3. Known operators for this frequency (from Synapse-pushed cache)
        """
        parts = [settings.fm_whisper_prompt_base]

        # Layer 2: repeater callsign
        repeater_info = self._repeater_db.lookup_frequency(frequency_hz)
        if repeater_info:
            rptr = repeater_info.get("Callsign", "").upper()
            if rptr:
                freq_mhz = frequency_hz / 1_000_000
                parts.append(f"Repeater {rptr} on {freq_mhz:.3f} MHz.")

        # Layer 3: known operators from cache
        known_ops = self._operator_cache.get_operators(frequency_hz)
        if known_ops:
            # Cap at 15 to keep prompt reasonable
            ops_str = ", ".join(known_ops[:15])
            parts.append(f"Known operators: {ops_str}.")

        return " ".join(parts)

    def _find_source_mp3(self, wav_path: Path) -> Optional[Path]:
        """Find the airband source MP3 for a WAV file.

        WAV naming: 20260210_040216_146940000Hz.wav
        MP3 naming: nodus_20260210_040216_146940000.mp3
        """
        stem = wav_path.stem  # e.g. "20260210_040216_146940000Hz"
        stem_no_hz = stem.rsplit("Hz", 1)[0]  # strip trailing "Hz"
        mp3_name = f"nodus_{stem_no_hz}.mp3"
        mp3_path = wav_path.parent / "airband" / mp3_name
        if mp3_path.is_file():
            return mp3_path
        return None

    def _cleanup_old_audio(self) -> None:
        """Delete WAV and MP3 files older than the retention window."""
        try:
            cutoff = time.time() - (settings.fm_audio_retention_hours * 3600)
            cleaned = 0
            for pattern in ("*.wav", "airband/*.mp3"):
                for f in settings.fm_capture_path.glob(pattern):
                    try:
                        if f.stat().st_mtime < cutoff:
                            f.unlink(missing_ok=True)
                            cleaned += 1
                    except OSError:
                        pass
            if cleaned:
                logger.info("Edge audio cleanup", files_deleted=cleaned)
        except Exception as e:
            logger.debug("Audio cleanup error", error=str(e))

    @staticmethod
    def _is_kerchunk_courtesy_tone(morse_result) -> bool:
        """Detect courtesy tone beeps misinterpreted as Morse code.

        When someone kerchunks a repeater (keys up PTT without speaking),
        the courtesy tone beeps get decoded as single-letter Morse characters
        (E=dit, T=dah). Real CW beacons with wide spacing (e.g. "N 0 Y M J / R")
        also produce single-char words but contain diverse characters.
        Kerchunks are exclusively E and T — the two simplest Morse symbols.
        """
        text = morse_result.text.strip()
        if not text:
            return True
        # Courtesy tones decode as only E (dit) and T (dah)
        chars = set(text.replace(" ", ""))
        return chars.issubset({"E", "T"})

    def _detect_repeater_beacon(
        self,
        text: str,
        repeater_callsign: str,
        detected_callsigns: list,
    ) -> Tuple[bool, str]:
        """
        Detect automated repeater beacon (voice ID / time announcement).

        Returns (is_beacon, heard_text) where heard_text is a cleaned
        summary of what the repeater said.

        Detection criteria:
        - Text contains a hyphen-spelled version of the repeater callsign
          (e.g. "W-0-W-Y-V" for W0WYV, fuzzy within Levenshtein 1)
        - No non-repeater operator callsigns present
        - Short remaining text after removing callsign (automated IDs are brief)
        """
        # Non-repeater operator callsigns present → real conversation
        for cs in detected_callsigns:
            if cs != repeater_callsign and levenshtein_distance(cs, repeater_callsign) > 1:
                return False, ""

        text_stripped = text.strip()
        text_upper = text_stripped.upper()

        # Look for hyphen-spelled callsign patterns (e.g. "W-0-W-Y-V")
        matches = self._SPELLED_CALLSIGN_RE.findall(text_upper)

        found_repeater = False
        found_other_operator = False
        matched_span = ""

        for match in matches:
            joined = match.replace('-', '')
            if levenshtein_distance(joined, repeater_callsign) <= 1:
                found_repeater = True
                matched_span = match
            elif (len(joined) >= 4
                  and joined[0] in 'WKNAXV'
                  and any(c.isdigit() for c in joined)):
                found_other_operator = True

        # Also check for standard-form callsign in text (W0WYV as-is)
        if not found_repeater and repeater_callsign in text_upper:
            found_repeater = True
            matched_span = repeater_callsign

        if not found_repeater:
            return False, ""

        # Another operator callsign present → real conversation
        if found_other_operator:
            return False, ""

        # Check remaining text after removing the callsign
        remainder = text_upper.replace(matched_span, '', 1)
        remainder = re.sub(r'[^A-Za-z0-9\s]', '', remainder).strip()
        remaining_words = [w for w in remainder.split() if len(w) > 1]

        # Automated beacons have minimal content beyond the callsign
        if len(remaining_words) <= 6:
            return True, text_stripped

        return False, ""

    def _calculate_confidence(
        self,
        transcription: Optional[Transcription],
    ) -> float:
        """
        Calculate overall segment confidence.

        For FM, primarily based on transcription confidence.
        """
        if transcription and transcription.confidence:
            return transcription.confidence

        # Default confidence for non-transcribed
        return 0.7

    # =========================================================================
    # Shadow transcription (fire-and-forget model comparison)
    # =========================================================================

    def _submit_shadow_transcription(
        self,
        recording_path: Path,
        initial_prompt: Optional[str],
        frequency_hz: int,
        primary_transcription: Transcription,
        primary_quality_score: float,
        primary_is_hallucination: bool,
        primary_rejection_reason: str,
    ) -> None:
        """Submit shadow transcription to background thread pool."""
        # Drop if backlog building up (CPU can't keep pace)
        if hasattr(self._shadow_executor, '_work_queue'):
            if self._shadow_executor._work_queue.qsize() > 3:
                logger.debug("Shadow transcription backlogged, skipping",
                             path=recording_path.name)
                return
        try:
            future = self._shadow_executor.submit(
                self._run_shadow_transcription,
                recording_path,
                initial_prompt,
                frequency_hz,
                primary_transcription,
                primary_quality_score,
                primary_is_hallucination,
                primary_rejection_reason,
            )
            future.add_done_callback(self._shadow_done_callback)
        except RuntimeError:
            pass  # Executor shut down

    def _run_shadow_transcription(
        self,
        recording_path: Path,
        initial_prompt: Optional[str],
        frequency_hz: int,
        primary_transcription: Transcription,
        primary_quality_score: float,
        primary_is_hallucination: bool,
        primary_rejection_reason: str,
    ) -> None:
        """Execute shadow transcription and log comparison (runs in thread pool)."""
        shadow_start = time.monotonic()
        try:
            shadow_result = self._shadow_whisper.transcribe(
                recording_path,
                initial_prompt=initial_prompt,
            )
        except Exception as e:
            logger.debug("Shadow transcription failed",
                         error=str(e), path=recording_path.name)
            self._shadow_error_count += 1
            return

        shadow_elapsed = time.monotonic() - shadow_start

        if shadow_result is None:
            logger.debug("Shadow transcription returned None",
                         path=recording_path.name)
            self._shadow_error_count += 1
            return

        # Apply same tail loop truncation
        if settings.fm_tail_loop_truncation_enabled and shadow_result.text:
            shadow_result.text, _ = truncate_tail_loop(shadow_result.text)

        # Evaluate shadow through the same quality gate
        shadow_is_hallucination = False
        shadow_quality_score = 0.0
        shadow_rejection_reason = ""
        if shadow_result.text and settings.fm_hallucination_filter_enabled:
            passes, shadow_quality_score, shadow_rejection_reason = (
                evaluate_transcription(shadow_result, initial_prompt=initial_prompt)
            )
            shadow_is_hallucination = not passes

        similarity = _text_similarity(
            primary_transcription.text or "",
            shadow_result.text or "",
        )

        self._shadow_count += 1

        # Structured comparison log
        logger.info(
            "shadow_comparison",
            filename=recording_path.name,
            frequency_hz=frequency_hz,
            # Primary
            primary_model=primary_transcription.model,
            primary_text=(primary_transcription.text or "")[:200],
            primary_confidence=round(primary_transcription.confidence or 0, 3),
            primary_quality=round(primary_quality_score, 3),
            primary_rejected=primary_is_hallucination,
            primary_reason=primary_rejection_reason or None,
            # Shadow
            shadow_model=shadow_result.model,
            shadow_text=(shadow_result.text or "")[:200],
            shadow_confidence=round(shadow_result.confidence or 0, 3),
            shadow_quality=round(shadow_quality_score, 3),
            shadow_rejected=shadow_is_hallucination,
            shadow_reason=shadow_rejection_reason or None,
            shadow_latency_s=round(shadow_elapsed, 2),
            # Comparison
            text_similarity=round(similarity, 3),
            both_rejected=primary_is_hallucination and shadow_is_hallucination,
            disagree=(primary_is_hallucination != shadow_is_hallucination),
        )

    def _shadow_done_callback(self, future):
        """Handle shadow future completion (catch exceptions)."""
        exc = future.exception()
        if exc:
            logger.debug("Shadow transcription thread error", error=str(exc))
            self._shadow_error_count += 1

    def get_segment_warning_counts(self) -> Dict[str, Dict[str, Any]]:
        """Get rolling 1-hour warning counts by code."""
        now = time.time()
        cutoff = now - self._segment_warning_window
        result = {}
        for code, timestamps in self._segment_warnings.items():
            # Prune old entries
            timestamps[:] = [t for t in timestamps if t > cutoff]
            if timestamps:
                result[code] = {
                    "count": len(timestamps),
                    "last_seen": datetime.utcfromtimestamp(timestamps[-1]).isoformat() + "Z",
                }
        return result

    def shutdown(self):
        """Clean up background resources."""
        if self._shadow_executor:
            self._shadow_executor.shutdown(wait=False)

    def get_stats(self) -> Dict[str, Any]:
        """Get pipeline statistics."""
        stats = {
            "node_id": self.node_id,
            "mode": "fm",
            "processed_count": self._processed_count,
            "transcribed_count": self._transcribed_count,
            "filtered_count": self._filtered_count,
            "beacon_count": self._beacon_count,
            "kerchunk_count": self._kerchunk_count,
            "bleedover_count": self._bleedover_count,
            "morse_count": self._morse_count,
            "callsigns_extracted": self._callsigns_extracted,
            "error_count": self._error_count,
            "transcription_failed_count": self._transcription_failed_count,
            "synapse_published_count": self._synapse_published_count,
            "transcription_enabled": self.transcription_enabled,
            "whisper_available": self._whisper_available,
            "emitter": self.emitter.get_stats(),
            "synapse": self.synapse_publisher.get_stats(),
        }
        if self._shadow_whisper:
            stats["shadow_whisper"] = {
                "enabled": True,
                "url": settings.shadow_whisper_api_url,
                "available": self._shadow_available,
                "comparisons": self._shadow_count,
                "errors": self._shadow_error_count,
            }
        return stats
