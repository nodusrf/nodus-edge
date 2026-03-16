"""
Trunk Recorder JSON -> TranscriptSegment.v1 mapper.

Converts Trunk Recorder's call JSON format to the canonical
TranscriptSegment.v1 schema, with ORION talkgroup enrichment.

Adapted for Nodus Edge.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List
from uuid import uuid4

from .. import __version__
from ..schema import (
    TranscriptSegmentV1,
    RFChannel,
    AudioMetadata,
    P25Metadata,
    CallEvent,
    TalkgroupMetadata,
    Transcription,
)
from .tr_schema import TRCallJSON, TRCallEvent, message_type_to_string


def map_tr_to_segment(
    call: TRCallJSON,
    json_path: Path,
    audio_path: Optional[Path] = None,
    transcription: Optional[Transcription] = None,
    talkgroup_metadata: Optional[TalkgroupMetadata] = None,
    node_id: str = "nodus-edge-p25",
    metro: Optional[str] = None,
) -> TranscriptSegmentV1:
    """
    Map a Trunk Recorder call JSON to TranscriptSegment.v1.

    Args:
        call: Parsed TR call JSON
        json_path: Path to the JSON file
        audio_path: Path to the audio file (optional)
        transcription: Transcription result (optional)
        talkgroup_metadata: ORION-enriched talkgroup data (optional)
        node_id: Source node identifier

    Returns:
        TranscriptSegmentV1 ready for emission to Synapse
    """
    # Map RF channel
    rf_channel = RFChannel(
        frequency_hz=call.freq,
        talkgroup_id=str(call.talkgroup),
        talkgroup_name=call.talkgroup_tag or (
            talkgroup_metadata.alpha_tag if talkgroup_metadata else None
        ),
        talkgroup=talkgroup_metadata,
        system_name=call.short_name or "ORION",
        timeslot=call.tdma_slot if call.tdma_slot > 0 else None,
    )

    # Map audio metadata
    audio_filename = audio_path.name if audio_path else json_path.with_suffix('.wav').name
    audio_filepath = str(audio_path) if audio_path else None
    audio_size = audio_path.stat().st_size if audio_path and audio_path.exists() else None

    audio = AudioMetadata(
        filename=audio_filename,
        filepath=audio_filepath,
        duration_seconds=float(call.call_length) if call.call_length else None,
        file_size_bytes=audio_size,
        sample_rate_hz=8000,
        format="wav",
    )

    # Map P25 metadata
    p25 = P25Metadata(
        protocol="APCO-25",
        event_type=_get_event_type(call),
        phase=2 if call.phase2_tdma else 1,
        encrypted=bool(call.encrypted),
        is_emergency=bool(call.emergency),
        priority=call.priority if call.priority else None,
        grant_type=message_type_to_string(call.message_type),
    )

    # Map call events
    call_events = _map_call_events(call)

    # Get primary source radio ID
    source_radio_id = None
    if call.srcList:
        source_radio_id = str(call.srcList[0].src)
    elif call_events:
        source_radio_id = call_events[0].source_radio_id

    # Calculate confidence
    confidence = _calculate_confidence(call, transcription)

    return TranscriptSegmentV1(
        schema_version="1.0",
        segment_id=uuid4(),
        source_node_id=node_id,
        source_node_version=__version__,
        metro=metro,
        timestamp=datetime.fromtimestamp(call.start_time, tz=timezone.utc),
        captured_at=datetime.now(timezone.utc),
        rf_channel=rf_channel,
        source_radio_id=source_radio_id,
        audio=audio,
        p25=p25,
        call_events=call_events,
        transcription=transcription,
        confidence=confidence,
        source_files={
            "json": str(json_path),
            "audio": str(audio_path) if audio_path else None,
        },
    )


def _map_call_events(call: TRCallJSON) -> List[CallEvent]:
    """Map TR call events to schema CallEvent objects."""
    events = []

    # Use enhanced call_events from NodusNet fork if available
    for evt in call.call_events:
        events.append(CallEvent(
            timestamp=datetime.fromtimestamp(evt.timestamp, tz=timezone.utc),
            event_type=message_type_to_string(evt.message_type) or "UNKNOWN",
            source_radio_id=str(evt.source) if evt.source else None,
            frequency_hz=evt.freq,
            timeslot=evt.tdma_slot if evt.tdma_slot > 0 else None,
            encrypted=bool(evt.encrypted),
            is_emergency=bool(evt.emergency),
            priority=evt.priority if evt.priority else None,
            grant_type=message_type_to_string(evt.message_type),
            opcode=evt.opcode,
        ))

    # Fallback to srcList if no call_events
    if not events and call.srcList:
        for src in call.srcList:
            events.append(CallEvent(
                timestamp=datetime.fromtimestamp(src.time, tz=timezone.utc),
                event_type="UPDATE",
                source_radio_id=str(src.src) if src.src else None,
                frequency_hz=call.freq,
                encrypted=bool(call.encrypted),
                is_emergency=bool(src.emergency),
            ))

    return events


def _get_event_type(call: TRCallJSON) -> str:
    """Determine event type from call metadata."""
    if call.encrypted:
        return "Encrypted Call"
    if call.audio_type == "analog":
        return "Analog Call"
    if call.phase2_tdma:
        return "Group Call (P25 Phase 2)"
    return "Group Call"


def _calculate_confidence(
    call: TRCallJSON,
    transcription: Optional[Transcription],
) -> float:
    """
    Calculate overall segment confidence.

    Factors in:
    - Transcription confidence (if available)
    - Signal quality indicators from TR
    - Error counts from freqList
    """
    scores = []

    # Transcription confidence
    if transcription and transcription.confidence:
        scores.append(transcription.confidence)

    # Signal quality (TR provides signal/noise as dBm, rough heuristic)
    if call.signal and call.signal != 999:  # 999 = unset
        signal_score = max(0.0, min(1.0, (call.signal + 100) / 60))
        scores.append(signal_score)

    # Error rate from freqList
    if call.freqList:
        total_errors = sum(f.error_count for f in call.freqList)
        total_spikes = sum(f.spike_count for f in call.freqList)
        error_penalty = min(1.0, (total_errors + total_spikes) / 100)
        scores.append(1.0 - error_penalty)

    if scores:
        return sum(scores) / len(scores)

    # Encrypted calls get metadata-only confidence
    if call.encrypted:
        return 0.5

    return 0.8
