"""
Segment emitter for Nodus Edge.

Writes TranscriptSegment.v1 JSON files to the output directory
for consumption by Synapse.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from uuid import UUID

import structlog

from ..config import settings
from ..schema import TranscriptSegmentV1, FMTranscriptSegmentV1, APRSPacketSegmentV1

logger = structlog.get_logger(__name__)


class UUIDEncoder(json.JSONEncoder):
    """JSON encoder that handles UUID and datetime objects."""

    def default(self, obj):
        if isinstance(obj, UUID):
            return str(obj)
        if isinstance(obj, datetime):
            # Format as ISO 8601 with Z suffix for UTC
            if obj.tzinfo is None:
                obj = obj.replace(tzinfo=timezone.utc)
            return obj.isoformat().replace("+00:00", "Z")
        return super().default(obj)


class SegmentEmitter:
    """
    Emits TranscriptSegment.v1 as JSON files.

    This is the simplest forwarding mechanism - writes JSON files
    to an output directory where Synapse can pick them up.
    """

    def __init__(self, output_dir: Optional[Path] = None):
        self.output_dir = output_dir or settings.output_path
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._emitted_count = 0

    def emit(self, segment: TranscriptSegmentV1) -> Path:
        """
        Write a segment to the output directory.

        Returns the path to the written file.
        """
        # Generate filename from segment ID and timestamp
        ts = segment.timestamp.strftime("%Y%m%d_%H%M%S")
        filename = f"{ts}_{segment.segment_id}.json"
        output_path = self.output_dir / filename

        # Serialize to JSON
        data = segment.model_dump(mode="json")

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, cls=UUIDEncoder, ensure_ascii=False)

        self._emitted_count += 1

        logger.debug(
            "Segment emitted",
            segment_id=str(segment.segment_id),
            path=filename,
            has_transcription=segment.transcription is not None,
            encrypted=segment.p25.encrypted if segment.p25 else False,
        )

        return output_path

    def emit_batch(self, segments: list[TranscriptSegmentV1]) -> list[Path]:
        """Emit multiple segments."""
        return [self.emit(s) for s in segments]

    def emit_fm(self, segment: FMTranscriptSegmentV1) -> Path:
        """
        Write an FM segment to the output directory.

        Returns the path to the written file.
        """
        # Generate filename from segment ID and timestamp
        ts = segment.timestamp.strftime("%Y%m%d_%H%M%S")
        filename = f"fm_{ts}_{segment.segment_id}.json"
        output_path = self.output_dir / filename

        # Serialize to JSON
        data = segment.model_dump(mode="json")

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, cls=UUIDEncoder, ensure_ascii=False)

        self._emitted_count += 1

        logger.debug(
            "FM segment emitted",
            segment_id=str(segment.segment_id),
            path=filename,
            frequency_hz=segment.rf_channel.frequency_hz,
            has_transcription=segment.transcription is not None,
            callsigns=segment.detected_callsigns or None,
        )

        return output_path

    def emit_aprs(self, segment: APRSPacketSegmentV1) -> Path:
        """Write an APRS packet segment to the output directory."""
        ts = segment.timestamp.strftime("%Y%m%d_%H%M%S")
        filename = f"aprs_{ts}_{segment.segment_id}.json"
        output_path = self.output_dir / filename

        data = segment.model_dump(mode="json")

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, cls=UUIDEncoder, ensure_ascii=False)

        self._emitted_count += 1

        logger.debug(
            "APRS segment emitted",
            segment_id=str(segment.segment_id),
            path=filename,
            from_callsign=segment.from_callsign,
            packet_type=segment.packet_type,
        )

        return output_path

    @property
    def emitted_count(self) -> int:
        """Total number of segments emitted."""
        return self._emitted_count

    def get_stats(self) -> dict:
        """Get emitter statistics."""
        return {
            "output_dir": str(self.output_dir),
            "emitted_count": self._emitted_count,
        }
