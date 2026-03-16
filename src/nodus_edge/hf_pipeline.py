"""
HF Amateur Radio Processing Pipeline for Nodus Edge.

Processes HF audio from USB sound card:
1. VOX-triggered audio capture from ALSA device
2. Read radio metadata from CAT/CI-V (optional)
3. Transcribe audio via Whisper
4. Extract callsigns from transcript
5. Build HFTranscriptSegment.v1
6. Publish to Synapse
"""

import base64
import io
import os
import time
import wave
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4

import structlog

from . import __version__
from .config import settings
from .hallucination_filter import is_whisper_hallucination
from .ingestion.cat_reader import CATPoller, RadioState, create_radio
from .ingestion.hf_audio_capture import HFAudioCapture
from .schema import (
    AudioMetadata,
    HFRFChannel,
    HFTranscriptSegmentV1,
    Transcription,
    frequency_to_band,
)
from .forwarding.synapse_publisher import SynapsePublisher
from .transcription.whisper_client import WhisperClient

logger = structlog.get_logger(__name__)


class HFPipeline:
    """
    Processing pipeline for HF amateur radio.

    Manages audio capture, CAT polling, transcription, and segment publishing.
    """

    def __init__(self, node_id: Optional[str] = None):
        self.node_id = node_id or settings.node_id
        self.station_callsign = settings.hf_station_callsign or None

        # Whisper client
        self.whisper = WhisperClient(
            api_url=settings.whisper_api_url,
            timeout=settings.whisper_timeout_seconds,
        ) if settings.transcription_enabled else None

        # Synapse publisher
        self.synapse_publisher = SynapsePublisher()

        # CAT poller (optional)
        self._cat_poller: Optional[CATPoller] = None

        # Audio capture
        self._audio_capture: Optional[HFAudioCapture] = None

        # Segment callbacks (dashboard, etc.)
        self._segment_callbacks: List[Callable] = []

        # Stats
        self._processed_count = 0
        self._transcribed_count = 0
        self._filtered_count = 0
        self._error_count = 0
        self._synapse_published_count = 0

    def start(self) -> None:
        """Start audio capture and CAT polling."""
        # Start CAT poller if configured
        if settings.hf_cat_protocol != "none":
            radio = create_radio(
                protocol=settings.hf_cat_protocol,
                port=settings.hf_cat_port,
                baud=settings.hf_cat_baud,
                address=settings.hf_cat_address,
            )
            self._cat_poller = CATPoller(radio=radio, poll_interval_ms=200)
            if self._cat_poller.start():
                logger.info("CAT poller started", protocol=settings.hf_cat_protocol)
            else:
                logger.warning("CAT poller failed to start, continuing without CAT")
                self._cat_poller = None

        # Start audio capture
        self._audio_capture = HFAudioCapture(
            capture_dir=str(settings.hf_capture_path),
            device=settings.hf_audio_device,
            sample_rate=settings.hf_audio_sample_rate,
            vox_threshold_db=settings.hf_vox_threshold_db,
            vox_hang_time=settings.hf_vox_hang_time_seconds,
            max_segment_seconds=settings.hf_segment_max_seconds,
            min_segment_seconds=settings.hf_segment_min_seconds,
            on_segment=self._on_audio_segment,
        )
        self._audio_capture.start()

    def stop(self) -> None:
        """Stop capture and polling."""
        if self._audio_capture:
            self._audio_capture.stop()
        if self._cat_poller:
            self._cat_poller.stop()

    def set_segment_callback(self, callback: Callable) -> None:
        """Register a callback invoked with each emitted segment dict."""
        self._segment_callbacks.append(callback)

    # Alias for compatibility with dashboard wiring
    register_segment_callback = set_segment_callback

    def _on_audio_segment(self, wav_path: str) -> None:
        """Callback from audio capture when a new WAV is ready."""
        try:
            self.process_recording(Path(wav_path))
        except Exception as e:
            logger.error("HF segment processing failed", path=wav_path, error=str(e))
            self._error_count += 1

    def process_recording(self, wav_path: Path) -> Optional[HFTranscriptSegmentV1]:
        """Process a single HF WAV recording through the pipeline."""
        if not wav_path.exists():
            logger.warning("WAV file not found", path=str(wav_path))
            return None

        # Read WAV metadata
        try:
            with wave.open(str(wav_path), "rb") as wf:
                duration = wf.getnframes() / wf.getframerate()
                sample_rate = wf.getframerate()
                file_size = wav_path.stat().st_size
        except Exception as e:
            logger.error("Failed to read WAV", path=str(wav_path), error=str(e))
            self._error_count += 1
            return None

        # Get CAT state (or defaults)
        radio_state = self._cat_poller.get_state() if self._cat_poller else RadioState()

        # Transcribe
        transcription = None
        detected_callsigns: List[str] = []
        if self.whisper:
            try:
                result = self.whisper.transcribe(str(wav_path))
                if result and result.get("text", "").strip():
                    text = result["text"].strip()

                    # Hallucination filter
                    if is_whisper_hallucination(text):
                        logger.debug("HF hallucination filtered", text=text[:50])
                        self._filtered_count += 1
                        self._cleanup_wav(wav_path)
                        return None

                    transcription = Transcription(
                        engine="whisper",
                        model=result.get("model", "unknown"),
                        text=text,
                        raw_text=text,
                        confidence=result.get("confidence"),
                        duration_seconds=duration,
                    )
                    self._transcribed_count += 1

                    # Extract callsigns (reuse FM parser if available)
                    detected_callsigns = self._extract_callsigns(text)
            except Exception as e:
                logger.error("Whisper transcription failed", error=str(e))

        # Encode audio to MP3 for transport
        audio_base64 = self._encode_mp3(wav_path)

        # Build segment
        rf_channel = HFRFChannel(
            frequency_hz=radio_state.frequency_hz,
            band=radio_state.band,
            mode=radio_state.mode,
            sideband=radio_state.sideband,
            bandwidth_hz=radio_state.bandwidth_hz,
            s_meter=radio_state.s_meter,
            s_meter_dbm=radio_state.s_meter_dbm,
            power_watts=radio_state.power_watts,
        )

        segment = HFTranscriptSegmentV1(
            segment_id=uuid4(),
            source_node_id=self.node_id,
            source_node_version=__version__,
            station_callsign=self.station_callsign,
            timestamp=datetime.now(timezone.utc),
            rf_channel=rf_channel,
            audio=AudioMetadata(
                filename=wav_path.name,
                filepath=str(wav_path),
                duration_seconds=duration,
                file_size_bytes=file_size,
                sample_rate_hz=sample_rate,
                format="wav",
                audio_data_base64=audio_base64,
            ),
            transcription=transcription,
            detected_callsigns=detected_callsigns,
            signal_type="voice" if transcription else None,
            confidence=transcription.confidence if transcription and transcription.confidence else 0.5,
            source_files={"audio": str(wav_path)},
        )

        self._processed_count += 1

        # Publish to Synapse
        if self.synapse_publisher.enabled:
            success = self.synapse_publisher.publish_hf(segment)
            if success:
                self._synapse_published_count += 1

        # Notify callbacks (dashboard)
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

        logger.info(
            "HF segment processed",
            freq=radio_state.frequency_hz,
            band=str(radio_state.band) if radio_state.band else "unknown",
            mode=str(radio_state.mode) if radio_state.mode else "unknown",
            duration=f"{duration:.1f}s",
            callsigns=detected_callsigns or "none",
            text=transcription.text[:60] if transcription else "no-transcript",
        )

        # Cleanup WAV after processing
        self._cleanup_wav(wav_path)

        return segment

    def _extract_callsigns(self, text: str) -> List[str]:
        """Extract ham radio callsigns from transcript text."""
        try:
            from .ingestion.fm_parser import FMRecordingParser
            parser = FMRecordingParser()
            return parser.extract_callsigns(text)
        except Exception:
            # Fallback: basic callsign regex
            import re
            pattern = r'\b[AKNW][A-Z]?\d[A-Z]{1,3}\b'
            return list(set(re.findall(pattern, text.upper())))

    def _encode_mp3(self, wav_path: Path) -> Optional[str]:
        """Encode WAV to MP3 base64 for transport."""
        try:
            import subprocess
            mp3_path = wav_path.with_suffix(".mp3")
            subprocess.run(
                ["ffmpeg", "-i", str(wav_path), "-q:a", "5", "-y", str(mp3_path)],
                capture_output=True, timeout=30,
            )
            if mp3_path.exists():
                data = mp3_path.read_bytes()
                mp3_path.unlink()
                return base64.b64encode(data).decode("ascii")
        except Exception as e:
            logger.debug("MP3 encode failed", error=str(e))
        return None

    def _cleanup_wav(self, wav_path: Path) -> None:
        """Remove processed WAV file to save disk space."""
        try:
            if wav_path.exists():
                wav_path.unlink()
        except Exception:
            pass

    def get_stats(self) -> Dict[str, Any]:
        """Get pipeline statistics for heartbeat."""
        stats = {
            "node_id": self.node_id,
            "mode": "hf",
            "processed_count": self._processed_count,
            "transcribed_count": self._transcribed_count,
            "filtered_count": self._filtered_count,
            "error_count": self._error_count,
            "synapse_published_count": self._synapse_published_count,
            "station_callsign": self.station_callsign or "",
        }

        # Add CAT state if available
        if self._cat_poller:
            state = self._cat_poller.get_state()
            stats["cat_connected"] = state.connected
            stats["frequency_hz"] = state.frequency_hz
            stats["band"] = str(state.band) if state.band else ""
            stats["mode"] = str(state.mode) if state.mode else ""
        else:
            stats["cat_connected"] = False

        # Add audio capture stats
        if self._audio_capture:
            stats["segments_captured"] = self._audio_capture.segments_captured
            stats["segments_discarded"] = self._audio_capture.segments_discarded

        return stats

    def shutdown(self) -> None:
        """Clean up resources."""
        self.stop()
