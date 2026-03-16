"""
HF Audio Capture — VOX-triggered recording from USB sound card.

Monitors an ALSA audio device (USB sound card connected to radio),
starts recording when audio level exceeds a threshold (VOX open),
and stops after a configurable hang time of silence (VOX close).

Outputs WAV files to a capture directory, named by timestamp and
current radio frequency/mode from CAT reader state.
"""

import logging
import os
import struct
import threading
import time
import wave
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, List, Optional

logger = logging.getLogger(__name__)

# Minimum frames before computing RMS (avoid division artifacts on tiny buffers)
MIN_RMS_FRAMES = 64


def compute_rms_db(audio_data: bytes, sample_width: int = 2) -> float:
    """Compute RMS level in dBFS from raw PCM audio bytes."""
    if len(audio_data) < sample_width * MIN_RMS_FRAMES:
        return -100.0

    if sample_width == 2:
        fmt = f"<{len(audio_data) // 2}h"
        samples = struct.unpack(fmt, audio_data)
    else:
        return -100.0

    if not samples:
        return -100.0

    sum_sq = sum(s * s for s in samples)
    rms = (sum_sq / len(samples)) ** 0.5

    if rms < 1:
        return -100.0

    # dBFS relative to 16-bit max (32767)
    return 20 * (rms / 32767.0).__class__.__import__("math").log10(rms / 32767.0) if rms > 0 else -100.0


def _rms_dbfs(audio_data: bytes) -> float:
    """Compute RMS level in dBFS from 16-bit PCM audio bytes."""
    import math

    n_samples = len(audio_data) // 2
    if n_samples < MIN_RMS_FRAMES:
        return -100.0

    fmt = f"<{n_samples}h"
    samples = struct.unpack(fmt, audio_data)

    sum_sq = sum(s * s for s in samples)
    rms = (sum_sq / n_samples) ** 0.5

    if rms < 1:
        return -100.0

    return 20 * math.log10(rms / 32767.0)


class HFAudioCapture:
    """
    VOX-triggered audio capture from a USB sound card.

    Monitors audio input continuously. When signal exceeds vox_threshold_db,
    starts recording. Stops after vox_hang_time_seconds of silence. Outputs
    WAV files to capture_dir.

    Args:
        capture_dir: Directory to write WAV files.
        device: ALSA device name (default "default").
        sample_rate: Audio sample rate in Hz.
        vox_threshold_db: dBFS threshold to trigger recording.
        vox_hang_time: Seconds of silence before stopping.
        max_segment_seconds: Maximum segment length.
        min_segment_seconds: Minimum segment length (shorter segments discarded).
        on_segment: Callback when a new WAV file is ready.
    """

    def __init__(
        self,
        capture_dir: str,
        device: str = "default",
        sample_rate: int = 48000,
        vox_threshold_db: float = -40.0,
        vox_hang_time: float = 2.0,
        max_segment_seconds: int = 120,
        min_segment_seconds: float = 1.0,
        on_segment: Optional[Callable[[str], None]] = None,
    ):
        self.capture_dir = Path(capture_dir)
        self.device = device
        self.sample_rate = sample_rate
        self.vox_threshold_db = vox_threshold_db
        self.vox_hang_time = vox_hang_time
        self.max_segment_seconds = max_segment_seconds
        self.min_segment_seconds = min_segment_seconds
        self.on_segment = on_segment

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

        # Recording state
        self._recording = False
        self._silence_start: Optional[float] = None
        self._record_start: Optional[float] = None
        self._frames: List[bytes] = []
        self._current_filename: Optional[str] = None

        # Stats
        self.segments_captured = 0
        self.segments_discarded = 0

    def start(self) -> bool:
        """Start audio capture in a background thread."""
        self.capture_dir.mkdir(parents=True, exist_ok=True)

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._capture_loop, daemon=True, name="hf-audio-capture"
        )
        self._thread.start()
        logger.info(
            "HF audio capture started (device=%s, rate=%d, vox=%.1f dBFS, hang=%.1fs)",
            self.device, self.sample_rate, self.vox_threshold_db, self.vox_hang_time,
        )
        return True

    def stop(self) -> None:
        """Stop capture and finalize any in-progress recording."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5.0)
        logger.info(
            "HF audio capture stopped (captured=%d, discarded=%d)",
            self.segments_captured, self.segments_discarded,
        )

    def _capture_loop(self) -> None:
        """Main capture loop using sounddevice."""
        try:
            import sounddevice as sd
        except ImportError:
            logger.error("sounddevice not installed — HF audio capture unavailable")
            return
        except Exception as e:
            logger.error("sounddevice import failed: %s", e)
            return

        chunk_duration = 0.1  # 100ms chunks
        chunk_samples = int(self.sample_rate * chunk_duration)

        try:
            stream = sd.RawInputStream(
                samplerate=self.sample_rate,
                blocksize=chunk_samples,
                device=self.device if self.device != "default" else None,
                channels=1,
                dtype="int16",
            )
            stream.start()
        except Exception as e:
            logger.error("Failed to open audio device '%s': %s", self.device, e)
            logger.info("HF audio capture entering standby (no audio device)")
            # Standby: wait for stop signal
            self._stop_event.wait()
            return

        logger.info("Audio stream opened on device '%s'", self.device)

        try:
            while not self._stop_event.is_set():
                data, overflowed = stream.read(chunk_samples)
                if overflowed:
                    logger.debug("Audio buffer overflow")

                audio_bytes = bytes(data)
                level_db = _rms_dbfs(audio_bytes)

                if level_db >= self.vox_threshold_db:
                    # Signal present
                    self._silence_start = None
                    if not self._recording:
                        self._start_recording()
                    self._frames.append(audio_bytes)
                else:
                    # Silence
                    if self._recording:
                        self._frames.append(audio_bytes)
                        if self._silence_start is None:
                            self._silence_start = time.time()
                        elif time.time() - self._silence_start >= self.vox_hang_time:
                            self._stop_recording()

                # Check max duration
                if self._recording and self._record_start:
                    elapsed = time.time() - self._record_start
                    if elapsed >= self.max_segment_seconds:
                        logger.debug("Max segment duration reached (%.0fs)", elapsed)
                        self._stop_recording()

        except Exception as e:
            logger.error("Audio capture error: %s", e)
        finally:
            if self._recording:
                self._stop_recording()
            stream.stop()
            stream.close()

    def _start_recording(self) -> None:
        """Begin a new recording."""
        self._recording = True
        self._record_start = time.time()
        self._silence_start = None
        self._frames = []
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        self._current_filename = f"{ts}.wav"
        logger.debug("VOX open — recording started")

    def _stop_recording(self) -> None:
        """Finalize recording and write WAV file."""
        self._recording = False
        duration = time.time() - self._record_start if self._record_start else 0

        if duration < self.min_segment_seconds:
            logger.debug("Segment too short (%.2fs < %.2fs), discarding", duration, self.min_segment_seconds)
            self.segments_discarded += 1
            self._frames = []
            return

        if not self._frames:
            return

        filepath = self.capture_dir / self._current_filename
        try:
            audio_data = b"".join(self._frames)
            with wave.open(str(filepath), "wb") as wf:
                wf.setnchannels(1)
                wf.setsampwidth(2)  # 16-bit
                wf.setframerate(self.sample_rate)
                wf.writeframes(audio_data)

            self.segments_captured += 1
            logger.debug(
                "VOX close — saved %s (%.1fs, %d bytes)",
                filepath.name, duration, len(audio_data),
            )

            if self.on_segment:
                self.on_segment(str(filepath))

        except Exception as e:
            logger.error("Failed to write WAV: %s", e)

        self._frames = []
        self._record_start = None
