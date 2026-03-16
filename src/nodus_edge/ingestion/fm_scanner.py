"""
FM Ham Radio Scanner for Nodus Edge.

Scans configured frequencies sequentially, captures audio when activity
is detected (squelch opens), and segments speech using VAD.

Uses rtl_fm for FM demodulation from RTL-SDR hardware.
"""

import array
import fcntl
import io
import os
import select
import struct
import subprocess
import threading
import time
import wave
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import structlog

from ..config import settings

logger = structlog.get_logger(__name__)

# Try to import webrtcvad, fall back to simple energy-based detection
try:
    import webrtcvad
    HAS_WEBRTCVAD = True
except ImportError:
    HAS_WEBRTCVAD = False
    logger.warning("webrtcvad not installed, using simple energy-based VAD")


class SimpleVAD:
    """Simple energy-based voice activity detection fallback."""

    def __init__(self, threshold: float = 500.0):
        self.threshold = threshold

    def is_speech(self, frame: bytes, sample_rate: int) -> bool:
        """Check if frame contains speech based on energy."""
        if len(frame) < 2:
            return False
        # Calculate RMS energy
        samples = [int.from_bytes(frame[i:i+2], 'little', signed=True)
                   for i in range(0, len(frame), 2)]
        if not samples:
            return False
        rms = (sum(s*s for s in samples) / len(samples)) ** 0.5
        return rms > self.threshold


class AudioBuffer:
    """Buffer for accumulating audio frames during speech."""

    def __init__(self, sample_rate: int = 16000, sample_width: int = 2):
        self.sample_rate = sample_rate
        self.sample_width = sample_width
        self.frames: List[bytes] = []
        self.start_time: Optional[datetime] = None

    def add_frame(self, frame: bytes) -> None:
        """Add a frame to the buffer."""
        if self.start_time is None:
            self.start_time = datetime.utcnow()
        self.frames.append(frame)

    def clear(self) -> None:
        """Clear the buffer."""
        self.frames = []
        self.start_time = None

    def duration_seconds(self) -> float:
        """Calculate buffer duration in seconds."""
        total_bytes = sum(len(f) for f in self.frames)
        return total_bytes / (self.sample_rate * self.sample_width)

    def to_wav_bytes(self) -> bytes:
        """Convert buffer to WAV format bytes."""
        output = io.BytesIO()
        with wave.open(output, 'wb') as wav:
            wav.setnchannels(1)
            wav.setsampwidth(self.sample_width)
            wav.setframerate(self.sample_rate)
            wav.writeframes(b''.join(self.frames))
        return output.getvalue()

    def is_empty(self) -> bool:
        """Check if buffer is empty."""
        return len(self.frames) == 0


class FMScanner:
    """
    Sequential frequency scanner with activity-triggered capture.

    Cycles through configured frequencies, dwells on each for a configured
    time, and captures audio when squelch opens (activity detected).
    """

    def __init__(
        self,
        frequencies: Optional[List[int]] = None,
        on_segment: Optional[Callable[[Path, int], None]] = None,
    ):
        """
        Initialize FM scanner.

        Args:
            frequencies: List of frequencies to scan in Hz
            on_segment: Callback when segment is captured (path, frequency_hz)
        """
        self.frequencies = frequencies or settings.fm_frequencies
        self.on_segment = on_segment

        self.dwell_seconds = settings.fm_dwell_seconds
        self.idle_timeout_seconds = settings.fm_idle_timeout_seconds
        self.silence_timeout = settings.fm_silence_timeout_seconds
        self.squelch_threshold = settings.fm_squelch_threshold
        self.sample_rate = settings.fm_sample_rate_hz
        self.output_sample_rate = settings.fm_output_sample_rate_hz
        self.max_segment_seconds = settings.fm_segment_max_seconds
        self.min_segment_seconds = settings.fm_segment_min_seconds
        self.device_index = settings.fm_rtl_device_index
        self.gain = settings.fm_gain
        self.capture_dir = settings.fm_capture_path

        self.max_freq_dwell_seconds = settings.fm_max_freq_dwell_seconds

        self.current_index = 0
        self._shutdown_event = threading.Event()
        self._rtl_process: Optional[subprocess.Popen] = None
        self._sox_process: Optional[subprocess.Popen] = None
        self._scan_thread: Optional[threading.Thread] = None

        # VAD setup
        if HAS_WEBRTCVAD:
            self._vad = webrtcvad.Vad(2)  # Aggressiveness 0-3
        else:
            self._vad = SimpleVAD()

        self._running = False

        # Data stream watchdog
        self._last_data_received_at: Optional[float] = None
        self._watchdog_timeout_seconds = settings.fm_watchdog_timeout_seconds
        self._watchdog_restart_count = 0

        # Segment-level watchdog — catches wedged SDR that still produces
        # tiny startup bursts (defeating the byte-level watchdog).
        self._segment_watchdog_timeout = settings.fm_segment_watchdog_timeout_seconds
        self._last_segment_monotonic: Optional[float] = None
        self._scan_start_monotonic: Optional[float] = None
        self._usb_reset_count: int = 0

        # Capture stats
        self._last_segment_captured_at: Optional[datetime] = None
        self._segments_captured_count: int = 0
        self._hourly_segment_times: List[float] = []

        # Noise channel detection — skip channels where squelch stays
        # continuously open (static/carrier breaking through squelch)
        self._noise_strikes: Dict[int, int] = {}   # freq -> consecutive noise hits
        self._noisy_until: Dict[int, float] = {}    # freq -> monotonic skip-until time
        self._noise_cooldown_seconds: float = 300.0  # 5 min cooldown
        self._noise_strike_threshold: int = 1        # skip after 1 full-noise visit

    def is_noisy(self, frequency_hz: int) -> bool:
        """Check if a channel is temporarily skipped due to noise."""
        skip_until = self._noisy_until.get(frequency_hz)
        if skip_until is not None:
            if time.monotonic() < skip_until:
                return True
            # Cooldown expired — allow probing again
            del self._noisy_until[frequency_hz]
        return False

    def _record_noise_result(self, frequency_hz: int, was_noisy: bool) -> None:
        """Track whether a frequency visit was noise (squelch never closed)."""
        if was_noisy:
            strikes = self._noise_strikes.get(frequency_hz, 0) + 1
            self._noise_strikes[frequency_hz] = strikes
            if strikes >= self._noise_strike_threshold:
                self._noisy_until[frequency_hz] = (
                    time.monotonic() + self._noise_cooldown_seconds
                )
                logger.info(
                    "Channel marked noisy — skipping",
                    frequency_mhz=frequency_hz / 1_000_000,
                    cooldown_seconds=self._noise_cooldown_seconds,
                    strikes=strikes,
                )
        else:
            # Clean visit — reset strikes
            self._noise_strikes.pop(frequency_hz, None)
            self._noisy_until.pop(frequency_hz, None)

    def start(self) -> None:
        """Start the scanner in a background thread."""
        if self._running:
            logger.warning("FM scanner already running")
            return

        if not self.frequencies:
            logger.error("No frequencies configured for FM scanning")
            return

        self._shutdown_event.clear()
        self._running = True
        self._scan_thread = threading.Thread(target=self._scan_loop, daemon=True)
        self._scan_thread.start()
        logger.info(
            "FM scanner started",
            frequencies=[f/1_000_000 for f in self.frequencies],
            dwell_seconds=self.dwell_seconds,
        )

    def stop(self) -> None:
        """Stop the scanner."""
        if not self._running:
            return

        logger.info("Stopping FM scanner...")
        self._shutdown_event.set()
        self._cleanup_processes()

        if self._scan_thread:
            self._scan_thread.join(timeout=5)
            self._scan_thread = None

        self._running = False
        logger.info("FM scanner stopped")

    def _scan_loop(self) -> None:
        """Main scanning loop."""
        logger.info("FM scan loop started")
        now = time.monotonic()
        self._last_data_received_at = now
        self._scan_start_monotonic = now

        while not self._shutdown_event.is_set():
            if not self.frequencies:
                time.sleep(1)
                continue

            # Data stream watchdog: if no full audio frames from any frequency
            # for too long, the USB/SDR is likely dead. Kill and restart.
            if self._last_data_received_at is not None:
                silence = time.monotonic() - self._last_data_received_at
                if silence > self._watchdog_timeout_seconds:
                    logger.warning(
                        "Data stream watchdog triggered — no data from any frequency",
                        silence_seconds=round(silence, 1),
                        restart_count=self._watchdog_restart_count,
                    )
                    self._cleanup_processes()
                    self._watchdog_restart_count += 1
                    self._last_data_received_at = time.monotonic()
                    time.sleep(5.0)
                    continue

            # Segment watchdog: if no segments captured for an extended period,
            # the SDR may be wedged (responds to USB control but produces no
            # real audio — startup bursts can defeat the byte-level watchdog).
            # Escalate to USB device reset.
            seg_ref = self._last_segment_monotonic or self._scan_start_monotonic
            if seg_ref is not None:
                seg_silence = time.monotonic() - seg_ref
                if seg_silence > self._segment_watchdog_timeout:
                    logger.warning(
                        "Segment watchdog triggered — no captures, attempting USB reset",
                        silence_minutes=round(seg_silence / 60, 1),
                        usb_reset_count=self._usb_reset_count,
                    )
                    self._cleanup_processes()
                    self._attempt_usb_reset()
                    # Reset timers to avoid rapid retriggers
                    now = time.monotonic()
                    self._last_data_received_at = now
                    self._last_segment_monotonic = now
                    time.sleep(5.0)
                    continue

            freq = self.frequencies[self.current_index]
            logger.debug(
                "Tuning to frequency",
                frequency_mhz=freq / 1_000_000,
                index=self.current_index,
            )

            try:
                self._monitor_frequency(freq)
            except Exception as e:
                logger.error("Error monitoring frequency", frequency=freq, error=str(e))

            # Move to next frequency
            self.current_index = (self.current_index + 1) % len(self.frequencies)

        logger.info("FM scan loop ended")

    def _monitor_frequency(self, frequency_hz: int) -> None:
        """
        Monitor a single frequency for activity.

        Stays on the frequency for dwell_seconds unless activity is detected,
        in which case it captures until silence.
        """
        # Start rtl_fm -> sox pipeline
        self._start_rtl_pipeline(frequency_hz)

        if self._sox_process is None:
            logger.warning("Failed to start rtl_fm pipeline")
            return

        buffer = AudioBuffer(sample_rate=self.output_sample_rate)
        frame_size = int(self.output_sample_rate * 0.03) * 2  # 30ms frames, 16-bit
        frame_count = 0
        in_speech = False
        had_activity = False  # Track if we ever detected speech on this freq
        squelch_closed_naturally = False  # Did squelch ever close during this visit?
        freq_start_time = time.monotonic()  # For max dwell enforcement

        # Use select() for non-blocking reads because rtl_fm outputs
        # NOTHING when squelch is closed (not silence - literally no data).
        # Without this, read() blocks forever on quiet frequencies.
        sox_fd = self._sox_process.stdout.fileno()
        read_timeout = 0.05  # 50ms poll interval — faster detection within dwell window
        no_data_elapsed = 0.0  # Track time with no data from rtl_fm

        try:
            while not self._shutdown_event.is_set():
                # Read audio frame from sox output
                if self._sox_process.stdout is None:
                    break

                # Check if data is available (non-blocking)
                ready, _, _ = select.select([sox_fd], [], [], read_timeout)

                if not ready:
                    # No data available - squelch is likely closed
                    no_data_elapsed += read_timeout
                    if in_speech:
                        # Transmission ended — save segment after silence_timeout,
                        # but stay on frequency (scanner delay) waiting for more traffic.
                        if no_data_elapsed >= self.silence_timeout:
                            if not buffer.is_empty() and buffer.duration_seconds() >= self.min_segment_seconds:
                                self._save_segment(buffer, frequency_hz)
                                buffer.clear()
                                frame_count = 0
                            in_speech = False
                            squelch_closed_naturally = True
                            # Don't reset no_data_elapsed — let it keep ticking
                            # toward idle_timeout so scanner delay works correctly.
                    else:
                        # No activity yet — use dwell/idle timeout
                        timeout = self.idle_timeout_seconds if had_activity else self.dwell_seconds
                        if no_data_elapsed >= timeout:
                            break
                    continue

                # Data available - read a frame
                no_data_elapsed = 0.0
                frame = os.read(sox_fd, frame_size)
                if not frame:
                    break

                # Accumulate partial reads into full frames
                while len(frame) < frame_size:
                    ready, _, _ = select.select([sox_fd], [], [], read_timeout)
                    if not ready:
                        break
                    chunk = os.read(sox_fd, frame_size - len(frame))
                    if not chunk:
                        break
                    frame += chunk

                # Only reset data watchdog on full frames — filters out tiny
                # startup bursts from rtl_fm that defeat the watchdog when
                # the SDR is wedged but still producing control traffic.
                if len(frame) >= frame_size:
                    self._last_data_received_at = time.monotonic()

                if len(frame) < frame_size:
                    # Pad incomplete frame
                    frame = frame + b'\x00' * (frame_size - len(frame))

                frame_count += 1

                # Squelch-based segmentation: buffer ALL frames while
                # data flows (squelch open). Segmentation happens when
                # squelch closes (no data timeout) or max duration hit.
                if not in_speech:
                    in_speech = True
                    had_activity = True
                    logger.debug("Squelch open", frequency_mhz=frequency_hz / 1_000_000)
                buffer.add_frame(frame)

                # Check max duration
                if buffer.duration_seconds() >= self.max_segment_seconds:
                    self._save_segment(buffer, frequency_hz)
                    buffer.clear()
                    in_speech = False
                    frame_count = 0

                    # Max dwell: force rotation to next frequency
                    if time.monotonic() - freq_start_time >= self.max_freq_dwell_seconds:
                        logger.info(
                            "Max frequency dwell reached, rotating",
                            frequency_mhz=frequency_hz / 1_000_000,
                            dwell_seconds=self.max_freq_dwell_seconds,
                        )
                        break

        finally:
            self._cleanup_processes()

            # Save any remaining buffered audio
            if not buffer.is_empty() and buffer.duration_seconds() >= self.min_segment_seconds:
                self._save_segment(buffer, frequency_hz)

            # Track noise: if squelch never closed naturally, this channel
            # is producing continuous static/carrier — mark for skipping.
            self._record_noise_result(
                frequency_hz,
                was_noisy=(had_activity and not squelch_closed_naturally),
            )

    def _start_rtl_pipeline(self, frequency_hz: int) -> None:
        """Start rtl_fm -> sox pipeline for the given frequency."""
        self._cleanup_processes()

        try:
            # rtl_fm command
            rtl_cmd = [
                "rtl_fm",
                "-d", str(self.device_index),
                "-f", str(frequency_hz),
                "-M", "fm",
                "-s", str(self.sample_rate),
                "-l", str(self.squelch_threshold),
                "-E", "deemp",
            ]

            if self.gain != "auto":
                rtl_cmd.extend(["-g", self.gain])

            # sox command for resampling
            sox_cmd = [
                "sox",
                "-t", "raw",
                "-r", str(self.sample_rate),
                "-e", "signed",
                "-b", "16",
                "-c", "1",
                "-",  # stdin
                "-t", "raw",
                "-r", str(self.output_sample_rate),
                "-",  # stdout
            ]

            # Start rtl_fm
            self._rtl_process = subprocess.Popen(
                rtl_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            )

            # Pipe rtl_fm output through sox
            self._sox_process = subprocess.Popen(
                sox_cmd,
                stdin=self._rtl_process.stdout,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            )

            # Allow rtl_fm to close its stdout when sox closes
            if self._rtl_process.stdout:
                self._rtl_process.stdout.close()

        except FileNotFoundError as e:
            logger.error("Required tool not found", error=str(e))
            self._cleanup_processes()
        except Exception as e:
            logger.error("Failed to start rtl_fm pipeline", error=str(e))
            self._cleanup_processes()

    def _cleanup_processes(self) -> None:
        """Clean up subprocess resources."""
        if self._sox_process:
            try:
                self._sox_process.terminate()
                self._sox_process.wait(timeout=0.5)
            except Exception:
                try:
                    self._sox_process.kill()
                except Exception:
                    pass
            self._sox_process = None

        if self._rtl_process:
            try:
                self._rtl_process.terminate()
                self._rtl_process.wait(timeout=0.5)
            except Exception:
                try:
                    self._rtl_process.kill()
                except Exception:
                    pass
            self._rtl_process = None

    def _attempt_usb_reset(self) -> bool:
        """Reset the RTL-SDR USB device via ioctl.

        Recovers wedged dongles that respond to USB control commands but
        produce no sample data.  Tries the USBDEVFS_RESET ioctl first
        (works in LXC containers with device passthrough), then falls
        back to the ``usbreset`` CLI tool.
        """
        USBDEVFS_RESET = 0x5514
        RTL_SDR_VID = "0bda"
        RTL_SDR_PID = "2838"

        # --- attempt 1: ioctl via sysfs lookup ---
        try:
            usb_root = Path("/sys/bus/usb/devices")
            if usb_root.exists():
                for dev_dir in usb_root.iterdir():
                    vid_path = dev_dir / "idVendor"
                    pid_path = dev_dir / "idProduct"
                    if not vid_path.exists() or not pid_path.exists():
                        continue
                    if (vid_path.read_text().strip() == RTL_SDR_VID
                            and pid_path.read_text().strip() == RTL_SDR_PID):
                        busnum = int((dev_dir / "busnum").read_text().strip())
                        devnum = int((dev_dir / "devnum").read_text().strip())
                        dev_path = f"/dev/bus/usb/{busnum:03d}/{devnum:03d}"
                        fd = os.open(dev_path, os.O_WRONLY)
                        try:
                            fcntl.ioctl(fd, USBDEVFS_RESET, 0)
                        finally:
                            os.close(fd)
                        self._usb_reset_count += 1
                        logger.info("USB device reset via ioctl", path=dev_path)
                        return True
        except Exception as e:
            logger.debug("ioctl USB reset failed, trying usbreset CLI", error=str(e))

        # --- attempt 2: usbreset command ---
        try:
            result = subprocess.run(
                ["usbreset", f"{RTL_SDR_VID}:{RTL_SDR_PID}"],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0:
                self._usb_reset_count += 1
                logger.info("USB device reset via usbreset command")
                return True
            logger.warning("usbreset command failed", stderr=result.stderr.strip())
        except FileNotFoundError:
            logger.warning("USB reset unavailable — neither ioctl nor usbreset worked")
        except Exception as e:
            logger.error("USB reset failed", error=str(e))

        return False

    def _save_segment(self, buffer: AudioBuffer, frequency_hz: int) -> None:
        """Save audio buffer to WAV file and trigger callback."""
        if buffer.is_empty():
            return

        duration = buffer.duration_seconds()
        if duration < self.min_segment_seconds:
            logger.debug(
                "Segment too short, discarding",
                duration=duration,
                min_duration=self.min_segment_seconds,
            )
            return

        # Generate filename: YYYYMMDD_HHMMSS_FREQHz.wav
        timestamp = buffer.start_time or datetime.utcnow()
        filename = f"{timestamp.strftime('%Y%m%d_%H%M%S')}_{frequency_hz}Hz.wav"
        filepath = self.capture_dir / filename

        try:
            # Write WAV file with audio normalization
            wav_bytes = self._normalize_audio(buffer.to_wav_bytes())
            with open(filepath, 'wb') as f:
                f.write(wav_bytes)

            logger.info(
                "FM segment captured",
                filename=filename,
                frequency_mhz=frequency_hz / 1_000_000,
                duration_seconds=round(duration, 2),
            )

            # Update capture stats
            self._last_segment_captured_at = datetime.utcnow()
            self._last_segment_monotonic = time.monotonic()
            self._segments_captured_count += 1
            self._hourly_segment_times.append(time.monotonic())

            # Trigger callback
            if self.on_segment:
                self.on_segment(filepath, frequency_hz)

        except Exception as e:
            logger.error("Failed to save segment", error=str(e), filepath=str(filepath))

    def get_capture_stats(self) -> Dict[str, Any]:
        """Get capture health stats for heartbeat reporting."""
        now = time.monotonic()
        cutoff = now - 3600
        self._hourly_segment_times = [t for t in self._hourly_segment_times if t >= cutoff]

        return {
            "last_segment_captured_at": (
                self._last_segment_captured_at.isoformat()
                if self._last_segment_captured_at else None
            ),
            "segments_total": self._segments_captured_count,
            "segments_last_hour": len(self._hourly_segment_times),
            "watchdog_restarts": self._watchdog_restart_count,
            "usb_resets": self._usb_reset_count,
            "current_frequency_mhz": (
                self.current_frequency / 1_000_000
                if self.current_frequency else None
            ),
        }

    @staticmethod
    def _normalize_audio(wav_bytes: bytes, target_rms: float = 0.15) -> bytes:
        """Normalize WAV audio to a target RMS level for reliable transcription."""
        with io.BytesIO(wav_bytes) as inp:
            with wave.open(inp, 'rb') as wav_in:
                params = wav_in.getparams()
                raw = wav_in.readframes(params.nframes)

        samples = array.array('h')
        samples.frombytes(raw)

        if not samples:
            return wav_bytes

        # Calculate current RMS
        rms = (sum(s * s for s in samples) / len(samples)) ** 0.5 / 32768.0

        if rms < 0.001:
            return wav_bytes  # Silence, don't amplify noise

        gain = target_rms / rms
        # Clamp gain to avoid extreme amplification of very quiet noise
        gain = min(gain, 30.0)

        # Apply gain with clipping protection
        normalized = array.array('h', (
            max(-32768, min(32767, int(s * gain))) for s in samples
        ))

        output = io.BytesIO()
        with wave.open(output, 'wb') as wav_out:
            wav_out.setparams(params)
            wav_out.writeframes(normalized.tobytes())
        return output.getvalue()

    @property
    def is_running(self) -> bool:
        """Check if scanner is running."""
        return self._running

    @property
    def current_frequency(self) -> Optional[int]:
        """Get currently monitored frequency."""
        if not self.frequencies:
            return None
        return self.frequencies[self.current_index]
