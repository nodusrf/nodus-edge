"""
RTLSDR-Airband multichannel FM scanner.

Replaces sequential rtl_fm scanning with simultaneous multi-channel
demodulation. RTLSDR-Airband captures the full 2m band in one FFT pass
and demodulates all configured NFM channels at once — zero scanning
latency, zero missed transmissions.

Output: one MP3 file per transmission per channel (squelch-gated).
This module watches for those files, converts to WAV, normalizes audio,
and fires the same on_segment callback used by FMScanner.
"""

import array
import fcntl
import io
import math
import os
import re
import shutil
import subprocess
import time
import wave
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from threading import Thread, Timer, Event, Lock
from typing import Callable, Dict, Any, List, Optional

import structlog
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileMovedEvent

from ..config import settings

logger = structlog.get_logger(__name__)

# RTLSDR-Airband names files: {template}_{YYYYMMDD}_{HHMMSS}[_{freq}].mp3
# With include_freq=true, freq is appended in Hz.
_AIRBAND_FILENAME_RE = re.compile(
    r"^nodus_(\d{8})_(\d{6})_(\d+)\.mp3$"
)


class _SegmentHandler(FileSystemEventHandler):
    """Watchdog handler that queues completed MP3 files for processing."""

    def __init__(self, on_file: Callable[[Path], None]):
        self._on_file = on_file

    def on_created(self, event: FileCreatedEvent) -> None:
        if not event.is_directory:
            self._check_file(Path(event.src_path))

    def on_moved(self, event: FileMovedEvent) -> None:
        if not event.is_directory:
            self._check_file(Path(event.dest_path))

    def _check_file(self, path: Path) -> None:
        # RTLSDR-Airband writes to .mp3.tmp then renames.
        # Only process final .mp3 files.
        if path.suffix == ".mp3" and not path.name.endswith(".tmp"):
            self._on_file(path)


class AirbandScanner:
    """
    Simultaneous multi-channel FM scanner using RTLSDR-Airband.

    Manages the RTLSDR-Airband process, generates its config, watches for
    new transmission MP3 files, converts them to 16 kHz mono WAV, and fires
    the on_segment callback that feeds into FMPipeline.

    Drop-in replacement for FMScanner — same callback signature, same stats
    interface, same lifecycle (start/stop).
    """

    def __init__(
        self,
        frequencies: List[int],
        on_segment: Callable[..., None],
    ):
        if not frequencies:
            raise ValueError("AirbandScanner requires at least one frequency")

        self.frequencies = sorted(frequencies)
        self.on_segment = on_segment

        # Paths
        self._airband_output_dir = settings.fm_airband_output_path
        self._capture_dir = settings.fm_capture_path
        self._config_path = self._airband_output_dir / "rtlsdr-airband.conf"

        # Hardware
        self._device_index = settings.fm_rtl_device_index
        self._gain = settings.fm_gain
        self._fft_size = settings.fm_airband_fft_size
        self._binary = settings.fm_airband_binary

        # Squelch (mutable at runtime via update_squelch)
        self._squelch_snr_db = settings.fm_airband_squelch_snr_db

        # Audio
        self._min_segment_seconds = settings.fm_segment_min_seconds
        self._normalize_target_rms = 0.15

        # Watchdog
        self._process_check_interval = 10.0  # seconds
        self._segment_watchdog_timeout = settings.fm_segment_watchdog_timeout_seconds

        # State
        self._process: Optional[subprocess.Popen] = None
        self._observer: Optional[Observer] = None
        self._watchdog_thread: Optional[Thread] = None
        self._shutdown_event = Event()
        self._running = False
        self._lock = Lock()
        self._squelch_lock = Lock()       # serializes update_squelch calls
        self._squelch_updating = False     # suppresses watchdog during squelch update

        # Stderr reader (SDR observability — issue #352)
        self._stderr_buffer: deque = deque(maxlen=50)
        self._stderr_lock = Lock()
        self._stderr_thread: Optional[Thread] = None
        self._process_start_time: Optional[float] = None

        # FFT spillover detection buffer
        # Groups segments by capture timestamp; after buffer delay, picks
        # the strongest signal per duration group and drops the rest.
        self._spillover_enabled = settings.fm_spillover_detection_enabled
        self._spillover_buffer_seconds = settings.fm_spillover_buffer_seconds
        self._spillover_duration_tolerance = settings.fm_spillover_duration_tolerance_seconds
        self._pending_groups: Dict[str, List[Dict[str, Any]]] = {}  # capture_ts -> entries
        self._pending_timers: Dict[str, Timer] = {}  # capture_ts -> flush timer
        self._spillover_dropped = 0

        # RF bleedover detection (multi-channel correlation)
        # When a nearby transmitter overloads the SDR, the same clipped signal
        # appears on many channels simultaneously.  If N+ channels fire at the
        # same capture timestamp, it's bleedover — drop them all.
        self._bleedover_enabled = settings.fm_bleedover_detection_enabled
        self._bleedover_min_channels = 3  # 3+ simultaneous = bleedover
        self._bleedover_dropped = 0
        self._bleedover_events = 0  # number of bleedover bursts detected

        # Stats
        self._segments_captured = 0
        self._segments_last_hour: deque = deque()  # timestamps of recent segments
        self._conversions_failed = 0
        self._watchdog_restarts = 0
        self._usb_reset_count = 0
        self._last_segment_at: Optional[datetime] = None
        self._last_segment_monotonic: Optional[float] = None
        self._start_monotonic: Optional[float] = None

        # Calculate center frequency
        if settings.fm_airband_center_freq_hz > 0:
            self._center_freq_hz = settings.fm_airband_center_freq_hz
        else:
            self._center_freq_hz = (min(self.frequencies) + max(self.frequencies)) // 2

        # Verify binary exists
        if not shutil.which(self._binary):
            raise FileNotFoundError(
                f"RTLSDR-Airband binary not found: {self._binary}. "
                "Build from source: git clone https://github.com/rtl-airband/RTLSDR-Airband && "
                "cmake -DNFM=1 -DRTLSDR=1 -DPULSEAUDIO=0 && make && make install"
            )

    def start(self) -> None:
        """Start RTLSDR-Airband process and file watcher."""
        if self._running:
            return

        logger.info(
            "Starting RTLSDR-Airband scanner",
            channels=len(self.frequencies),
            center_freq_mhz=self._center_freq_hz / 1_000_000,
            device_index=self._device_index,
            fft_size=self._fft_size,
            squelch_snr_db=self._squelch_snr_db,
        )

        # Generate config
        self._generate_config()

        # Start RTLSDR-Airband process
        self._start_airband_process()

        # Start stderr reader thread
        self._stderr_thread = Thread(target=self._stderr_reader_loop, daemon=True)
        self._stderr_thread.start()

        # Start file watcher
        handler = _SegmentHandler(on_file=self._on_mp3_file)
        self._observer = Observer()
        self._observer.schedule(handler, str(self._airband_output_dir), recursive=False)
        self._observer.daemon = True
        self._observer.start()

        # Start watchdog thread
        self._start_monotonic = time.monotonic()
        self._last_segment_monotonic = self._start_monotonic
        self._watchdog_thread = Thread(target=self._watchdog_loop, daemon=True)
        self._watchdog_thread.start()

        self._running = True
        logger.info("RTLSDR-Airband scanner started")

    def stop(self) -> None:
        """Stop scanner, process, and watcher."""
        if not self._running:
            return

        logger.info("Stopping RTLSDR-Airband scanner")
        self._shutdown_event.set()

        # Stop file watcher
        if self._observer:
            self._observer.stop()
            self._observer.join(timeout=5)

        # Stop RTLSDR-Airband process
        self._stop_airband_process()

        # Wait for watchdog thread
        if self._watchdog_thread and self._watchdog_thread.is_alive():
            self._watchdog_thread.join(timeout=5)

        # Wait for stderr reader thread
        if self._stderr_thread and self._stderr_thread.is_alive():
            self._stderr_thread.join(timeout=5)

        # Flush any pending spillover buffer entries
        for ts in list(self._pending_timers):
            self._pending_timers[ts].cancel()
        for ts in list(self._pending_groups):
            self._flush_group(ts)

        self._running = False
        logger.info(
            "RTLSDR-Airband scanner stopped",
            segments_captured=self._segments_captured,
            conversions_failed=self._conversions_failed,
            watchdog_restarts=self._watchdog_restarts,
            spillover_dropped=self._spillover_dropped,
            bleedover_events=self._bleedover_events,
            bleedover_dropped=self._bleedover_dropped,
        )

    # ------------------------------------------------------------------
    # Config generation
    # ------------------------------------------------------------------

    def _generate_config(self) -> None:
        """Generate RTLSDR-Airband libconfig configuration file."""
        gain_val = float(self._gain) if self._gain != "auto" else 40.0
        center_mhz = self._center_freq_hz / 1_000_000

        # Build channel entries
        channel_lines = []
        for i, freq_hz in enumerate(self.frequencies):
            freq_mhz = freq_hz / 1_000_000
            squelch_line = ""
            if self._squelch_snr_db > 0:
                squelch_line = (
                    f"      squelch_snr_threshold = "
                    f"{self._squelch_snr_db:.1f};\n"
                )

            comma = "," if i < len(self.frequencies) - 1 else ""
            channel_lines.append(
                f"    {{\n"
                f"      freq = {freq_mhz:.3f};\n"
                f"      modulation = \"nfm\";\n"
                f"{squelch_line}"
                f"      outputs = ({{\n"
                f"        type = \"file\";\n"
                f"        directory = \"{self._airband_output_dir}\";\n"
                f"        filename_template = \"nodus\";\n"
                f"        split_on_transmission = true;\n"
                f"        include_freq = true;\n"
                f"        continuous = false;\n"
                f"      }});\n"
                f"    }}{comma}"
            )

        channels_block = "\n".join(channel_lines)

        config = (
            f"# Auto-generated by Nodus AirbandScanner — do not edit manually\n"
            f"devices = ({{\n"
            f"  type = \"rtlsdr\";\n"
            f"  index = {self._device_index};\n"
            f"  gain = {gain_val:.1f};\n"
            f"  centerfreq = {center_mhz:.3f};\n"
            f"  mode = \"multichannel\";\n"
            f"  channels = (\n"
            f"{channels_block}\n"
            f"  );\n"
            f"}});\n"
        )

        self._config_path.write_text(config)
        logger.info(
            "RTLSDR-Airband config generated",
            path=str(self._config_path),
            channels=len(self.frequencies),
            center_mhz=center_mhz,
            squelch_snr_db=self._squelch_snr_db,
        )

    # ------------------------------------------------------------------
    # Process management
    # ------------------------------------------------------------------

    def _start_airband_process(self) -> None:
        """Start the RTLSDR-Airband subprocess."""
        cmd = [
            self._binary,
            "-f",  # foreground (don't daemonize)
            "-e",  # log to stderr
            "-c", str(self._config_path),
        ]
        try:
            self._process = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
            )
            self._process_start_time = time.monotonic()
            logger.info(
                "RTLSDR-Airband process started",
                pid=self._process.pid,
                cmd=" ".join(cmd),
            )
        except Exception as e:
            logger.error("Failed to start RTLSDR-Airband", error=str(e))
            raise

    def _stop_airband_process(self) -> None:
        """Stop the RTLSDR-Airband subprocess."""
        if not self._process:
            return

        try:
            self._process.terminate()
            try:
                self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait(timeout=3)
            logger.debug("RTLSDR-Airband process stopped", pid=self._process.pid)
        except Exception as e:
            logger.warning("Error stopping RTLSDR-Airband", error=str(e))
        finally:
            self._process = None

    def _restart_airband_process(self) -> None:
        """Restart the RTLSDR-Airband process (watchdog recovery)."""
        logger.warning("Restarting RTLSDR-Airband process")
        self._stop_airband_process()
        time.sleep(2)  # Brief cooldown before restart
        try:
            self._start_airband_process()
            self._watchdog_restarts += 1
        except Exception as e:
            logger.error("Failed to restart RTLSDR-Airband", error=str(e))

    def _stderr_reader_loop(self) -> None:
        """Continuously read stderr from rtl_airband, buffering recent lines."""
        _SIGNIFICANT = ("error", "squelch", "channel", "device", "started", "frequency")
        while not self._shutdown_event.is_set():
            proc = self._process
            if proc is None or proc.stderr is None:
                self._shutdown_event.wait(timeout=1.0)
                continue

            try:
                line = proc.stderr.readline()
            except (ValueError, OSError):
                # Pipe closed (process died/restarted)
                self._shutdown_event.wait(timeout=0.5)
                continue

            if not line:
                # EOF — process exited; wait for restart
                self._shutdown_event.wait(timeout=0.5)
                continue

            line = line.rstrip("\n")
            with self._stderr_lock:
                self._stderr_buffer.append(line)

            lower = line.lower()
            if any(kw in lower for kw in _SIGNIFICANT):
                logger.info("rtl_airband", stderr_line=line)
            else:
                logger.debug("rtl_airband", stderr_line=line)

    def update_squelch(self, snr_db: float) -> dict:
        """Update squelch SNR threshold at runtime and restart the process.

        Thread-safe: a Lock serializes concurrent calls.  If a second call
        arrives while the first is running (stop/sleep/start cycle), the
        second caller waits for the lock, then applies only the *latest*
        requested value — the intermediate restart is skipped.
        """
        # Store the desired value immediately so a queued caller always
        # picks up the most recent request.
        self._squelch_snr_db = snr_db

        if not self._squelch_lock.acquire(blocking=False):
            # Another update is already in progress.  Wait for it to finish,
            # then check whether our value still needs to be applied.
            logger.info(
                "Squelch update coalesced — waiting for in-progress update",
                requested_snr_db=snr_db,
            )
            with self._squelch_lock:
                # The previous holder applied whatever _squelch_snr_db was at
                # that point.  If nobody else changed it since, our value was
                # already applied — skip the redundant restart.
                if self._squelch_snr_db == snr_db:
                    logger.info(
                        "Squelch update already applied by previous cycle",
                        squelch_snr_db=snr_db,
                    )
                    return {"applied": True, "squelch_snr_db": snr_db}
                # Value changed again while we waited — apply the latest.
                return self._apply_squelch_update()

        # We acquired the lock without blocking — apply immediately.
        try:
            return self._apply_squelch_update()
        finally:
            self._squelch_lock.release()

    def _apply_squelch_update(self) -> dict:
        """Internal: stop, reconfigure, restart with current _squelch_snr_db.

        Caller MUST hold _squelch_lock.
        """
        snr_db = self._squelch_snr_db
        self._squelch_updating = True
        try:
            logger.info("Applying squelch update", new_snr_db=snr_db)
            self._stop_airband_process()
            self._generate_config()
            time.sleep(1)
            # Re-check: if another request changed the value while we slept,
            # use the latest value and regenerate config.
            if self._squelch_snr_db != snr_db:
                snr_db = self._squelch_snr_db
                logger.info("Squelch value changed during restart, using latest", snr_db=snr_db)
                self._generate_config()
            self._start_airband_process()
            logger.info("Squelch updated — process restarted", squelch_snr_db=snr_db)
            return {"applied": True, "squelch_snr_db": snr_db}
        finally:
            self._squelch_updating = False

    # ------------------------------------------------------------------
    # File processing
    # ------------------------------------------------------------------

    def _on_mp3_file(self, mp3_path: Path) -> None:
        """Handle a new MP3 file from RTLSDR-Airband."""
        try:
            # Brief delay to ensure file write is fully committed
            time.sleep(0.2)

            if not mp3_path.exists():
                return

            # Check minimum file size (noise filter)
            mp3_size = mp3_path.stat().st_size
            if mp3_size < settings.fm_airband_min_mp3_bytes:
                logger.debug(
                    "MP3 too small, discarding",
                    name=mp3_path.name,
                    size=mp3_size,
                    min_required=settings.fm_airband_min_mp3_bytes,
                )
                mp3_path.unlink(missing_ok=True)
                return

            # Parse filename
            match = _AIRBAND_FILENAME_RE.match(mp3_path.name)
            if not match:
                logger.debug("Ignoring non-matching file", name=mp3_path.name)
                return

            date_str = match.group(1)   # YYYYMMDD
            time_str = match.group(2)   # HHMMSS
            freq_hz = int(match.group(3))

            # Convert MP3 → WAV
            wav_filename = f"{date_str}_{time_str}_{freq_hz}Hz.wav"
            wav_path = self._capture_dir / wav_filename

            if not self._convert_mp3_to_wav(mp3_path, wav_path):
                self._conversions_failed += 1
                logger.warning(
                    "MP3 to WAV conversion failed",
                    mp3=mp3_path.name,
                )
                return

            # Check minimum duration and measure pre-normalization RMS
            duration = 0.0
            rms = 0.0
            try:
                wav_bytes = wav_path.read_bytes()
                with io.BytesIO(wav_bytes) as buf:
                    with wave.open(buf, "rb") as wf:
                        duration = wf.getnframes() / wf.getframerate()
                        raw = wf.readframes(wf.getnframes())
                if duration < self._min_segment_seconds:
                    wav_path.unlink(missing_ok=True)
                    logger.debug(
                        "Segment too short, discarding",
                        duration=f"{duration:.2f}s",
                        min_required=self._min_segment_seconds,
                    )
                    return
                # Compute RMS before normalization (spillover has weaker RMS)
                samples = array.array("h")
                samples.frombytes(raw)
                if samples:
                    rms = math.sqrt(sum(s * s for s in samples) / len(samples)) / 32768.0
            except Exception:
                pass  # If we can't read, process anyway

            capture_ts = f"{date_str}_{time_str}"
            entry = {
                "freq_hz": freq_hz,
                "wav_path": wav_path,
                "mp3_path": mp3_path,
                "duration": duration,
                "rms": rms,
            }

            # Buffer segments by timestamp for cross-channel analysis
            # (spillover detection and/or bleedover detection)
            if self._spillover_enabled or self._bleedover_enabled:
                self._buffer_segment(capture_ts, entry)
            else:
                self._finalize_segment(entry)

        except Exception as e:
            logger.error(
                "Error processing airband file",
                file=mp3_path.name,
                error=str(e),
            )

    # ------------------------------------------------------------------
    # FFT spillover detection
    # ------------------------------------------------------------------

    def _buffer_segment(self, capture_ts: str, entry: Dict[str, Any]) -> None:
        """Add a segment to the spillover buffer and schedule flush."""
        if capture_ts not in self._pending_groups:
            self._pending_groups[capture_ts] = []
        self._pending_groups[capture_ts].append(entry)

        # Cancel existing timer for this timestamp and reset
        if capture_ts in self._pending_timers:
            self._pending_timers[capture_ts].cancel()

        timer = Timer(self._spillover_buffer_seconds, self._flush_group, args=(capture_ts,))
        timer.daemon = True
        timer.start()
        self._pending_timers[capture_ts] = timer

    def _flush_group(self, capture_ts: str) -> None:
        """Flush a buffered timestamp group.

        Two-stage filter:
        1. Bleedover: if N+ channels fire simultaneously, the SDR front end
           is overloaded by a nearby transmitter — drop everything.
        2. Spillover: among remaining segments, group by duration and keep
           the strongest signal per group (FFT bin leakage).
        """
        entries = self._pending_groups.pop(capture_ts, [])
        self._pending_timers.pop(capture_ts, None)

        if not entries:
            return

        # --- Stage 1: Multi-channel bleedover detection ---
        if self._bleedover_enabled and len(entries) >= self._bleedover_min_channels:
            freqs = [e["freq_hz"] / 1_000_000 for e in entries]
            signal_dbs = [
                round(20 * math.log10(e["rms"]), 1) if e["rms"] > 0 else -999.0
                for e in entries
            ]
            logger.warning(
                "🔥 Bleedover detected — %d channels fired simultaneously",
                len(entries),
                channels=len(entries),
                capture_ts=capture_ts,
                frequencies_mhz=freqs,
                signal_dbs=signal_dbs,
            )
            # Clean up all files
            for entry in entries:
                entry["wav_path"].unlink(missing_ok=True)
                if not settings.fm_airband_keep_mp3:
                    entry["mp3_path"].unlink(missing_ok=True)
            self._bleedover_dropped += len(entries)
            self._bleedover_events += 1
            return

        if len(entries) == 1:
            # Single entry, no spillover possible
            self._finalize_segment(entries[0])
            return

        # --- Stage 2: FFT spillover detection ---
        if not self._spillover_enabled:
            # Spillover disabled — finalize everything
            for entry in entries:
                self._finalize_segment(entry)
            return

        # Group by similar duration
        duration_groups: List[List[Dict[str, Any]]] = []
        for entry in entries:
            placed = False
            for group in duration_groups:
                if abs(entry["duration"] - group[0]["duration"]) <= self._spillover_duration_tolerance:
                    group.append(entry)
                    placed = True
                    break
            if not placed:
                duration_groups.append([entry])

        for group in duration_groups:
            if len(group) == 1:
                # Unique duration, no spillover match
                self._finalize_segment(group[0])
                continue

            # Multiple segments with same timestamp + same duration = spillover.
            # Keep the one with highest pre-normalization RMS (strongest signal).
            winner = max(group, key=lambda e: e["rms"])
            for entry in group:
                if entry is winner:
                    self._finalize_segment(entry)
                else:
                    logger.info(
                        "FFT spillover dropped",
                        dropped_freq_mhz=entry["freq_hz"] / 1_000_000,
                        kept_freq_mhz=winner["freq_hz"] / 1_000_000,
                        dropped_rms=round(entry["rms"], 4),
                        kept_rms=round(winner["rms"], 4),
                        duration=round(entry["duration"], 2),
                    )
                    entry["wav_path"].unlink(missing_ok=True)
                    if not settings.fm_airband_keep_mp3:
                        entry["mp3_path"].unlink(missing_ok=True)
                    self._spillover_dropped += 1

    def _finalize_segment(self, entry: Dict[str, Any]) -> None:
        """Normalize audio and fire the on_segment callback."""
        wav_path = entry["wav_path"]
        freq_hz = entry["freq_hz"]
        mp3_path = entry["mp3_path"]
        rms = entry.get("rms", 0.0)

        # Convert pre-normalization RMS to dB for signal strength reporting
        signal_db: Optional[float] = None
        if rms > 0:
            signal_db = round(20 * math.log10(rms), 1)

        # Normalize audio
        try:
            wav_bytes = wav_path.read_bytes()
            normalized = self._normalize_audio(wav_bytes)
            wav_path.write_bytes(normalized)
        except Exception as e:
            logger.debug("Audio normalization failed, using raw", error=str(e))

        # Clean up source MP3 unless keeping for pipeline embedding
        if not settings.fm_airband_keep_mp3:
            mp3_path.unlink(missing_ok=True)

        # Update stats
        now = datetime.now(timezone.utc)
        self._segments_captured += 1
        self._last_segment_at = now
        self._last_segment_monotonic = time.monotonic()
        self._segments_last_hour.append(time.monotonic())

        # Prune segments older than 1 hour from the deque
        cutoff = time.monotonic() - 3600
        while self._segments_last_hour and self._segments_last_hour[0] < cutoff:
            self._segments_last_hour.popleft()

        # Fire callback
        logger.debug(
            "Segment ready",
            wav=wav_path.name,
            freq_mhz=freq_hz / 1_000_000,
            signal_db=signal_db,
        )
        self.on_segment(wav_path, freq_hz, signal_db=signal_db)

    # ------------------------------------------------------------------
    # MP3/WAV conversion
    # ------------------------------------------------------------------

    def _convert_mp3_to_wav(self, mp3_path: Path, wav_path: Path) -> bool:
        """Convert MP3 to 16 kHz mono 16-bit WAV for Whisper."""
        try:
            result = subprocess.run(
                [
                    "sox", str(mp3_path),
                    "-r", "16000",  # 16 kHz sample rate
                    "-c", "1",      # mono
                    "-b", "16",     # 16-bit
                    str(wav_path),
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if result.returncode != 0:
                logger.debug("sox conversion error", stderr=result.stderr.strip()[:200])
                return False
            return True
        except FileNotFoundError:
            logger.error("sox not found — install with: apt install sox libsox-fmt-mp3")
            return False
        except subprocess.TimeoutExpired:
            logger.warning("sox conversion timed out", mp3=mp3_path.name)
            return False
        except Exception as e:
            logger.warning("sox conversion failed", error=str(e))
            return False

    @staticmethod
    def _normalize_audio(wav_bytes: bytes, target_rms: float = 0.15) -> bytes:
        """Normalize WAV audio to a target RMS level for reliable transcription.

        Same algorithm as FMScanner._normalize_audio — target RMS 0.15,
        max 30x gain clamp.
        """
        with io.BytesIO(wav_bytes) as inp:
            with wave.open(inp, "rb") as wav_in:
                params = wav_in.getparams()
                raw = wav_in.readframes(params.nframes)

        samples = array.array("h")
        samples.frombytes(raw)

        if not samples:
            return wav_bytes

        rms = (sum(s * s for s in samples) / len(samples)) ** 0.5 / 32768.0

        if rms < 0.001:
            return wav_bytes  # Silence — don't amplify noise

        gain = target_rms / rms
        gain = min(gain, 30.0)  # Clamp extreme amplification

        normalized = array.array(
            "h",
            (max(-32768, min(32767, int(s * gain))) for s in samples),
        )

        output = io.BytesIO()
        with wave.open(output, "wb") as wav_out:
            wav_out.setparams(params)
            wav_out.writeframes(normalized.tobytes())
        return output.getvalue()

    # ------------------------------------------------------------------
    # Watchdog
    # ------------------------------------------------------------------

    def _watchdog_loop(self) -> None:
        """Monitor RTLSDR-Airband process health and segment production."""
        while not self._shutdown_event.is_set():
            self._shutdown_event.wait(timeout=self._process_check_interval)
            if self._shutdown_event.is_set():
                break

            # --- Process liveness check ---
            # Skip if a squelch update is in progress (it does its own
            # stop/start cycle and the process is expected to be dead).
            if self._squelch_updating:
                continue
            if self._process and self._process.poll() is not None:
                rc = self._process.returncode
                # Read any stderr output for diagnostics
                stderr = ""
                try:
                    stderr = self._process.stderr.read() if self._process.stderr else ""
                except Exception:
                    pass
                logger.warning(
                    "RTLSDR-Airband process died",
                    returncode=rc,
                    stderr=stderr.strip()[-500:] if stderr else "",
                )
                self._restart_airband_process()
                continue

            # --- Segment watchdog ---
            # If no segments for segment_watchdog_timeout, attempt USB reset
            ref_time = self._last_segment_monotonic or self._start_monotonic
            if ref_time is not None:
                silence = time.monotonic() - ref_time
                if silence > self._segment_watchdog_timeout:
                    logger.warning(
                        "No segments for extended period — attempting USB reset",
                        silence_seconds=int(silence),
                        threshold=int(self._segment_watchdog_timeout),
                    )
                    self._stop_airband_process()
                    time.sleep(1)
                    self._attempt_usb_reset()
                    time.sleep(3)
                    try:
                        self._start_airband_process()
                        self._watchdog_restarts += 1
                        self._last_segment_monotonic = time.monotonic()
                    except Exception as e:
                        logger.error("Failed to restart after USB reset", error=str(e))

    def _attempt_usb_reset(self) -> bool:
        """Reset the RTL-SDR USB device via ioctl.

        Same approach as FMScanner — tries ioctl USBDEVFS_RESET first,
        falls back to usbreset CLI.
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
                    if (
                        vid_path.read_text().strip() == RTL_SDR_VID
                        and pid_path.read_text().strip() == RTL_SDR_PID
                    ):
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
                capture_output=True,
                text=True,
                timeout=10,
            )
            if result.returncode == 0:
                self._usb_reset_count += 1
                logger.info("USB device reset via usbreset command")
                return True
            logger.warning("usbreset command failed", stderr=result.stderr.strip())
        except FileNotFoundError:
            logger.warning("USB reset unavailable — neither ioctl nor usbreset worked")
        except Exception as e:
            logger.warning("USB reset failed", error=str(e))

        return False

    # ------------------------------------------------------------------
    # Stats / properties
    # ------------------------------------------------------------------

    def get_capture_stats(self) -> Dict[str, Any]:
        """Get capture statistics (same interface as FMScanner)."""
        # Prune old entries
        cutoff = time.monotonic() - 3600
        while self._segments_last_hour and self._segments_last_hour[0] < cutoff:
            self._segments_last_hour.popleft()

        return {
            "scanner_type": "airband",
            "last_segment_captured_at": (
                self._last_segment_at.isoformat() if self._last_segment_at else None
            ),
            "segments_total": self._segments_captured,
            "segments_last_hour": len(self._segments_last_hour),
            "conversions_failed": self._conversions_failed,
            "watchdog_restarts": self._watchdog_restarts,
            "usb_resets": self._usb_reset_count,
            "spillover_dropped": self._spillover_dropped,
            "bleedover_events": self._bleedover_events,
            "bleedover_dropped": self._bleedover_dropped,
            "active_channels": len(self.frequencies),
            "center_freq_mhz": self._center_freq_hz / 1_000_000,
            "process_alive": self._process is not None and self._process.poll() is None,
            "squelch_snr_db": self._squelch_snr_db,
        }

    def get_sdr_config(self) -> Dict[str, Any]:
        """Get SDR hardware config and diagnostic info for observability."""
        config_contents = ""
        try:
            if self._config_path.exists():
                config_contents = self._config_path.read_text()
        except Exception:
            config_contents = "<error reading config>"

        with self._stderr_lock:
            recent_stderr = list(self._stderr_buffer)

        proc = self._process
        alive = proc is not None and proc.poll() is None
        uptime = None
        if self._process_start_time is not None and alive:
            uptime = round(time.monotonic() - self._process_start_time, 1)

        return {
            "squelch_snr_db": self._squelch_snr_db,
            "config_file_contents": config_contents,
            "recent_stderr": recent_stderr,
            "process_pid": proc.pid if proc and alive else None,
            "process_uptime_seconds": uptime,
            "process_alive": alive,
            "device_index": self._device_index,
            "gain": self._gain,
            "fft_size": self._fft_size,
            "center_freq_mhz": self._center_freq_hz / 1_000_000,
            "active_channels": len(self.frequencies),
            "frequencies_mhz": [f / 1_000_000 for f in self.frequencies],
        }

    @property
    def is_running(self) -> bool:
        return self._running
