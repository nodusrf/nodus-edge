"""
Adaptive FM Scanner with Wideband Activity Detection.

Uses two SDRs:
1. Wideband SDR - monitors spectrum for activity on candidate frequencies
2. Narrowband SDR - scans core + promoted frequencies for audio capture

Frequencies must "earn" their way into the scan rotation by showing activity.
Core frequencies are always scanned regardless of activity.
"""

import logging
import subprocess
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set
import struct
import numpy as np

from nodus_edge.config import settings

logger = logging.getLogger(__name__)


@dataclass
class FrequencyState:
    """Track state of a candidate frequency."""
    frequency_hz: int
    last_activity: Optional[datetime] = None
    is_promoted: bool = False
    activity_count: int = 0  # Total times activity detected
    signal_strength_db: float = -100.0  # Last observed signal strength


@dataclass
class ScanCycle:
    """Current scan cycle state."""
    core_frequencies: List[int] = field(default_factory=list)
    promoted_frequencies: List[int] = field(default_factory=list)
    current_index: int = 0
    cycle_count: int = 0

    @property
    def active_frequencies(self) -> List[int]:
        """All frequencies currently being scanned."""
        return self.core_frequencies + self.promoted_frequencies

    def next_frequency(self) -> Optional[int]:
        """Get next frequency in rotation."""
        freqs = self.active_frequencies
        if not freqs:
            return None
        freq = freqs[self.current_index % len(freqs)]
        self.current_index += 1
        if self.current_index >= len(freqs):
            self.current_index = 0
            self.cycle_count += 1
        return freq


class WidebandMonitor:
    """
    Monitor wideband spectrum for activity on known frequencies.

    Uses rtl_power or direct RTL-SDR FFT to detect signals.
    """

    def __init__(
        self,
        candidate_frequencies: List[int],
        center_hz: int = 146_000_000,
        sample_rate: int = 2_400_000,
        device_index: int = 1,
        fft_size: int = 4096,
        threshold_db: float = 10.0,
    ):
        self.candidate_frequencies = set(candidate_frequencies)
        self.center_hz = center_hz
        self.sample_rate = sample_rate
        self.device_index = device_index
        self.fft_size = fft_size
        self.threshold_db = threshold_db

        # Frequency -> bin mapping
        self._freq_to_bin: Dict[int, int] = {}
        self._setup_frequency_bins()

        # Background monitoring
        self._monitor_thread: Optional[threading.Thread] = None
        self._shutdown = threading.Event()
        self._activity_callback: Optional[Callable[[int, float], None]] = None

        # Noise floor tracking (adaptive)
        self._noise_floor: np.ndarray = np.zeros(fft_size)
        self._noise_floor_alpha = 0.1  # Exponential moving average factor

    def _setup_frequency_bins(self):
        """Map candidate frequencies to FFT bins."""
        bin_width = self.sample_rate / self.fft_size
        start_freq = self.center_hz - (self.sample_rate / 2)

        for freq in self.candidate_frequencies:
            if start_freq <= freq < start_freq + self.sample_rate:
                bin_idx = int((freq - start_freq) / bin_width)
                self._freq_to_bin[freq] = bin_idx
                logger.debug(f"Mapped {freq/1e6:.4f} MHz to bin {bin_idx}")

        logger.info(
            f"Wideband monitor: {len(self._freq_to_bin)} frequencies in range "
            f"({self.center_hz/1e6:.1f} MHz ± {self.sample_rate/2e6:.1f} MHz)"
        )

    def start(self, activity_callback: Callable[[int, float], None]):
        """Start background spectrum monitoring."""
        self._activity_callback = activity_callback
        self._shutdown.clear()
        self._monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._monitor_thread.start()
        logger.info("Wideband spectrum monitor started")

    def stop(self):
        """Stop background monitoring."""
        self._shutdown.set()
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)
        logger.info("Wideband spectrum monitor stopped")

    def _monitor_loop(self):
        """Background loop reading RTL-SDR and computing FFT."""
        try:
            # Start rtl_sdr process for raw IQ samples
            cmd = [
                "rtl_sdr",
                "-d", str(self.device_index),
                "-f", str(self.center_hz),
                "-s", str(self.sample_rate),
                "-g", "40",
                "-",  # Output to stdout
            ]

            logger.debug(f"Starting wideband SDR: {' '.join(cmd)}")
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            )

            # Read samples and compute FFT
            samples_per_fft = self.fft_size * 2  # I/Q pairs
            buffer = bytearray()

            while not self._shutdown.is_set():
                # Read chunk
                chunk = proc.stdout.read(samples_per_fft * 2)  # 2 bytes per sample
                if not chunk:
                    break

                buffer.extend(chunk)

                # Process complete FFT frames
                while len(buffer) >= samples_per_fft * 2:
                    frame = buffer[:samples_per_fft * 2]
                    buffer = buffer[samples_per_fft * 2:]

                    # Convert to complex samples
                    iq = np.frombuffer(frame, dtype=np.uint8).astype(np.float32)
                    iq = (iq - 127.5) / 127.5
                    samples = iq[0::2] + 1j * iq[1::2]

                    # Compute FFT
                    spectrum = np.fft.fftshift(np.abs(np.fft.fft(samples)))
                    power_db = 20 * np.log10(spectrum + 1e-10)

                    # Update noise floor (exponential moving average)
                    self._noise_floor = (
                        self._noise_floor_alpha * power_db +
                        (1 - self._noise_floor_alpha) * self._noise_floor
                    )

                    # Check each candidate frequency
                    self._check_activity(power_db)

                time.sleep(settings.fm_wideband_scan_interval)

            proc.terminate()

        except Exception as e:
            logger.error(f"Wideband monitor error: {e}")

    def _check_activity(self, power_db: np.ndarray):
        """Check for activity on candidate frequencies."""
        for freq, bin_idx in self._freq_to_bin.items():
            # Check bins around the target (±2 bins for FM signal width)
            start_bin = max(0, bin_idx - 2)
            end_bin = min(len(power_db), bin_idx + 3)

            signal_power = np.max(power_db[start_bin:end_bin])
            noise_floor = np.mean(self._noise_floor[start_bin:end_bin])
            snr = signal_power - noise_floor

            if snr >= self.threshold_db:
                logger.debug(
                    f"Activity detected: {freq/1e6:.4f} MHz, "
                    f"SNR={snr:.1f} dB"
                )
                if self._activity_callback:
                    self._activity_callback(freq, snr)


class AdaptiveFMScanner:
    """
    Adaptive FM scanner with activity-based frequency promotion.

    Core frequencies are always scanned.
    Candidate frequencies are promoted when wideband monitor detects activity.
    Promoted frequencies are demoted after idle timeout.
    """

    def __init__(
        self,
        on_segment: Callable[[Path, int], None],
        core_frequencies: Optional[List[int]] = None,
        candidate_frequencies: Optional[List[int]] = None,
    ):
        self.on_segment = on_segment

        # Frequency lists
        self.core_frequencies = core_frequencies or settings.fm_core_frequencies
        self.candidate_frequencies = candidate_frequencies or settings.fm_candidate_frequencies

        # State tracking
        self._frequency_states: Dict[int, FrequencyState] = {}
        for freq in self.candidate_frequencies:
            self._frequency_states[freq] = FrequencyState(frequency_hz=freq)

        # Scan cycle
        self._scan_cycle = ScanCycle(core_frequencies=list(self.core_frequencies))

        # Wideband monitor
        self._wideband_monitor: Optional[WidebandMonitor] = None
        if self.candidate_frequencies:
            self._wideband_monitor = WidebandMonitor(
                candidate_frequencies=self.candidate_frequencies,
                center_hz=settings.fm_wideband_center_hz,
                sample_rate=settings.fm_wideband_sample_rate,
                device_index=settings.fm_wideband_device_index,
                fft_size=settings.fm_wideband_fft_size,
                threshold_db=settings.fm_promotion_threshold_db,
            )

        # Narrowband scanner state
        self._shutdown = threading.Event()
        self._scan_thread: Optional[threading.Thread] = None
        self._rtl_process: Optional[subprocess.Popen] = None

        # Lock for thread-safe state updates
        self._lock = threading.Lock()

        logger.info(
            f"Adaptive scanner initialized: "
            f"{len(self.core_frequencies)} core, "
            f"{len(self.candidate_frequencies)} candidates"
        )

    def _on_activity_detected(self, frequency_hz: int, snr_db: float):
        """Callback when wideband monitor detects activity."""
        with self._lock:
            state = self._frequency_states.get(frequency_hz)
            if not state:
                return

            now = datetime.now(timezone.utc)
            state.last_activity = now
            state.activity_count += 1
            state.signal_strength_db = snr_db

            # Promote if not already
            if not state.is_promoted:
                self._promote_frequency(frequency_hz)

    def _promote_frequency(self, frequency_hz: int):
        """Promote a candidate frequency to active scanning."""
        state = self._frequency_states.get(frequency_hz)
        if not state or state.is_promoted:
            return

        # Check if we're at max capacity
        current_promoted = len(self._scan_cycle.promoted_frequencies)
        max_promoted = settings.fm_max_active_frequencies - len(self.core_frequencies)

        if current_promoted >= max_promoted:
            # Demote oldest promoted frequency
            self._demote_oldest()

        state.is_promoted = True
        self._scan_cycle.promoted_frequencies.append(frequency_hz)

        logger.info(
            f"Promoted {frequency_hz/1e6:.4f} MHz to active scan "
            f"(total active: {len(self._scan_cycle.active_frequencies)})"
        )

    def _demote_frequency(self, frequency_hz: int):
        """Demote a frequency from active scanning."""
        state = self._frequency_states.get(frequency_hz)
        if not state or not state.is_promoted:
            return

        state.is_promoted = False
        if frequency_hz in self._scan_cycle.promoted_frequencies:
            self._scan_cycle.promoted_frequencies.remove(frequency_hz)

        logger.info(f"Demoted {frequency_hz/1e6:.4f} MHz (idle timeout)")

    def _demote_oldest(self):
        """Demote the oldest promoted frequency to make room."""
        oldest_freq = None
        oldest_time = datetime.now(timezone.utc)

        for freq in self._scan_cycle.promoted_frequencies:
            state = self._frequency_states.get(freq)
            if state and state.last_activity:
                if state.last_activity < oldest_time:
                    oldest_time = state.last_activity
                    oldest_freq = freq

        if oldest_freq:
            self._demote_frequency(oldest_freq)

    def _check_demotions(self):
        """Check for frequencies that should be demoted due to idle timeout."""
        now = datetime.now(timezone.utc)
        timeout = timedelta(minutes=settings.fm_demotion_timeout_minutes)

        to_demote = []
        for freq in list(self._scan_cycle.promoted_frequencies):
            state = self._frequency_states.get(freq)
            if state and state.last_activity:
                if now - state.last_activity > timeout:
                    to_demote.append(freq)

        for freq in to_demote:
            self._demote_frequency(freq)

    def start(self):
        """Start adaptive scanning."""
        self._shutdown.clear()

        # Start wideband monitor
        if self._wideband_monitor:
            self._wideband_monitor.start(self._on_activity_detected)

        # Start narrowband scan loop
        self._scan_thread = threading.Thread(target=self._scan_loop, daemon=True)
        self._scan_thread.start()

        logger.info("Adaptive FM scanner started")

    def stop(self):
        """Stop adaptive scanning."""
        self._shutdown.set()

        if self._wideband_monitor:
            self._wideband_monitor.stop()

        if self._rtl_process:
            self._rtl_process.terminate()

        if self._scan_thread:
            self._scan_thread.join(timeout=5)

        logger.info("Adaptive FM scanner stopped")

    def _scan_loop(self):
        """Main narrowband scanning loop."""
        from nodus_edge.ingestion.fm_scanner import FMScanner

        # Create base scanner for audio capture
        base_scanner = FMScanner(
            frequencies=[],  # We manage frequencies dynamically
            on_segment=self.on_segment,
        )
        self._base_scanner = base_scanner

        # Initialize watchdog timing (normally done in FMScanner._scan_loop)
        now_mono = time.monotonic()
        base_scanner._last_data_received_at = now_mono
        base_scanner._scan_start_monotonic = now_mono

        last_demotion_check = datetime.now(timezone.utc)
        demotion_check_interval = timedelta(minutes=5)

        while not self._shutdown.is_set():
            # --- Watchdog checks (ported from FMScanner._scan_loop) ---

            # Data stream watchdog: if no full audio frames from any
            # frequency for too long, the USB/SDR is likely dead.
            if base_scanner._last_data_received_at is not None:
                silence = time.monotonic() - base_scanner._last_data_received_at
                if silence > base_scanner._watchdog_timeout_seconds:
                    logger.warning(
                        "Data stream watchdog triggered — no data from any frequency",
                        silence_seconds=round(silence, 1),
                        restart_count=base_scanner._watchdog_restart_count,
                    )
                    base_scanner._cleanup_processes()
                    base_scanner._watchdog_restart_count += 1
                    base_scanner._last_data_received_at = time.monotonic()
                    time.sleep(5.0)
                    continue

            # Segment watchdog: if no segments captured for an extended
            # period, the SDR may be wedged. Escalate to USB device reset.
            seg_ref = (base_scanner._last_segment_monotonic
                       or base_scanner._scan_start_monotonic)
            if seg_ref is not None:
                seg_silence = time.monotonic() - seg_ref
                if seg_silence > base_scanner._segment_watchdog_timeout:
                    logger.warning(
                        "Segment watchdog triggered — no captures, attempting USB reset",
                        silence_minutes=round(seg_silence / 60, 1),
                        usb_reset_count=base_scanner._usb_reset_count,
                    )
                    base_scanner._cleanup_processes()
                    base_scanner._attempt_usb_reset()
                    now_mono = time.monotonic()
                    base_scanner._last_data_received_at = now_mono
                    base_scanner._last_segment_monotonic = now_mono
                    time.sleep(5.0)
                    continue

            # --- End watchdog checks ---

            # Periodic demotion check
            now = datetime.now(timezone.utc)
            if now - last_demotion_check > demotion_check_interval:
                with self._lock:
                    self._check_demotions()
                last_demotion_check = now

            # Get next frequency
            with self._lock:
                freq = self._scan_cycle.next_frequency()

            if not freq:
                time.sleep(0.5)
                continue

            # Skip channels marked noisy (static breaking through squelch)
            if base_scanner.is_noisy(freq):
                logger.debug(f"Skipping noisy channel {freq/1e6:.4f} MHz")
                continue

            # Tune and capture
            try:
                base_scanner._monitor_frequency(freq)
            except Exception as e:
                logger.error(f"Error scanning {freq/1e6:.4f} MHz: {e}")

    def get_capture_stats(self) -> Dict:
        """Get capture health stats for heartbeat reporting.

        Delegates to the underlying FMScanner so watchdog restart
        counts, USB reset counts, and segment-per-hour metrics appear
        in the Diagnostics heartbeat.
        """
        base = getattr(self, '_base_scanner', None)
        if base is not None:
            return base.get_capture_stats()
        return {}

    def get_stats(self) -> Dict:
        """Get scanner statistics."""
        with self._lock:
            promoted_details = []
            for freq in self._scan_cycle.promoted_frequencies:
                state = self._frequency_states.get(freq)
                if state:
                    promoted_details.append({
                        "frequency_mhz": freq / 1e6,
                        "last_activity": state.last_activity.isoformat() if state.last_activity else None,
                        "activity_count": state.activity_count,
                        "signal_db": state.signal_strength_db,
                    })

            return {
                "core_count": len(self.core_frequencies),
                "candidate_count": len(self.candidate_frequencies),
                "promoted_count": len(self._scan_cycle.promoted_frequencies),
                "active_frequencies": len(self._scan_cycle.active_frequencies),
                "cycle_count": self._scan_cycle.cycle_count,
                "promoted_frequencies": promoted_details,
            }
