"""Coverage reporter for spectrum coordination.

Reports this node's monitored frequencies to the Gateway on startup
and whenever the frequency list changes at runtime.
"""

import hashlib
import logging
import threading
from typing import Callable, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)


class CoverageReporter:
    """Reports edge node frequency coverage to Gateway."""

    def __init__(
        self,
        gateway_url: str,
        node_id: str,
        metro: str,
        mode: str = "fm",
        auth_token: Optional[str] = None,
        get_signal_db: Optional[Callable[[int], Optional[float]]] = None,
    ):
        self.gateway_url = gateway_url.rstrip("/")
        self.node_id = node_id
        self.metro = metro
        self.mode = mode
        self.auth_token = auth_token
        self.get_signal_db = get_signal_db
        self._last_hash: Optional[str] = None
        self._last_report_args: Optional[tuple] = None
        self._needs_signal_update = False
        self._periodic_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    PERIODIC_INTERVAL_SEC = 1800  # 30 minutes

    def _compute_hash(self, frequencies: List[Dict]) -> str:
        """SHA-256 hash of sorted frequency list for change detection."""
        sorted_freqs = sorted(f["frequency_hz"] for f in frequencies)
        return hashlib.sha256(str(sorted_freqs).encode()).hexdigest()[:16]

    @property
    def coverage_hash(self) -> Optional[str]:
        """Current coverage hash for heartbeat stats."""
        return self._last_hash

    def report(
        self,
        core_frequencies: List[int],
        candidate_frequencies: Optional[List[int]] = None,
        lat: Optional[float] = None,
        lon: Optional[float] = None,
    ) -> None:
        """Report coverage to Gateway (non-blocking, runs in background thread)."""
        frequencies = []
        for freq_hz in core_frequencies:
            entry = {"frequency_hz": freq_hz, "is_core": True}
            if self.get_signal_db:
                sig = self.get_signal_db(freq_hz)
                if sig is not None:
                    entry["avg_signal_db"] = sig
            frequencies.append(entry)
        for freq_hz in (candidate_frequencies or []):
            entry = {"frequency_hz": freq_hz, "is_core": False}
            if self.get_signal_db:
                sig = self.get_signal_db(freq_hz)
                if sig is not None:
                    entry["avg_signal_db"] = sig
            frequencies.append(entry)

        new_hash = self._compute_hash(frequencies)
        if new_hash == self._last_hash and not self._needs_signal_update:
            return  # No change

        self._last_hash = new_hash
        self._needs_signal_update = False

        # Save args for periodic re-reports with signal data
        self._last_report_args = (
            core_frequencies, candidate_frequencies or [], lat, lon,
        )

        # Fire and forget in background thread
        thread = threading.Thread(
            target=self._send_report,
            args=(frequencies, lat, lon),
            daemon=True,
        )
        thread.start()

        # Start periodic re-report thread if not already running
        if self._periodic_thread is None and self.get_signal_db:
            self._start_periodic_report()

    def _send_report(
        self,
        frequencies: List[Dict],
        lat: Optional[float],
        lon: Optional[float],
    ) -> None:
        """Send coverage report to Gateway (blocking, runs in thread)."""
        payload = {
            "node_id": self.node_id,
            "metro": self.metro,
            "mode": self.mode,
            "frequencies": frequencies,
        }
        if lat is not None:
            payload["lat"] = lat
        if lon is not None:
            payload["lon"] = lon

        headers = {}
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"

        try:
            with httpx.Client(timeout=10.0) as client:
                resp = client.post(
                    f"{self.gateway_url}/v1/edge/coverage",
                    json=payload,
                    headers=headers,
                )
                if resp.status_code == 200:
                    logger.info(
                        f"Coverage reported: {len(frequencies)} frequencies "
                        f"for {self.metro}"
                    )
                else:
                    logger.warning(
                        f"Coverage report failed: {resp.status_code} "
                        f"{resp.text[:200]}"
                    )
        except httpx.ConnectError:
            logger.debug("Gateway unreachable, coverage report skipped")
        except Exception as e:
            logger.debug(f"Coverage report failed: {e}")

    def _start_periodic_report(self) -> None:
        """Start background thread that re-reports coverage with signal data."""
        self._periodic_thread = threading.Thread(
            target=self._periodic_loop, daemon=True,
        )
        self._periodic_thread.start()

    def _periodic_loop(self) -> None:
        """Re-report coverage every 30 min to update signal strength."""
        while not self._stop_event.is_set():
            self._stop_event.wait(self.PERIODIC_INTERVAL_SEC)
            if self._stop_event.is_set():
                break
            if self._last_report_args:
                core, cands, lat, lon = self._last_report_args
                self._needs_signal_update = True
                self.report(core, cands, lat, lon)

    def stop(self) -> None:
        """Stop periodic re-reporting."""
        self._stop_event.set()
