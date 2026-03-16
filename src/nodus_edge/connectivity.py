"""
Internet connectivity probe.

Periodically probes a configured URL (default: Synapse health endpoint).
After N consecutive failures, declares the link offline. Recovers when
connectivity returns.
"""

import time
from threading import Event, Thread
from typing import Optional

import structlog

logger = structlog.get_logger(__name__)


class ConnectivityProbe:
    """
    Background thread that monitors internet connectivity.

    Usage:
        probe = ConnectivityProbe(
            probe_url="https://api.nodusrf.com/health",
            interval_sec=30,
            fail_threshold=3,
        )
        probe.start()
        if probe.is_offline:
            ...  # activate APRS beaconing
        probe.stop()
    """

    def __init__(
        self,
        probe_url: str,
        interval_sec: float = 30.0,
        fail_threshold: int = 3,
        timeout_sec: float = 10.0,
    ):
        self._probe_url = probe_url
        self._interval_sec = interval_sec
        self._fail_threshold = fail_threshold
        self._timeout_sec = timeout_sec

        self._consecutive_failures = 0
        self._is_offline = False
        self._thread: Optional[Thread] = None
        self._stop_event = Event()

        # Stats
        self._probes_sent = 0
        self._probes_failed = 0
        self._offline_transitions = 0
        self._online_transitions = 0

    @property
    def is_offline(self) -> bool:
        return self._is_offline

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    def start(self) -> None:
        """Start the probe thread."""
        if not self._probe_url:
            logger.info("Connectivity probe disabled (no URL configured)")
            return

        self._stop_event.clear()
        self._thread = Thread(
            target=self._probe_loop,
            daemon=True,
            name="connectivity-probe",
        )
        self._thread.start()
        logger.info(
            "Connectivity probe started",
            url=self._probe_url,
            interval_sec=self._interval_sec,
            fail_threshold=self._fail_threshold,
        )

    def stop(self) -> None:
        """Stop the probe thread."""
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        logger.info(
            "Connectivity probe stopped",
            probes_sent=self._probes_sent,
            probes_failed=self._probes_failed,
            offline=self._is_offline,
        )

    def get_stats(self) -> dict:
        return {
            "probe_url": self._probe_url,
            "is_offline": self._is_offline,
            "consecutive_failures": self._consecutive_failures,
            "probes_sent": self._probes_sent,
            "probes_failed": self._probes_failed,
            "offline_transitions": self._offline_transitions,
            "online_transitions": self._online_transitions,
        }

    def _probe_loop(self) -> None:
        """Main probe loop."""
        # Delay first probe slightly to let services stabilize
        self._stop_event.wait(timeout=5)

        while not self._stop_event.is_set():
            try:
                self._do_probe()
            except Exception as e:
                logger.debug("Probe loop error", error=str(e))

            self._stop_event.wait(timeout=self._interval_sec)

    def _do_probe(self) -> None:
        """Execute a single connectivity probe."""
        self._probes_sent += 1
        success = False

        try:
            import httpx
            response = httpx.get(
                self._probe_url,
                timeout=self._timeout_sec,
                follow_redirects=True,
            )
            success = response.status_code < 500
        except ImportError:
            # Fall back to urllib if httpx not available
            try:
                import urllib.request
                req = urllib.request.Request(self._probe_url, method="GET")
                urllib.request.urlopen(req, timeout=self._timeout_sec)
                success = True
            except Exception:
                success = False
        except Exception:
            success = False

        if success:
            if self._consecutive_failures > 0:
                logger.debug(
                    "Connectivity restored",
                    after_failures=self._consecutive_failures,
                )
            self._consecutive_failures = 0
            if self._is_offline:
                self._is_offline = False
                self._online_transitions += 1
                logger.warning("Connectivity restored")
        else:
            self._consecutive_failures += 1
            self._probes_failed += 1
            if (
                not self._is_offline
                and self._consecutive_failures >= self._fail_threshold
            ):
                self._is_offline = True
                self._offline_transitions += 1
                logger.warning(
                    "Internet unreachable",
                    consecutive_failures=self._consecutive_failures,
                )
