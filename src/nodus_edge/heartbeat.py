"""
Heartbeat emitter for Nodus Edge.

Emits heartbeat events to Diagnostics every 10 seconds.
"""

import threading
from datetime import datetime, timezone
from typing import Callable, Dict, Any, Optional

import httpx
import structlog

from . import __version__

logger = structlog.get_logger(__name__)

HEARTBEAT_INTERVAL_SECONDS = 10.0
HEARTBEAT_TIMEOUT_SECONDS = 5.0


class HeartbeatEmitter:
    """
    Emits periodic heartbeats to the Diagnostics service.

    Each heartbeat contains service identity, uptime, and current stats.
    """

    def __init__(
        self,
        diagnostics_endpoint: str,
        service: str = "nodus-edge",
        node_id: str = "unknown",
        get_stats: Optional[Callable[[], Dict[str, Any]]] = None,
        auth_token: Optional[str] = None,
    ):
        self.diagnostics_endpoint = diagnostics_endpoint.rstrip("/")
        self.service = service
        self.node_id = node_id
        self.get_stats = get_stats or (lambda: {})
        self.auth_token = auth_token

        self._start_time = datetime.now(timezone.utc)
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start emitting heartbeats in a background thread."""
        if self._thread is not None:
            return

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._thread.start()
        logger.info(
            "Heartbeat emitter started",
            endpoint=self.diagnostics_endpoint,
            interval=HEARTBEAT_INTERVAL_SECONDS,
        )

    def stop(self) -> None:
        """Stop the heartbeat emitter."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None
        logger.info("Heartbeat emitter stopped")

    def _heartbeat_loop(self) -> None:
        """Background loop that sends heartbeats. Never dies."""
        while not self._stop_event.is_set():
            try:
                self._send_heartbeat()
            except Exception:
                logger.exception("Unexpected error in heartbeat loop")
            self._stop_event.wait(HEARTBEAT_INTERVAL_SECONDS)

    def _send_heartbeat(self) -> None:
        """Send a single heartbeat to Diagnostics."""
        uptime = (datetime.now(timezone.utc) - self._start_time).total_seconds()

        try:
            stats = self.get_stats()
        except Exception as e:
            logger.warning("Stats callback failed, sending heartbeat without stats", error=str(e))
            stats = {}
        stats["version"] = __version__

        heartbeat = {
            "service": self.service,
            "node_id": self.node_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "uptime_seconds": uptime,
            "stats": stats,
        }

        try:
            headers = {}
            if self.auth_token:
                headers["Authorization"] = f"Bearer {self.auth_token}"
            with httpx.Client(timeout=HEARTBEAT_TIMEOUT_SECONDS) as client:
                response = client.post(
                    f"{self.diagnostics_endpoint}/v1/heartbeat",
                    json=heartbeat,
                    headers=headers,
                )
                if response.status_code != 202:
                    logger.warning(
                        "Heartbeat rejected",
                        status=response.status_code,
                    )
        except httpx.ConnectError:
            logger.debug("Diagnostics unreachable, heartbeat skipped")
        except Exception as e:
            logger.debug("Heartbeat failed", error=str(e))
