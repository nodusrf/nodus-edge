"""REM check-in emitter for edge nodes.

Checks in with the Remote Edge Management service on a regular interval.
On successful check-in, receives a compliance token that Synapse requires
for segment ingestion, plus version policy information.
"""

import json
import os
import platform
import threading
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

import httpx
import structlog

from . import __version__
from . import diagnostic_collector

logger = structlog.get_logger(__name__)

CHECKIN_TIMEOUT_SECONDS = 10.0


class REMCheckIn:
    """Periodic check-in with the REM service.

    On each check-in:
    - Reports node state (version, arch, uptime, frequencies, segment count)
    - Receives version policy (target + supported versions)
    - Receives a signed compliance token if version is supported
    - Stores the token for Synapse to use on segment POST
    """

    def __init__(
        self,
        rem_endpoint: str,
        node_id: str,
        auth_token: Optional[str] = None,
        get_stats: Optional[Callable[[], Dict[str, Any]]] = None,
        get_frequencies: Optional[Callable[[], list]] = None,
        initial_interval_seconds: int = 1800,
    ):
        self.rem_endpoint = rem_endpoint.rstrip("/").removesuffix("/v1")
        self.node_id = node_id
        self.auth_token = auth_token
        self.get_stats = get_stats or (lambda: {})
        self.get_frequencies = get_frequencies or (lambda: [])

        self._interval_seconds = initial_interval_seconds
        self._start_time = datetime.now(timezone.utc)
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

        # Signals when the first check-in attempt completes (success or failure)
        self.first_checkin_done = threading.Event()

        # Compliance state (read by SynapsePublisher)
        self.compliance_token: Optional[str] = None
        self.is_compliant: bool = False
        self.is_target: bool = False
        self.upgrade_available: Optional[str] = None
        self.delay_upgrade_seconds: int = 0
        self._last_checkin_at: Optional[str] = None
        self._consecutive_failures: int = 0

    @property
    def has_valid_token(self) -> bool:
        """True if we have a compliance token from a recent check-in."""
        return self.compliance_token is not None

    def start(self) -> None:
        """Start check-in loop in a background thread."""
        if self._thread is not None:
            return

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._checkin_loop, daemon=True)
        self._thread.start()
        logger.info(
            "REM check-in started",
            endpoint=self.rem_endpoint,
            interval_seconds=self._interval_seconds,
        )

    def stop(self) -> None:
        """Stop the check-in loop."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5.0)
            self._thread = None
        logger.info("REM check-in stopped")

    def _checkin_loop(self) -> None:
        """Background loop. Checks in immediately on start, then on interval."""
        # Check in immediately on startup
        self._do_checkin()
        self.first_checkin_done.set()

        while not self._stop_event.is_set():
            self._stop_event.wait(self._interval_seconds)
            if not self._stop_event.is_set():
                self._do_checkin()

    def _do_checkin(self) -> None:
        """Perform a single check-in with REM."""
        uptime = (datetime.now(timezone.utc) - self._start_time).total_seconds()

        try:
            stats = self.get_stats()
        except Exception:
            stats = {}

        try:
            frequencies = self.get_frequencies()
        except Exception:
            frequencies = []

        image_digest = os.environ.get("NODUS_IMAGE_DIGEST", "")

        payload = {
            "node_id": self.node_id,
            "version": __version__,
            "image_digest": image_digest,
            "arch": platform.machine(),
            "uptime_seconds": uptime,
            "segment_count": stats.get("processed_count", 0),
            "frequencies": frequencies,
            "hardware": platform.platform(),
            "os_info": platform.system(),
        }

        try:
            headers = {}
            if self.auth_token:
                headers["Authorization"] = f"Bearer {self.auth_token}"

            with httpx.Client(timeout=CHECKIN_TIMEOUT_SECONDS) as client:
                response = client.post(
                    f"{self.rem_endpoint}/v1/checkin",
                    json=payload,
                    headers=headers,
                )

            if response.status_code == 200:
                data = response.json()
                self.is_compliant = data.get("is_compliant", False)
                self.is_target = data.get("is_target", False)
                self.compliance_token = data.get("compliance_token")
                self.upgrade_available = data.get("upgrade_available")
                self.delay_upgrade_seconds = data.get("delay_upgrade_seconds", 0)
                self._last_checkin_at = datetime.now(timezone.utc).isoformat()
                self._consecutive_failures = 0

                # REM tells us when to check in next
                next_interval = data.get("next_checkin_seconds")
                if next_interval and next_interval > 0:
                    self._interval_seconds = next_interval

                # Handle one-shot actions from REM
                actions = data.get("actions", [])
                if actions:
                    self._handle_actions(actions)

                if not self.is_compliant:
                    logger.warning(
                        "Node is non-compliant",
                        version=__version__,
                        message=data.get("message", ""),
                    )
                elif not self.is_target:
                    logger.info(
                        "Upgrade available",
                        current=__version__,
                        target=self.upgrade_available,
                        delay_seconds=self.delay_upgrade_seconds,
                    )
                else:
                    logger.debug("REM check-in OK", version=__version__)

            elif response.status_code == 404:
                logger.warning(
                    "Node not enrolled in REM, attempting auto-enrollment",
                    node_id=self.node_id,
                )
                self.compliance_token = None
                self.is_compliant = False
                self._auto_enroll()
            else:
                logger.warning(
                    "REM check-in rejected",
                    status=response.status_code,
                    detail=response.text[:200],
                )
                self._consecutive_failures += 1

        except httpx.ConnectError:
            self._consecutive_failures += 1
            logger.debug(
                "REM unreachable, check-in skipped",
                failures=self._consecutive_failures,
            )
        except Exception as e:
            self._consecutive_failures += 1
            logger.debug("REM check-in failed", error=str(e))

    def _auto_enroll(self) -> None:
        """Attempt to enroll this node with REM on first 404."""
        callsign = os.environ.get("NODUS_EDGE_CALLSIGN", "")
        metro = os.environ.get("NODUS_EDGE_METRO", "")

        signup_payload = {
            "node_id": self.node_id,
            "callsign": callsign,
            "email": "",
            "metro": metro,
        }

        try:
            with httpx.Client(timeout=CHECKIN_TIMEOUT_SECONDS) as client:
                resp = client.post(
                    f"{self.rem_endpoint}/v1/signup",
                    json=signup_payload,
                )
            if resp.status_code == 200:
                data = resp.json()
                api_token = data.get("api_token", "")
                if api_token:
                    self.auth_token = api_token
                    os.environ["NODUSNET_TOKEN"] = api_token
                    self._persist_token(api_token)
                    logger.info("Auto-enrolled with REM, API token received", node_id=self.node_id)
                else:
                    logger.info("Auto-enrolled with REM", node_id=self.node_id)
            elif resp.status_code == 409:
                logger.info("Node already enrolled in REM", node_id=self.node_id)
            else:
                logger.warning(
                    "REM auto-enrollment failed",
                    status=resp.status_code,
                    detail=resp.text[:200],
                )
        except Exception as e:
            logger.warning("REM auto-enrollment error", error=str(e))

    def _persist_token(self, token: str) -> None:
        """Write NODUSNET_TOKEN to .env so it survives container restarts."""
        env_path = "/root/nodusedge/.env"
        if not os.path.exists(env_path):
            env_path = os.path.join(os.getcwd(), ".env")
        if not os.path.exists(env_path):
            logger.warning("Cannot persist token: .env not found")
            return
        try:
            with open(env_path) as f:
                lines = f.readlines()
            found = False
            for i, line in enumerate(lines):
                if line.startswith("NODUSNET_TOKEN="):
                    lines[i] = f"NODUSNET_TOKEN={token}\n"
                    found = True
                    break
            if not found:
                lines.append(f"NODUSNET_TOKEN={token}\n")
            with open(env_path, "w") as f:
                f.writelines(lines)
            logger.info("API token persisted to .env")
        except Exception as e:
            logger.warning("Failed to persist token to .env", error=str(e))

    def _handle_actions(self, actions: List[Dict[str, Any]]) -> None:
        """Process one-shot actions from REM. Runs handlers in background threads."""
        for action in actions:
            action_type = action.get("type", "")
            action_id = action.get("action_id", "")

            if action_type == "diagnostic_dump":
                logger.info("Diagnostic dump requested by REM", action_id=action_id)
                thread = threading.Thread(
                    target=self._collect_and_upload_dump,
                    args=(action_id,),
                    daemon=True,
                )
                thread.start()
            elif action_type == "notification":
                payload = action.get("payload", {})
                logger.info(
                    "Notification from NodusRF",
                    title=payload.get("title", ""),
                    action_id=action_id,
                )
                thread = threading.Thread(
                    target=self._deliver_dashboard_notification,
                    args=(payload,),
                    daemon=True,
                )
                thread.start()
            elif action_type == "config_push":
                payload = action.get("payload", {})
                logger.info(
                    "Config push from NodusRF",
                    keys=list(payload.get("env", {}).keys()),
                    reason=payload.get("reason", ""),
                    action_id=action_id,
                )
                thread = threading.Thread(
                    target=self._apply_config_push,
                    args=(payload, action_id),
                    daemon=True,
                )
                thread.start()
            else:
                logger.warning("Unknown REM action type", action_type=action_type)

    def _deliver_dashboard_notification(self, payload: Dict[str, Any]) -> None:
        """Push a notification to the local edge dashboard."""
        try:
            with httpx.Client(timeout=5.0) as client:
                resp = client.post(
                    "http://127.0.0.1:8073/api/notifications",
                    json=payload,
                )
            if resp.status_code == 200:
                logger.info("Dashboard notification delivered", title=payload.get("title", ""))
            else:
                logger.warning(
                    "Dashboard notification failed",
                    status=resp.status_code,
                    detail=resp.text[:200],
                )
        except Exception as e:
            logger.warning("Dashboard notification delivery error", error=str(e))

    def _apply_config_push(self, payload: Dict[str, Any], action_id: str) -> None:
        """Apply config overrides from REM.

        Writes overrides to /data/config-overrides.env (persistent across
        restarts) and applies them to os.environ for the current process.
        Settings that read from os.environ on access pick up changes
        immediately; others take effect on next container restart.
        """
        env_overrides = payload.get("env", {})
        reason = payload.get("reason", "")
        if not env_overrides:
            logger.warning("Config push had no env overrides", action_id=action_id)
            return

        # Allowlist: only accept known edge config keys to prevent injection
        allowed_prefixes = (
            "NODUS_EDGE_",
            "NODUSNET_",
            "NODUS_EDGE_",
        )
        allowed_exact = {"NODUS_TUNNEL_TOKEN"}
        applied = {}
        rejected = {}
        for key, value in env_overrides.items():
            if key not in allowed_exact and not any(key.startswith(p) for p in allowed_prefixes):
                rejected[key] = "not in allowed prefix list"
                continue
            old_value = os.environ.get(key, "<unset>")
            os.environ[key] = str(value)
            applied[key] = {"old": old_value, "new": str(value)}

        # Persist to override file so changes survive container restarts
        override_path = "/data/config-overrides.env"
        try:
            # Read existing overrides
            existing = {}
            if os.path.exists(override_path):
                with open(override_path) as f:
                    for line in f:
                        line = line.strip()
                        if line and "=" in line and not line.startswith("#"):
                            k, v = line.split("=", 1)
                            existing[k] = v
            # Merge new overrides
            for key in applied:
                existing[key] = applied[key]["new"]
            # Write back
            with open(override_path, "w") as f:
                f.write(f"# REM config push — {reason}\n")
                f.write(f"# Applied at: {datetime.now(timezone.utc).isoformat()}\n")
                for k, v in sorted(existing.items()):
                    f.write(f"{k}={v}\n")
            logger.info(
                "Config overrides persisted",
                path=override_path,
                keys=list(applied.keys()),
            )
        except Exception as e:
            logger.warning("Failed to persist config overrides", error=str(e))

        if applied:
            logger.info(
                "Config push applied",
                applied=applied,
                reason=reason,
                action_id=action_id,
            )
        if rejected:
            logger.warning(
                "Config push keys rejected",
                rejected=rejected,
                action_id=action_id,
            )

        # Notify dashboard
        self._deliver_dashboard_notification({
            "title": "Config Updated by NodusRF",
            "body": (
                f"Settings changed: {', '.join(applied.keys())}. "
                f"Reason: {reason or 'not specified'}."
            ),
            "priority": "medium",
        })

    def _collect_and_upload_dump(self, action_id: str) -> None:
        """Collect diagnostics and upload to REM via Gateway."""
        try:
            dump = diagnostic_collector.collect(
                node_id=self.node_id,
                get_stats=self.get_stats,
            )

            payload = {
                "node_id": self.node_id,
                "action_id": action_id,
                "dump": dump,
            }

            headers = {}
            if self.auth_token:
                headers["Authorization"] = f"Bearer {self.auth_token}"

            upload_url = f"{self.rem_endpoint}/v1/edge/diagnostic-dump"
            with httpx.Client(timeout=15.0) as client:
                resp = client.post(upload_url, json=payload, headers=headers)

            if resp.status_code == 200:
                logger.info("Diagnostic dump uploaded", action_id=action_id)
            else:
                logger.warning(
                    "Diagnostic dump upload failed",
                    status=resp.status_code,
                    detail=resp.text[:200],
                )

        except Exception as e:
            logger.error("Diagnostic dump collection/upload failed", error=str(e))

    def get_checkin_stats(self) -> Dict[str, Any]:
        """Stats for inclusion in heartbeat or dashboard."""
        return {
            "is_compliant": self.is_compliant,
            "is_target": self.is_target,
            "has_token": self.has_valid_token,
            "upgrade_available": self.upgrade_available,
            "latest_checkin_at": self._last_checkin_at,
            "consecutive_failures": self._consecutive_failures,
        }
