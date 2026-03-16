"""
FastAPI dashboard server for Edge FM edge nodes.

Serves:
- SSE event stream at /events (live segment feed)
- REST API at /api/* (segments, frequencies, traffic, status, debug, env, squelch)
- Static files (SPA dashboard)

Runs as a daemon thread alongside the FM pipeline.
"""

import asyncio
import json
import secrets
from datetime import datetime, timezone
from pathlib import Path
from threading import Thread
from typing import Dict, Optional

import structlog
import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from .segment_store import SegmentStore
from .sync_cache import SyncCache

logger = structlog.get_logger(__name__)

STATIC_DIR = Path(__file__).parent / "static"

# Track the compose file version baked into this image.
# Bump when the canonical docker-compose.yml changes.
COMPOSE_VERSION = "2"

# Module-level references set by start_dashboard()
_store: Optional[SegmentStore] = None
_cache: Optional[SyncCache] = None
_health_url: Optional[str] = None
_node_id: str = "unknown"
_squelch_snr: float = 6.0
_timezone: str = ""
_metro: str = ""
_env_path: Optional[Path] = None
_startup_warnings: list = []
_get_segment_warnings = None  # callable returning {code: {count, last_seen}}
_dashboard_token: str = ""  # Auth token for mutative endpoints
_get_pipeline_stats = None  # callable returning pipeline stats dict
_rem_checkin = None  # REMCheckIn instance for diagnostic dump trigger
_auto_dump_sent: bool = False  # one-shot: only auto-send one dump per container lifetime
_pending_notifications: list = []  # Server-pushed notifications from NodusRF


def _require_dashboard_token(request: Request):
    """Dependency: reject mutative requests unless a valid dashboard token is provided.

    When no NODUS_EDGE_DASHBOARD_TOKEN is set, the dashboard is open (localhost only).
    """
    if not _dashboard_token:
        return  # No token configured — allow all (local dashboard)
    auth = request.headers.get("Authorization", "")
    if auth == f"Bearer {_dashboard_token}":
        return
    # Also accept ?token= query param for simple browser forms
    if request.query_params.get("token") == _dashboard_token:
        return
    raise HTTPException(status_code=401, detail="Dashboard token required")


def _maybe_auto_dump():
    """Auto-send a diagnostic dump if issues detected. One-shot per container lifetime."""
    global _auto_dump_sent
    if _auto_dump_sent or not _rem_checkin:
        return
    _auto_dump_sent = True

    import threading
    from ..diagnostic_collector import collect

    def _upload():
        try:
            dump = collect(
                node_id=_node_id,
                get_stats=_get_pipeline_stats,
            )
            import httpx
            payload = {"node_id": _node_id, "action_id": "", "dump": dump}
            upload_url = f"{_rem_checkin.rem_endpoint}/v1/edge/diagnostic-dump"
            headers = {}
            if _rem_checkin.auth_token:
                headers["Authorization"] = f"Bearer {_rem_checkin.auth_token}"
            with httpx.Client(timeout=15.0) as client:
                resp = client.post(upload_url, json=payload, headers=headers)
            logger.info("Auto diagnostic dump uploaded", status=resp.status_code)
        except Exception as e:
            logger.warning("Auto diagnostic dump failed", error=str(e))

    threading.Thread(target=_upload, daemon=True).start()


# .env field definitions: field_name -> (label, category)
_ENV_FIELDS: Dict[str, dict] = {
    "NODUS_EDGE_NODE_ID":                        {"label": "Node ID",               "cat": "Node"},
    "NODUS_EDGE_METRO":                          {"label": "Metro Area",            "cat": "Node"},
    "NODUS_EDGE_TIMEZONE":                       {"label": "Timezone",              "cat": "Node"},
    "NODUS_EDGE_FM_FREQUENCIES":                 {"label": "Frequencies (Hz JSON)",  "cat": "Scanner"},
    "NODUS_EDGE_FM_CORE_FREQUENCIES":            {"label": "Core Frequencies",       "cat": "Scanner"},
    "NODUS_EDGE_FM_CANDIDATE_FREQUENCIES":       {"label": "Candidate Frequencies",  "cat": "Scanner"},
    "NODUS_EDGE_FM_SCANNER_BACKEND":             {"label": "Scanner Backend",        "cat": "Scanner"},
    "NODUS_EDGE_FM_AIRBAND_SQUELCH_SNR_DB":      {"label": "Squelch SNR (dB)",       "cat": "Scanner"},
    "NODUS_EDGE_FM_AIRBAND_FFT_SIZE":            {"label": "FFT Size",              "cat": "Scanner"},
    "NODUS_EDGE_FM_GAIN":                        {"label": "RTL Gain",              "cat": "Scanner"},
    "NODUS_EDGE_FM_RTL_DEVICE_INDEX":            {"label": "RTL Device Index",      "cat": "Scanner"},
    "NODUS_EDGE_FM_SEGMENT_MIN_SECONDS":         {"label": "Min Segment (sec)",     "cat": "Scanner"},
    "NODUS_EDGE_FM_SPILLOVER_DETECTION_ENABLED": {"label": "Spillover Detection",   "cat": "Scanner"},
    "NODUS_EDGE_WHISPER_API_URL":                {"label": "Whisper API URL",        "cat": "Transcription"},
    "NODUS_EDGE_TRANSCRIPTION_ENABLED":          {"label": "Transcription Enabled",  "cat": "Transcription"},
    "NODUSNET_SERVER":                       {"label": "Server",                 "cat": "Network"},
    "NODUS_EDGE_DASHBOARD_ENABLED":              {"label": "Dashboard Enabled",      "cat": "Dashboard"},
    "NODUS_EDGE_DASHBOARD_PORT":                 {"label": "Dashboard Port",         "cat": "Dashboard"},
}

_ENV_CATEGORY_ORDER = ["Node", "Scanner", "Transcription", "Network", "Dashboard"]


def create_app() -> FastAPI:
    """Create the FastAPI dashboard application."""
    app = FastAPI(title="NodusEdge Dashboard", docs_url=None, redoc_url=None, openapi_url=None)

    # --- SSE endpoint ---

    @app.get("/events")
    async def sse_events(request: Request):
        """Server-Sent Events stream for live segment feed."""
        if _store is None:
            return JSONResponse({"error": "Store not initialized"}, status_code=503)

        queue = _store.subscribe_sse()

        async def event_generator():
            try:
                while True:
                    if await request.is_disconnected():
                        break
                    try:
                        segment = await asyncio.wait_for(queue.get(), timeout=30.0)
                        yield f"data: {json.dumps(segment, default=str)}\n\n"
                    except asyncio.TimeoutError:
                        # Send keepalive
                        yield ": keepalive\n\n"
            finally:
                _store.unsubscribe_sse(queue)

        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    # --- Notifications API (server-pushed from NodusRF via REM) ---

    @app.post("/api/notifications")
    async def receive_notification(request: Request):
        """Accept a notification from the local REM checkin handler.

        Only accepts requests from localhost (the edge container itself).
        """
        client = request.client.host if request.client else ""
        if client not in ("127.0.0.1", "::1", "localhost"):
            raise HTTPException(status_code=403, detail="Local only")

        body = await request.json()
        notif = {
            "id": secrets.token_hex(8),
            "title": body.get("title", "Notification"),
            "body": body.get("body", ""),
            "priority": body.get("priority", "medium"),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "type": "server_push",
        }
        _pending_notifications.append(notif)
        # Keep at most 50
        while len(_pending_notifications) > 50:
            _pending_notifications.pop(0)

        # Broadcast to SSE clients
        if _store is not None:
            _store.broadcast_notification(notif)

        logger.info("Dashboard notification received", title=notif["title"])
        return {"ok": True, "id": notif["id"]}

    @app.get("/api/notifications")
    async def get_notifications():
        """Get server-pushed notifications (polled by dashboard JS)."""
        return {"notifications": list(_pending_notifications)}

    # --- REST API ---

    @app.get("/api/segments")
    async def get_segments(limit: int = 50, offset: int = 0):
        """Get recent segments from the ring buffer."""
        if _store is None:
            return JSONResponse({"error": "Store not initialized"}, status_code=503)
        segments = _store.get_segments(limit=limit, offset=offset)
        return JSONResponse({"segments": segments, "count": len(segments)})

    @app.get("/api/frequencies")
    async def get_frequencies():
        """Get per-frequency activity stats plus repeater enrichment."""
        if _store is None:
            return JSONResponse({"error": "Store not initialized"}, status_code=503)

        freq_stats = _store.get_frequency_stats()

        # Enrich with repeater data from cache
        if _cache and _cache.has_repeaters:
            for freq_key, stats in freq_stats.items():
                freq_hz = stats["frequency_hz"]
                repeater = _cache.get_repeater_by_frequency(freq_hz)
                if repeater:
                    stats["repeater_callsign"] = repeater.get("Callsign") or repeater.get("callsign", "")
                    stats["repeater_city"] = repeater.get("Nearest City") or repeater.get("city", "")
                    stats["pl_tone"] = repeater.get("PL") or repeater.get("pl_tone", "")

        # Enrich with upcoming net info
        if _cache and _cache.has_nets:
            nets = _cache.get_nets()
            for freq_key, stats in freq_stats.items():
                freq_hz = stats["frequency_hz"]
                for net in nets:
                    net_freq = net.get("frequency_hz") or net.get("frequency", 0)
                    if isinstance(net_freq, str):
                        try:
                            net_freq = int(float(net_freq) * 1_000_000)
                        except ValueError:
                            continue
                    elif isinstance(net_freq, float) and net_freq < 1_000_000:
                        net_freq = int(net_freq * 1_000_000)
                    if int(net_freq) == freq_hz:
                        stats["upcoming_net"] = net.get("name", "")
                        stats["net_schedule"] = net.get("schedule", "")
                        break

        synced = _cache.has_repeaters if _cache else False
        return JSONResponse({
            "frequencies": freq_stats,
            "synced": synced,
        })

    @app.get("/api/traffic")
    async def get_traffic():
        """Get traffic overview stats."""
        if _store is None:
            return JSONResponse({"error": "Store not initialized"}, status_code=503)
        return JSONResponse(_store.get_traffic_stats())

    @app.get("/api/status")
    async def get_status():
        """Get system status for the Status tab."""
        import httpx
        from nodus_edge import __version__

        status = {
            "node_id": _node_id,
            "version": __version__,
            "cache": _cache.get_status() if _cache else {},
        }

        # Fetch pipeline/scanner stats from health server
        if _health_url:
            try:
                async with httpx.AsyncClient(timeout=3.0) as client:
                    resp = await client.get(f"{_health_url}/stats")
                    if resp.status_code == 200:
                        status["pipeline"] = resp.json()
            except Exception:
                status["pipeline"] = {"error": "Health server unreachable"}

        return JSONResponse(status)

    @app.get("/api/spectrum")
    async def get_spectrum():
        """Get spectrum data for bar chart + waterfall display."""
        if _store is None:
            return JSONResponse({"error": "Store not initialized"}, status_code=503)

        channels = _store.get_spectrum_data()

        # Enrich with repeater callsign/city from cache
        if _cache and _cache.has_repeaters:
            for ch in channels:
                repeater = _cache.get_repeater_by_frequency(ch["frequency_hz"])
                if repeater:
                    ch["repeater_callsign"] = repeater.get("Callsign") or repeater.get("callsign", "")
                    ch["repeater_city"] = repeater.get("Nearest City") or repeater.get("city", "")

        result = {
            "channels": channels,
            "squelch_db": -_squelch_snr * 3,  # dBFS for UI display
        }

        # Include recommended squelch if available
        rec_db = _store.get_recommended_squelch_db()
        if rec_db is not None:
            result["recommended_squelch_db"] = rec_db

        return JSONResponse(result)

    @app.get("/api/spectrum/events")
    async def get_spectrum_events(freq_hz: int, from_ts: float, to_ts: float):
        """Get segments matching a waterfall cell (frequency + time bucket)."""
        if _store is None:
            return JSONResponse({"error": "Store not initialized"}, status_code=503)
        segments = _store.get_spectrum_events(freq_hz, from_ts, to_ts)
        return JSONResponse({"segments": segments, "count": len(segments)})

    @app.get("/api/config")
    async def get_config():
        """Return node config for the dashboard client."""
        return JSONResponse({"timezone": _timezone, "node_id": _node_id, "metro": _metro})

    @app.get("/api/warnings")
    async def get_warnings():
        """Return startup warnings and rolling segment issue counts."""
        startup = [w.to_dict() for w in _startup_warnings]
        segment_issues = {}
        if _get_segment_warnings:
            try:
                segment_issues = _get_segment_warnings()
            except Exception:
                pass

        # Detect zero captures and suggest support sidecar
        alerts = []
        if _get_pipeline_stats:
            try:
                stats = _get_pipeline_stats()
                scanner = stats.get("scanner", {})
                capture_count = scanner.get("capture_count", stats.get("processed_count", -1))
                uptime = stats.get("uptime_seconds", 0)

                if capture_count == 0 and uptime > 1800:
                    alerts.append({
                        "code": "zero_captures",
                        "severity": "error",
                        "message": (
                            f"No audio captured in {int(uptime / 60)} minutes. "
                            "Make sure your SDR dongle is plugged in and the antenna "
                            "is connected."
                        ),
                    })
                    alerts.append({
                        "code": "enable_support",
                        "severity": "warning",
                        "message": (
                            "NodusRF can help diagnose this remotely. "
                            "Go to the Support tab to start a support session."
                        ),
                    })
                    # Auto-send diagnostic dump (one-shot)
                    _maybe_auto_dump()
            except Exception:
                pass

        return JSONResponse({
            "startup": startup,
            "segment_issues": segment_issues,
            "alerts": alerts,
        })

    # --- Debug endpoints ---

    @app.get("/api/debug")
    async def get_debug():
        """Aggregate stats, audit, and metrics from the health server."""
        import httpx

        result = {}
        if _health_url:
            try:
                async with httpx.AsyncClient(timeout=3.0) as client:
                    tasks = {
                        "stats": client.get(f"{_health_url}/stats"),
                        "audit": client.get(f"{_health_url}/audit?limit=100"),
                        "metrics": client.get(f"{_health_url}/metrics"),
                    }
                    responses = {}
                    for key, coro in tasks.items():
                        try:
                            responses[key] = await coro
                        except Exception:
                            responses[key] = None

                    for key, resp in responses.items():
                        if resp and resp.status_code == 200:
                            result[key] = resp.json()
                        else:
                            result[key] = {}
            except Exception:
                pass
        result["has_rem"] = bool(_rem_checkin)
        return JSONResponse(result)

    @app.get("/api/sdr-config")
    async def get_sdr_config():
        """Proxy SDR config and diagnostic info from health server."""
        import httpx

        if not _health_url:
            return JSONResponse({"error": "Health server not configured"}, status_code=503)
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                resp = await client.get(f"{_health_url}/sdr-config")
                return JSONResponse(resp.json(), status_code=resp.status_code)
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=502)

    @app.get("/api/audio/{filename}")
    async def get_audio(filename: str):
        """Proxy audio file request to the health server."""
        import httpx

        if not _health_url:
            return JSONResponse({"error": "Health server not configured"}, status_code=503)
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(f"{_health_url}/audio/{filename}")
                if resp.status_code == 200:
                    ct = resp.headers.get("content-type", "audio/wav")
                    return StreamingResponse(
                        iter([resp.content]),
                        media_type=ct,
                        headers={"Cache-Control": "public, max-age=86400"},
                    )
                return JSONResponse({"error": "Not found"}, status_code=resp.status_code)
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=502)

    # --- Log streaming ---

    @app.get("/events/logs")
    async def sse_logs(request: Request):
        """SSE stream of container log lines (stdout of PID 1)."""
        log_path = "/proc/1/fd/1"

        async def log_generator():
            # Send backlog (last 50 lines)
            try:
                proc_backlog = await asyncio.create_subprocess_exec(
                    "tail", "-n", "50", log_path,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL,
                )
                stdout, _ = await proc_backlog.communicate()
                for line in stdout.decode(errors="replace").splitlines():
                    event = json.dumps({
                        "line": line,
                        "ts": datetime.now(timezone.utc).isoformat(),
                    })
                    yield f"data: {event}\n\n"
            except Exception:
                pass

            # Stream live lines
            proc = await asyncio.create_subprocess_exec(
                "tail", "-f", "-n", "0", log_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            try:
                while True:
                    if await request.is_disconnected():
                        break
                    try:
                        line_bytes = await asyncio.wait_for(
                            proc.stdout.readline(), timeout=30.0
                        )
                        if not line_bytes:
                            break
                        line = line_bytes.decode(errors="replace").rstrip("\n")
                        event = json.dumps({
                            "line": line,
                            "ts": datetime.now(timezone.utc).isoformat(),
                        })
                        yield f"data: {event}\n\n"
                    except asyncio.TimeoutError:
                        yield ": keepalive\n\n"
            finally:
                proc.kill()
                await proc.wait()

        return StreamingResponse(
            log_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    @app.post("/api/debug/push-logs", dependencies=[Depends(_require_dashboard_token)])
    async def push_logs():
        """User-triggered diagnostic dump upload to REM."""
        if not _rem_checkin:
            return JSONResponse(
                {"error": "REM not configured. Set NODUSNET_SERVER."},
                status_code=400,
            )

        import httpx
        from ..diagnostic_collector import collect

        try:
            dump = collect(node_id=_node_id, get_stats=_get_pipeline_stats)
            payload = {"node_id": _node_id, "action_id": "", "dump": dump}
            upload_url = f"{_rem_checkin.rem_endpoint}/v1/edge/diagnostic-dump"
            headers = {}
            if _rem_checkin.auth_token:
                headers["Authorization"] = f"Bearer {_rem_checkin.auth_token}"
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.post(upload_url, json=payload, headers=headers)
            if resp.status_code >= 400:
                return JSONResponse(
                    {"error": f"REM returned {resp.status_code}"},
                    status_code=502,
                )
            logger.info("Manual diagnostic dump uploaded", status=resp.status_code)
            return JSONResponse({"status": "uploaded"})
        except Exception as e:
            logger.warning("Manual diagnostic dump failed", error=str(e))
            return JSONResponse({"error": str(e)}, status_code=502)

    # --- Settings endpoints ---

    @app.get("/api/env")
    async def get_env():
        """Read .env file and return grouped fields."""
        if not _env_path or not _env_path.exists():
            return JSONResponse({"fields": [], "categories": _ENV_CATEGORY_ORDER})

        env_vals = _parse_env_file(_env_path)

        fields = []
        for key, meta in _ENV_FIELDS.items():
            fields.append({
                "key": key,
                "label": meta["label"],
                "category": meta["cat"],
                "value": env_vals.get(key, ""),
            })

        return JSONResponse({"fields": fields, "categories": _ENV_CATEGORY_ORDER})

    @app.put("/api/env", dependencies=[Depends(_require_dashboard_token)])
    async def update_env(request: Request):
        """Update .env file preserving comments and structure."""
        if not _env_path:
            return JSONResponse({"error": "No .env path configured"}, status_code=503)

        try:
            body = await request.json()
            updates = body.get("fields", {})
            if not isinstance(updates, dict):
                return JSONResponse({"error": "fields must be a dict"}, status_code=400)

            # Only allow known fields
            updates = {k: v for k, v in updates.items() if k in _ENV_FIELDS}
            if not updates:
                return JSONResponse({"error": "No valid fields to update"}, status_code=400)

            _update_env_file(_env_path, updates)
            return JSONResponse({"updated": list(updates.keys())})
        except json.JSONDecodeError:
            return JSONResponse({"error": "Invalid JSON"}, status_code=400)
        except Exception as e:
            logger.error("env_update_error", error=str(e))
            return JSONResponse({"error": str(e)}, status_code=500)

    @app.get("/api/version-check")
    async def version_check():
        """Check if a newer version is available from the Gateway manifest."""
        import httpx
        from nodus_edge import __version__

        current = __version__
        result = {"current_version": current, "latest_version": None,
                  "update_available": False}

        # Suppress banner for dev/non-CI builds
        if current in ("0.1.0", "dev"):
            return JSONResponse(result)

        if not _cache or not _cache.gateway_url:
            result["error"] = "No gateway configured"
            return JSONResponse(result)

        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(
                    f"{_cache.gateway_url.rstrip('/')}/v1/edge/manifest"
                )
                if resp.status_code == 200:
                    manifest = resp.json()
                    latest = (manifest.get("images", {})
                              .get("nodus-edge", {})
                              .get("tag"))
                    if latest:
                        result["latest_version"] = latest
                        result["update_available"] = (latest != current)

                    # Check compose version
                    compose_latest = (manifest.get("compose", {})
                                      .get("version"))
                    if compose_latest and compose_latest != COMPOSE_VERSION:
                        result["compose_update_available"] = True
                        result["compose_version"] = {
                            "current": COMPOSE_VERSION,
                            "latest": compose_latest,
                        }
                else:
                    result["error"] = f"Manifest HTTP {resp.status_code}"
        except Exception as e:
            result["error"] = str(e)

        return JSONResponse(result)

    @app.post("/api/restart", dependencies=[Depends(_require_dashboard_token)])
    async def request_restart():
        """Write a restart signal file for the container entrypoint to detect."""
        signal_path = Path("/data/.restart-signal")
        try:
            signal_path.parent.mkdir(parents=True, exist_ok=True)
            signal_path.write_text("restart requested from dashboard")
            return JSONResponse({"status": "restart signal written"})
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=500)

    @app.post("/api/squelch", dependencies=[Depends(_require_dashboard_token)])
    async def update_squelch(request: Request):
        """Proxy squelch update to health server and persist to .env."""
        import httpx

        if not _health_url:
            return JSONResponse({"error": "Health server not configured"}, status_code=503)

        try:
            body = await request.json()
            squelch_db = body.get("squelch_db")
            if squelch_db is None:
                return JSONResponse({"error": "squelch_db required"}, status_code=400)

            # Convert dBFS → SNR for internal airband pipeline
            snr_db = -float(squelch_db) / 3

            # Proxy to health server
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.post(
                    f"{_health_url}/squelch",
                    json={"squelch_snr_db": snr_db},
                )
                result = resp.json()

            # Update module-level squelch for spectrum API
            global _squelch_snr
            if result.get("applied"):
                _squelch_snr = snr_db
                # Persist to .env if path configured
                if _env_path:
                    try:
                        _update_env_file(_env_path, {"NODUS_EDGE_FM_AIRBAND_SQUELCH_SNR_DB": str(snr_db)})
                    except Exception as e:
                        logger.warning("Failed to persist squelch to .env", error=str(e))

            return JSONResponse(result, status_code=resp.status_code)
        except json.JSONDecodeError:
            return JSONResponse({"error": "Invalid JSON"}, status_code=400)
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=502)

    # --- Synapse/Sync ---

    @app.post("/api/synapse/toggle", dependencies=[Depends(_require_dashboard_token)])
    async def synapse_toggle(request: Request):
        """Proxy Synapse pause/unpause to the health server."""
        import httpx

        if not _health_url:
            return JSONResponse({"error": "Health server not configured"}, status_code=503)
        try:
            body = await request.body()
            async with httpx.AsyncClient(timeout=3.0) as client:
                resp = await client.post(f"{_health_url}/synapse/toggle", content=body,
                                         headers={"Content-Type": "application/json"})
                return JSONResponse(resp.json(), status_code=resp.status_code)
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=502)

    @app.post("/api/sync")
    def trigger_sync():
        """Trigger a manual sync of repeater/net data from Gateway.

        Uses a plain def (not async) so FastAPI runs the blocking
        httpx call in a threadpool instead of blocking the event loop.
        """
        if _cache is None:
            return JSONResponse({"error": "Cache not initialized"}, status_code=503)
        if not _cache.can_sync:
            return JSONResponse(
                {"error": "No gateway URL configured. Set NODUSNET_SERVER."},
                status_code=400,
            )
        result = _cache.sync()
        return JSONResponse(result)

    # --- Support ---

    # Map dashboard-facing tags to support-agent router tags
    _TAG_MAP = {
        "no-audio": "no_segments",
        "no-transcript": "whisper_fail",
        "crashed": "container_crash",
        "usb": "usb_issue",
        "disk-full": "disk_full",
        "network": "connectivity",
        "slow": "slow",
        "update": "outdated",
    }

    def _map_tags(tags):
        """Translate dashboard tags to support-agent router tags."""
        return list(dict.fromkeys(_TAG_MAP.get(t, t) for t in tags))

    # Support session state (in-memory; resets on container restart)
    _support_state = {
        "active": False,
        "description": "",
        "tags": [],
        "started_at": None,
        "expires_at": None,
        "tunnel_hostname": "",
        "result": None,
        "current_tier": 1,
        "host_diagnostics": {},
        "version": {},
    }

    @app.post("/api/support/start", dependencies=[Depends(_require_dashboard_token)])
    async def support_start(request: Request):
        """Start a support session: gather diagnostics, start sidecar, relay to Gateway."""
        import subprocess
        import os

        if _support_state["active"]:
            return JSONResponse({"error": "Support session already active"}, status_code=409)

        try:
            body = await request.json()
        except Exception:
            body = {}

        description = (body.get("description") or "").strip()
        tags = _map_tags(body.get("tags") or [])
        ttl = int(os.environ.get("NODUS_EDGE_SUPPORT_TTL", "10800"))

        # Gather host diagnostics (these run on the host, not in the sidecar)
        host_diag = {}
        try:
            r = subprocess.run(["lsusb"], capture_output=True, text=True, timeout=5)
            # Parse lsusb into structured list for playbook matching
            usb_list = []
            for line in r.stdout.strip().splitlines():
                usb_list.append({"raw": line, "vendor": line.lower()})
            host_diag["usb_devices"] = usb_list
        except Exception:
            host_diag["usb_devices"] = []

        try:
            r = subprocess.run(["df", "--output=pcent", "/data"], capture_output=True, text=True, timeout=5)
            lines = r.stdout.strip().split("\n")
            pct = int(lines[-1].strip().rstrip("%")) if len(lines) > 1 else 0
            host_diag["disk_usage"] = {"percent": pct}
        except Exception:
            host_diag["disk_usage"] = {"percent": 0}

        try:
            r = subprocess.run(["free", "-m"], capture_output=True, text=True, timeout=5)
            lines = r.stdout.strip().split("\n")
            if len(lines) > 1:
                parts = lines[1].split()
                total = int(parts[1]) if len(parts) > 1 else 1
                used = int(parts[2]) if len(parts) > 2 else 0
                host_diag["memory"] = {"percent": round(used / total * 100)}
            else:
                host_diag["memory"] = {"percent": 0}
        except Exception:
            host_diag["memory"] = {"percent": 0}

        # Check for USB contention (playbook expects a bool)
        usb_contention = False
        try:
            r = subprocess.run(
                ["fuser", "/dev/bus/usb/001/001"],
                capture_output=True, text=True, timeout=5,
            )
            if r.stdout.strip():
                usb_contention = True
        except Exception:
            pass
        host_diag["usb_contention"] = usb_contention

        # Get version info from environment
        version = {
            "nodus_edge": os.environ.get("NODUS_VERSION", "unknown"),
            "edge_stack": os.environ.get("NODUS_VERSION", "unknown"),
        }

        # Provision ephemeral CF tunnel via REM
        tunnel_hostname = ""
        rem_url = os.environ.get("NODUS_EDGE_REM_URL", os.environ.get("NODUS_EDGE_REM_ENDPOINT", os.environ.get("NODUSNET_SERVER", "")))
        if rem_url:
            try:
                import httpx
                async with httpx.AsyncClient(timeout=15.0) as hc:
                    t_resp = await hc.post(
                        f"{rem_url.rstrip('/')}/v1/tunnels/provision",
                        json={"node_id": _node_id, "ttl_seconds": ttl},
                        headers={"Authorization": f"Bearer {os.environ.get('NODUS_EDGE_REM_ADMIN_KEY', '')}"},
                    )
                    if t_resp.status_code == 200:
                        t_data = t_resp.json()
                        tunnel_token = t_data.get("tunnel_token", "")
                        tunnel_hostname = t_data.get("hostname", "")
                        if tunnel_token and _env_path:
                            _update_env_file(_env_path, {"NODUS_TUNNEL_TOKEN": tunnel_token})
                        logger.info("tunnel_provisioned", hostname=tunnel_hostname)
                    else:
                        logger.warning("tunnel_provision_failed", status=t_resp.status_code, body=t_resp.text[:200])
            except Exception as e:
                logger.warning("tunnel_provision_error", error=str(e))

        # Try to start the support sidecar
        try:
            r = subprocess.run(
                ["docker", "compose", "--profile", "support", "up", "-d", "support-sidecar"],
                capture_output=True, text=True, timeout=30,
                cwd=os.environ.get("COMPOSE_PROJECT_DIR", "/app"),
            )
            if r.returncode != 0:
                logger.warning("support_sidecar_start_failed", stderr=r.stderr[:200])
                return JSONResponse(
                    {"error": "Failed to start support tunnel", "detail": r.stderr[:200]},
                    status_code=500,
                )
        except FileNotFoundError:
            # Docker CLI not available inside the container — signal via file
            signal_path = Path("/data/.support-start-signal")
            signal_path.write_text(json.dumps({
                "description": description,
                "tags": tags,
                "host_diagnostics": host_diag,
                "version": version,
            }))
            logger.info("support_signal_written", path=str(signal_path))
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=500)

        from datetime import datetime, timezone, timedelta
        now = datetime.now(timezone.utc)
        expires = now + timedelta(seconds=ttl)

        _support_state.update({
            "active": True,
            "description": description,
            "tags": tags,
            "started_at": now.isoformat(),
            "expires_at": expires.isoformat(),
            "tunnel_hostname": tunnel_hostname,
            "result": None,
            "current_tier": 1,
            "host_diagnostics": host_diag,
            "version": version,
        })

        # Relay support request to Gateway (primary path)
        support_payload = {
            "node_id": _node_id,
            "callsign": os.environ.get("NODUS_EDGE_CALLSIGN", ""),
            "description": description,
            "tags": tags,
            "version": version,
            "host_diagnostics": host_diag,
        }
        gateway_url = os.environ.get("NODUS_EDGE_GATEWAY_URL")
        if gateway_url:
            try:
                import httpx
                async with httpx.AsyncClient(timeout=10.0) as client:
                    resp = await client.post(
                        f"{gateway_url.rstrip('/')}/v1/edge/support",
                        json=support_payload,
                    )
                    logger.info("support_gateway_relay", status=resp.status_code)
            except Exception as e:
                logger.warning("support_gateway_relay_failed", error=str(e))

        # Schedule auto-expire
        asyncio.get_event_loop().call_later(ttl, _expire_support)

        logger.info(
            "support_session_started",
            description=description[:100],
            tags=tags,
            ttl=ttl,
        )

        return JSONResponse({
            "status": "active",
            "active": True,
            "tags": tags,
            "expires_at": expires.isoformat(),
            "tunnel_hostname": tunnel_hostname,
        })

    @app.post("/api/support/stop", dependencies=[Depends(_require_dashboard_token)])
    async def support_stop():
        """Stop the support session and tear down the sidecar."""
        import subprocess
        import os

        if not _support_state["active"]:
            return JSONResponse({"error": "No active support session"}, status_code=404)

        # Stop sidecar
        try:
            subprocess.run(
                ["docker", "compose", "--profile", "support", "stop", "support-sidecar"],
                capture_output=True, text=True, timeout=30,
                cwd=os.environ.get("COMPOSE_PROJECT_DIR", "/app"),
            )
        except FileNotFoundError:
            signal_path = Path("/data/.support-stop-signal")
            signal_path.write_text("stop")
        except Exception as e:
            logger.warning("support_sidecar_stop_error", error=str(e))

        # Teardown CF tunnel via REM
        rem_url = os.environ.get("NODUS_EDGE_REM_URL", os.environ.get("NODUS_EDGE_REM_ENDPOINT", os.environ.get("NODUSNET_SERVER", "")))
        if rem_url:
            try:
                import httpx
                async with httpx.AsyncClient(timeout=15.0) as hc:
                    await hc.post(
                        f"{rem_url.rstrip('/')}/v1/tunnels/{_node_id}/teardown",
                        headers={"Authorization": f"Bearer {os.environ.get('NODUS_EDGE_REM_ADMIN_KEY', '')}"},
                    )
            except Exception as e:
                logger.warning("tunnel_teardown_error", error=str(e))
        if _env_path:
            _update_env_file(_env_path, {"NODUS_TUNNEL_TOKEN": ""})

        _support_state.update({
            "active": False,
            "description": "",
            "tags": [],
            "started_at": None,
            "expires_at": None,
            "tunnel_hostname": "",
        })

        _support_state.update({
            "current_tier": 1,
            "host_diagnostics": {},
            "version": {},
        })

        logger.info("support_session_stopped", reason="user_ended")
        return JSONResponse({"status": "stopped"})

    @app.post("/api/support/escalate", dependencies=[Depends(_require_dashboard_token)])
    async def support_escalate(request: Request):
        """Escalate from Tier 1 to Tier 2: user indicated issue not resolved."""
        import os

        if not _support_state["active"]:
            return JSONResponse({"error": "No active support session"}, status_code=404)
        if _support_state["current_tier"] >= 2:
            return JSONResponse({"error": "Already escalated"}, status_code=409)

        try:
            body = await request.json()
        except Exception:
            body = {}

        user_feedback = (body.get("feedback") or "").strip()
        session_id = body.get("session_id", "")

        if not user_feedback:
            return JSONResponse({"error": "Feedback is required"}, status_code=400)

        _support_state["current_tier"] = 2
        _support_state["result"] = None  # Clear Tier 1 result for Tier 2 polling

        escalation_payload = {
            "node_id": _node_id,
            "callsign": os.environ.get("NODUS_EDGE_CALLSIGN", ""),
            "tier1_session_id": session_id,
            "user_feedback": user_feedback,
            "description": _support_state.get("description", ""),
            "tags": _support_state.get("tags", []),
            "version": _support_state.get("version", {}),
            "host_diagnostics": _support_state.get("host_diagnostics", {}),
            "action": "escalate",
        }

        # Relay to Gateway (primary path)
        gateway_url = os.environ.get("NODUS_EDGE_GATEWAY_URL")
        if gateway_url:
            try:
                import httpx
                async with httpx.AsyncClient(timeout=10.0) as client:
                    resp = await client.post(
                        f"{gateway_url.rstrip('/')}/v1/edge/support/escalate",
                        json=escalation_payload,
                    )
                    logger.info("support_escalate_relay", status=resp.status_code)
            except Exception as e:
                logger.warning("support_escalate_relay_failed", error=str(e))

        logger.info("support_escalated_to_tier2", session_id=session_id, feedback=user_feedback[:100])
        return JSONResponse({"status": "escalated", "current_tier": 2})

    @app.get("/api/support/status")
    async def support_status():
        """Get current support session status, polling Gateway for results."""
        import os
        import httpx

        if _support_state["active"] and not _support_state["result"]:
            gateway_url = os.environ.get("NODUS_EDGE_GATEWAY_URL")
            current_tier = _support_state.get("current_tier", 1)
            if gateway_url:
                try:
                    async with httpx.AsyncClient(timeout=5.0) as client:
                        resp = await client.get(
                            f"{gateway_url.rstrip('/')}/v1/edge/support/result/{_node_id}"
                        )
                        if resp.status_code == 200:
                            data = resp.json()
                            if data.get("status") != "pending":
                                # Only accept results matching current tier
                                result_tier = data.get("escalation_tier", 1)
                                if result_tier == current_tier:
                                    created = data.get("created_at", "")
                                    started = _support_state.get("started_at", "")
                                    if not started or not created or created >= started:
                                        _support_state["result"] = data
                except Exception:
                    pass

        response = dict(_support_state)
        rem_url = os.environ.get("NODUS_EDGE_REM_URL", os.environ.get("NODUS_EDGE_REM_ENDPOINT", os.environ.get("NODUSNET_SERVER", "")))
        gateway_url = os.environ.get("NODUS_EDGE_GATEWAY_URL")
        response["has_nodusnet"] = bool(rem_url or gateway_url)
        return JSONResponse(response)

    @app.post("/api/support/result")
    async def support_result(request: Request):
        """Receive support session results from the Support Agent (via Gateway or direct POST)."""
        try:
            result = await request.json()
            _support_state["result"] = result
            logger.info("support_result_received", status=result.get("status"))
            return JSONResponse({"status": "ok"})
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=400)

    @app.get("/api/support/progress")
    async def support_progress():
        """Proxy live progress events from Gateway for the active support session."""
        import os
        import httpx

        gateway_url = os.environ.get("NODUS_EDGE_GATEWAY_URL")
        if not gateway_url:
            return JSONResponse({"events": []})
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                resp = await client.get(
                    f"{gateway_url.rstrip('/')}/v1/edge/support/progress/{_node_id}"
                )
                if resp.status_code == 200:
                    return JSONResponse(resp.json())
        except Exception:
            pass
        return JSONResponse({"events": []})

    @app.post("/api/support/apply-fix", dependencies=[Depends(_require_dashboard_token)])
    async def apply_support_fix(request: Request):
        """Apply proposed env var fixes from a support session result."""
        if not _env_path:
            return JSONResponse({"error": "No .env path configured"}, status_code=503)

        try:
            body = await request.json()
            fixes = body.get("fixes", [])
            if not fixes or not isinstance(fixes, list):
                return JSONResponse({"error": "fixes array required"}, status_code=400)

            # Only allow known, whitelisted env var keys
            updates = {}
            skipped = []
            for fix in fixes:
                var = fix.get("var", "")
                val = fix.get("proposed_value", "")
                if var in _ENV_FIELDS:
                    updates[var] = val
                else:
                    skipped.append(var)

            if not updates:
                return JSONResponse({"error": "No valid env vars to update", "skipped": skipped}, status_code=400)

            _update_env_file(_env_path, updates)
            logger.info("support_fix_applied", updated=list(updates.keys()))

            # Check if any fix requires a restart
            needs_restart = any(fix.get("requires_restart", True) for fix in fixes if fix.get("var") in updates)

            result = {"updated": list(updates.keys()), "skipped": skipped, "restart_required": needs_restart}

            if needs_restart:
                # Write restart signal — picked up by host systemd path unit
                signal_path = Path("/data/.restart-signal")
                try:
                    signal_path.parent.mkdir(parents=True, exist_ok=True)
                    signal_path.write_text("restart after support fix")
                    result["restart"] = "signal written"
                except Exception as e:
                    result["restart"] = f"signal failed: {e}"

            return JSONResponse(result)
        except json.JSONDecodeError:
            return JSONResponse({"error": "Invalid JSON"}, status_code=400)
        except Exception as e:
            logger.error("apply_fix_error", error=str(e))
            return JSONResponse({"error": str(e)}, status_code=500)

    def _get_container_status():
        """Get Docker container status (best-effort)."""
        import subprocess
        try:
            r = subprocess.run(
                ["docker", "compose", "ps", "--format", "json"],
                capture_output=True, text=True, timeout=10,
            )
            if r.returncode == 0 and r.stdout.strip():
                containers = {}
                for line in r.stdout.strip().split("\n"):
                    try:
                        c = json.loads(line)
                        containers[c.get("Name", "?")] = c.get("State", "unknown")
                    except Exception:
                        pass
                return containers
        except Exception:
            pass
        return {}

    async def _teardown_tunnel_async(rem_url: str, node_id: str):
        """Async helper: call REM tunnel teardown endpoint."""
        import os
        try:
            import httpx
            async with httpx.AsyncClient(timeout=15.0) as hc:
                await hc.post(
                    f"{rem_url.rstrip('/')}/v1/tunnels/{node_id}/teardown",
                    headers={"Authorization": f"Bearer {os.environ.get('NODUS_EDGE_REM_ADMIN_KEY', '')}"},
                )
        except Exception as e:
            logger.warning("tunnel_teardown_async_error", error=str(e))


    def _expire_support():
        """Auto-expire support session after TTL."""
        import subprocess
        import os
        if not _support_state["active"]:
            return

        try:
            subprocess.run(
                ["docker", "compose", "--profile", "support", "stop", "support-sidecar"],
                capture_output=True, text=True, timeout=30,
                cwd=os.environ.get("COMPOSE_PROJECT_DIR", "/app"),
            )
        except Exception:
            pass

        # Teardown CF tunnel via REM (fire-and-forget via new event loop)
        rem_url = os.environ.get("NODUS_EDGE_REM_URL", os.environ.get("NODUS_EDGE_REM_ENDPOINT", os.environ.get("NODUSNET_SERVER", "")))
        if rem_url:
            import httpx
            try:
                # _expire_support is called from call_later (sync context),
                # so we need to schedule the async teardown
                loop = asyncio.get_event_loop()
                loop.create_task(_teardown_tunnel_async(rem_url, _node_id))
            except Exception as e:
                logger.warning("tunnel_teardown_schedule_error", error=str(e))
        if _env_path:
            _update_env_file(_env_path, {"NODUS_TUNNEL_TOKEN": ""})

        _support_state.update({
            "active": False,
            "description": "",
            "tags": [],
            "started_at": None,
            "expires_at": None,
            "tunnel_hostname": "",
            "current_tier": 1,
            "host_diagnostics": {},
            "version": {},
        })
        logger.info("support_session_expired")

    # --- Feedback ---

    @app.post("/api/feedback")
    async def submit_feedback(request: Request):
        """Accept feedback from the dashboard UI and forward to Gateway."""
        import os
        import httpx

        try:
            body = await request.json()
            title = body.get("title", "").strip()
            fb_body = body.get("body", "").strip()
            category = body.get("category", "General Feedback")
            page_url = body.get("page_url", "")
            node_id = body.get("node_id", "unknown")

            if not fb_body:
                return JSONResponse({"error": "Message required"}, status_code=400)

            # Always log locally
            logger.info(
                "feedback_received",
                title=title[:120],
                category=category,
                node_id=node_id,
                body=fb_body[:200],
            )

            # Forward to Gateway if available
            gateway_url = os.environ.get("NODUS_EDGE_GATEWAY_URL")
            if gateway_url:
                payload = {
                    "title": title or category,
                    "body": f"[NodusEdge {node_id}] {fb_body}",
                    "category": category,
                    "page_url": page_url,
                }
                try:
                    async with httpx.AsyncClient(timeout=5.0) as client:
                        resp = await client.post(
                            f"{gateway_url.rstrip('/')}/v1/feedback",
                            json=payload,
                        )
                        if resp.status_code < 300:
                            return JSONResponse({"status": "forwarded"})
                        logger.warning(
                            "feedback_forward_failed",
                            status=resp.status_code,
                            detail=resp.text[:200],
                        )
                except Exception as fwd_err:
                    logger.warning("feedback_forward_error", error=str(fwd_err))

            # Fallback: logged locally above
            return JSONResponse({"status": "received"})
        except Exception as e:
            return JSONResponse({"error": str(e)}, status_code=400)

    # --- Static files (SPA) ---

    @app.get("/")
    async def index():
        """Serve the dashboard SPA."""
        index_path = STATIC_DIR / "index.html"
        if index_path.exists():
            return HTMLResponse(index_path.read_text())
        return HTMLResponse("<h1>Dashboard files not found</h1>", status_code=500)

    # Mount static files for CSS/JS
    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    return app


def _parse_env_file(path: Path) -> Dict[str, str]:
    """Parse a .env file into a dict, ignoring comments and blank lines."""
    result = {}
    if not path.exists():
        return result
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, _, val = line.partition("=")
            result[key.strip()] = val.strip()
    return result


def _update_env_file(path: Path, updates: Dict[str, str]) -> None:
    """Update specific keys in a .env file, preserving comments and structure."""
    if path.exists():
        lines = path.read_text().splitlines()
    else:
        lines = []

    updated_keys = set()

    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" in stripped:
            key = stripped.split("=", 1)[0].strip()
            if key in updates:
                lines[i] = f"{key}={updates[key]}"
                updated_keys.add(key)

    # Append any keys not already in the file
    for key, val in updates.items():
        if key not in updated_keys:
            lines.append(f"{key}={val}")

    path.write_text("\n".join(lines) + "\n")


def start_dashboard(
    store: SegmentStore,
    cache: SyncCache,
    port: int = 8073,
    node_id: str = "unknown",
    health_port: int = 8082,
    **kwargs,
) -> Thread:
    """
    Start the dashboard server as a daemon thread.

    Args:
        store: SegmentStore instance (shared with pipeline callback)
        cache: SyncCache instance for repeater/net data
        port: HTTP port for the dashboard
        node_id: Node identifier
        health_port: Port of the existing health server

    Returns:
        The daemon thread running the server.
    """
    global _store, _cache, _health_url, _node_id, _squelch_snr, _timezone, _metro, _env_path
    global _startup_warnings, _get_segment_warnings, _dashboard_token
    global _get_pipeline_stats, _rem_checkin
    _store = store
    _cache = cache
    _health_url = f"http://127.0.0.1:{health_port}"
    _node_id = node_id
    _timezone = kwargs.pop("timezone", "") or ""
    _metro = kwargs.pop("metro", "") or ""
    _dashboard_token = kwargs.pop("dashboard_token", "") or ""

    # Startup warnings from validation
    _startup_warnings = kwargs.pop("startup_warnings", []) or []

    # Segment warning count getter (callable from pipeline)
    _get_segment_warnings = kwargs.pop("get_segment_warnings", None)

    # Pipeline stats + REM check-in for auto-diagnostics
    _get_pipeline_stats = kwargs.pop("get_pipeline_stats", None)
    _rem_checkin = kwargs.pop("rem_checkin", None)

    # Env path for settings editor
    env_path = kwargs.pop("env_path", None)
    if env_path is not None:
        _env_path = Path(env_path)

    # Spectrum configuration
    channel_frequencies = kwargs.get("channel_frequencies")
    squelch_snr_db = kwargs.get("squelch_snr_db")
    if channel_frequencies:
        store.init_channels(channel_frequencies)
    if squelch_snr_db is not None:
        _squelch_snr = float(squelch_snr_db)

    app = create_app()

    config = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=port,
        log_level="warning",
        access_log=False,
    )
    server = uvicorn.Server(config)

    thread = Thread(target=server.run, daemon=True, name="dashboard")
    thread.start()
    logger.info("Dashboard server started", port=port, url=f"http://localhost:{port}")

    return thread
