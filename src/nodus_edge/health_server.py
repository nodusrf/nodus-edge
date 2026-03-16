"""
Simple HTTP health server for Nodus Edge.

Runs on a separate thread to provide health/stats endpoints
for monitoring tools like nodus-diagnostics, plus audio file
serving for playback features.
"""

import json
import os
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from threading import Thread
from typing import Callable, Dict, Any, Optional

import structlog

logger = structlog.get_logger(__name__)


class HealthHandler(BaseHTTPRequestHandler):
    """HTTP handler for health, audio, and audit endpoints."""

    # Class-level references (set by HealthServer.start)
    # Stored in a list to prevent Python's descriptor protocol from binding
    # the function as a method (which would pass self as first argument)
    _get_stats: list = [lambda: {}]
    _operator_cache: list = [None]  # OperatorCache instance (optional)
    _audit_log: list = [None]  # TranscriptionAuditLog instance (optional)
    _synapse_publisher: list = [None]  # SynapsePublisher instance (optional)
    _scanner: list = [None]  # AirbandScanner instance (optional)
    node_id: str = "unknown"
    start_time: datetime = datetime.now(timezone.utc)
    audio_dir: Optional[Path] = None  # FM capture directory for audio serving

    def log_message(self, format, *args):
        """Suppress default HTTP logging."""
        pass

    def do_GET(self):
        """Handle GET requests."""
        if self.path == "/health":
            self._send_health()
        elif self.path == "/stats":
            self._send_stats()
        elif self.path == "/audit" or self.path.startswith("/audit?"):
            self._send_audit()
        elif self.path == "/metrics":
            self._send_metrics()
        elif self.path.startswith("/audio/"):
            self._send_audio()
        elif self.path == "/sdr-config":
            self._send_sdr_config()
        else:
            self._send_not_found()

    def do_HEAD(self):
        """Handle HEAD requests — headers only, no body."""
        if self.path.startswith("/audio/"):
            self._send_audio(head_only=True)
        else:
            self.do_GET()

    def do_POST(self):
        """Handle POST requests."""
        if self.path == "/operators":
            self._receive_operators()
        elif self.path == "/synapse/toggle":
            self._toggle_synapse()
        elif self.path == "/squelch":
            self._update_squelch()
        else:
            self._send_json(404, {"error": "Not found"})

    def do_OPTIONS(self):
        """Handle CORS preflight requests."""
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Range")
        self.end_headers()

    def _send_health(self):
        """Send health check response."""
        stats = self._get_stats[0]()
        uptime = (datetime.now(timezone.utc) - self.start_time).total_seconds()

        response = {
            "status": "healthy",
            "node_id": self.node_id,
            "uptime_seconds": uptime,
            "processed_count": stats.get("processed_count", 0),
            "error_count": stats.get("error_count", 0),
        }

        self._send_json(200, response)

    def _send_stats(self):
        """Send full statistics."""
        stats = self._get_stats[0]()
        uptime = (datetime.now(timezone.utc) - self.start_time).total_seconds()

        response = {
            "node_id": self.node_id,
            "uptime_seconds": uptime,
            **stats,
        }

        self._send_json(200, response)

    def _send_audio(self, head_only: bool = False):
        """Serve a WAV audio file from the FM capture directory."""
        if not self.audio_dir:
            self._send_json(503, {"error": "Audio serving not configured"})
            return

        # Extract filename from path (strip /audio/ prefix)
        filename = self.path[7:]  # len("/audio/") == 7

        # Path traversal protection
        if not filename or "/" in filename or "\\" in filename or ".." in filename:
            self._send_json(400, {"error": "Invalid filename"})
            return

        # Only allow WAV and MP3 files
        lower = filename.lower()
        if not (lower.endswith(".wav") or lower.endswith(".mp3")):
            self._send_json(400, {"error": "Only WAV and MP3 files are served"})
            return

        filepath = self.audio_dir / filename
        if not filepath.is_file():
            # Also check the airband subdirectory
            filepath = self.audio_dir / "airband" / filename
            if not filepath.is_file():
                self._send_json(404, {"error": "Audio file not found"})
                return

        # Verify resolved path is still within audio_dir (defense in depth)
        try:
            resolved = filepath.resolve()
            audio_resolved = self.audio_dir.resolve()
            if not str(resolved).startswith(str(audio_resolved)):
                self._send_json(403, {"error": "Access denied"})
                return
        except (OSError, ValueError):
            self._send_json(500, {"error": "Path resolution failed"})
            return

        content_type = "audio/wav" if lower.endswith(".wav") else "audio/mpeg"
        file_size = filepath.stat().st_size

        try:
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(file_size))
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Cache-Control", "public, max-age=86400")
            self.end_headers()

            if not head_only:
                with open(filepath, "rb") as f:
                    # Stream in 64KB chunks
                    while True:
                        chunk = f.read(65536)
                        if not chunk:
                            break
                        self.wfile.write(chunk)
        except (BrokenPipeError, ConnectionResetError):
            pass  # Client disconnected

    def _receive_operators(self):
        """Receive operator data from Synapse and update local cache."""
        cache = self._operator_cache[0]
        if cache is None:
            self._send_json(503, {"error": "Operator cache not configured"})
            return

        try:
            content_length = int(self.headers.get("Content-Length", 0))
            if content_length == 0 or content_length > 1_000_000:  # 1MB limit
                self._send_json(400, {"error": "Invalid content length"})
                return

            body = self.rfile.read(content_length)
            data = json.loads(body)

            operators = data.get("operators_by_frequency")
            if not isinstance(operators, dict):
                self._send_json(400, {"error": "Expected operators_by_frequency dict"})
                return

            cache.update(operators)
            total = sum(len(v) for v in operators.values())
            logger.info(
                "Operator cache updated via POST",
                frequencies=len(operators),
                total_operators=total,
            )
            self._send_json(200, {
                "status": "ok",
                "frequencies": len(operators),
                "total_operators": total,
            })
        except json.JSONDecodeError:
            self._send_json(400, {"error": "Invalid JSON"})
        except Exception as e:
            logger.error("operator_update_error", error=str(e))
            self._send_json(500, {"error": str(e)})

    def _toggle_synapse(self):
        """Toggle Synapse publishing (pause/unpause)."""
        publisher = self._synapse_publisher[0]
        if publisher is None:
            self._send_json(503, {"error": "Synapse publisher not configured"})
            return

        try:
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length) if content_length > 0 else b"{}"
            data = json.loads(body)
            action = data.get("action", "")

            if action == "pause":
                publisher.pause()
            elif action == "unpause":
                publisher.unpause()
            else:
                self._send_json(400, {"error": "action must be 'pause' or 'unpause'"})
                return

            self._send_json(200, publisher.get_stats())
        except json.JSONDecodeError:
            self._send_json(400, {"error": "Invalid JSON"})
        except Exception as e:
            logger.error("synapse_toggle_error", error=str(e))
            self._send_json(500, {"error": str(e)})

    def _update_squelch(self):
        """Update squelch SNR threshold at runtime."""
        scanner = self._scanner[0]
        if scanner is None or not hasattr(scanner, 'update_squelch'):
            self._send_json(503, {"error": "Scanner not configured"})
            return

        try:
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length) if content_length > 0 else b"{}"
            data = json.loads(body)
            snr_db = data.get("squelch_snr_db")

            if snr_db is None or not isinstance(snr_db, (int, float)):
                self._send_json(400, {"error": "squelch_snr_db required (number)"})
                return
            if snr_db < 1.0 or snr_db > 20.0:
                self._send_json(400, {"error": "squelch_snr_db must be 1.0–20.0"})
                return

            result = scanner.update_squelch(float(snr_db))
            self._send_json(200, result)
        except json.JSONDecodeError:
            self._send_json(400, {"error": "Invalid JSON"})
        except Exception as e:
            logger.error("squelch_update_error", error=str(e))
            self._send_json(500, {"error": str(e)})

    def _send_sdr_config(self):
        """Send SDR hardware config and diagnostic info."""
        scanner = self._scanner[0]
        if scanner is None or not hasattr(scanner, 'get_sdr_config'):
            self._send_json(503, {"error": "Scanner not configured"})
            return

        try:
            config = scanner.get_sdr_config()
            self._send_json(200, config)
        except Exception as e:
            logger.error("sdr_config_error", error=str(e))
            self._send_json(500, {"error": str(e)})

    def _send_audit(self):
        """Send recent transcription audit entries."""
        audit = self._audit_log[0]
        if audit is None:
            self._send_json(503, {"error": "Audit log not configured"})
            return

        # Parse limit and outcome from query string
        limit = 50
        outcome = None
        if "?" in self.path:
            from urllib.parse import parse_qs, urlparse
            qs = parse_qs(urlparse(self.path).query)
            try:
                limit = int(qs.get("limit", ["50"])[0])
            except (ValueError, IndexError):
                pass
            outcome_vals = qs.get("outcome", [])
            if outcome_vals and outcome_vals[0]:
                outcome = outcome_vals[0]

        entries = audit.get_recent(limit=limit, outcome=outcome)
        self._send_json(200, {"entries": entries, "count": len(entries)})

    def _send_metrics(self):
        """Send transcription quality metrics."""
        audit = self._audit_log[0]
        if audit is None:
            self._send_json(503, {"error": "Audit log not configured"})
            return

        metrics = audit.get_metrics()
        self._send_json(200, metrics)

    def _send_not_found(self):
        """Send 404 response."""
        self._send_json(404, {"error": "Not found"})

    def _send_json(self, status: int, data: Dict[str, Any]):
        """Send JSON response."""
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())


class HealthServer:
    """
    HTTP health server for Nodus Edge monitoring.

    Runs on port 8082 by default, provides /health, /stats,
    and /audio/{filename} endpoints.
    """

    def __init__(
        self,
        port: int = 8082,
        node_id: str = "unknown",
        get_stats: Optional[Callable[[], Dict[str, Any]]] = None,
        audio_dir: Optional[Path] = None,
        operator_cache=None,
        audit_log=None,
        synapse_publisher=None,
        scanner=None,
    ):
        self.port = port
        self.node_id = node_id
        self.get_stats = get_stats or (lambda: {})
        self.audio_dir = audio_dir
        self.operator_cache = operator_cache
        self.audit_log = audit_log
        self.synapse_publisher = synapse_publisher
        self.scanner = scanner
        self._server: Optional[HTTPServer] = None
        self._thread: Optional[Thread] = None

    def start(self):
        """Start the health server in a background thread."""
        # Configure handler
        HealthHandler._get_stats = [self.get_stats]
        HealthHandler._operator_cache = [self.operator_cache]
        HealthHandler._audit_log = [self.audit_log]
        HealthHandler._synapse_publisher = [self.synapse_publisher]
        HealthHandler._scanner = [self.scanner]
        HealthHandler.node_id = self.node_id
        HealthHandler.start_time = datetime.now(timezone.utc)
        HealthHandler.audio_dir = self.audio_dir

        try:
            self._server = HTTPServer(("0.0.0.0", self.port), HealthHandler)
            self._thread = Thread(target=self._server.serve_forever, daemon=True)
            self._thread.start()
            logger.info("Health server started", port=self.port,
                       audio_serving=self.audio_dir is not None)
        except OSError as e:
            logger.warning("Could not start health server", port=self.port, error=str(e))

    def stop(self):
        """Stop the health server."""
        if self._server:
            self._server.shutdown()
            logger.info("Health server stopped")
