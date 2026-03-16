"""Diagnostic collector for edge nodes.

Gathers system info, sanitized config, recent logs, and scanner status
into a JSON bundle for upload to REM via Gateway.
"""

import os
import platform
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import structlog

from . import __version__

logger = structlog.get_logger(__name__)

# Env var keys that contain secrets (case-insensitive substring match)
_SENSITIVE_PATTERNS = {"TOKEN", "SECRET", "KEY", "PASSWORD", "AUTH", "CREDENTIAL"}


def _redact_env(env: Dict[str, str]) -> Dict[str, str]:
    """Redact sensitive values from env vars."""
    redacted = {}
    for k, v in env.items():
        k_upper = k.upper()
        if any(pat in k_upper for pat in _SENSITIVE_PATTERNS):
            redacted[k] = "***REDACTED***"
        else:
            redacted[k] = v
    return redacted


def _parse_env_file(path: str) -> Dict[str, str]:
    """Parse a .env file into a dict."""
    env = {}
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, _, value = line.partition("=")
                    env[key.strip()] = value.strip()
    except Exception:
        pass
    return env


def _get_system_info() -> Dict[str, Any]:
    """Collect system-level information."""
    info: Dict[str, Any] = {
        "platform": platform.platform(),
        "arch": platform.machine(),
        "python": platform.python_version(),
    }

    # Uptime from /proc/uptime
    try:
        with open("/proc/uptime") as f:
            info["uptime_seconds"] = float(f.read().split()[0])
    except Exception:
        pass

    # Disk usage
    try:
        usage = shutil.disk_usage("/")
        info["disk"] = {
            "total_bytes": usage.total,
            "used_bytes": usage.used,
            "free_bytes": usage.free,
        }
    except Exception:
        pass

    # Memory from /proc/meminfo
    try:
        meminfo = {}
        with open("/proc/meminfo") as f:
            for line in f:
                parts = line.split(":")
                if len(parts) == 2:
                    key = parts[0].strip()
                    val = parts[1].strip().split()[0]
                    meminfo[key] = int(val) * 1024  # kB to bytes
        info["memory"] = {
            "total_bytes": meminfo.get("MemTotal", 0),
            "available_bytes": meminfo.get("MemAvailable", 0),
            "free_bytes": meminfo.get("MemFree", 0),
        }
    except Exception:
        pass

    return info


def _get_recent_logs(max_lines: int = 100) -> str:
    """Get recent log output from the container's stdout."""
    try:
        result = subprocess.run(
            ["tail", "-n", str(max_lines), "/proc/1/fd/1"],
            capture_output=True, text=True, timeout=5,
        )
        return result.stdout[-20000:]  # cap at 20KB
    except Exception:
        pass

    # Fallback: check /data for log files
    try:
        log_files = sorted(Path("/data").glob("*.log"), key=lambda p: p.stat().st_mtime, reverse=True)
        if log_files:
            return log_files[0].read_text()[-20000:]
    except Exception:
        pass

    return ""


def collect(
    node_id: str,
    env_path: str = "/app/.env",
    get_stats: Optional[Callable[[], Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Collect diagnostic information. Returns a dict targeting < 100KB."""
    dump: Dict[str, Any] = {
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "node_id": node_id,
        "version": __version__,
    }

    # System info
    try:
        dump["system"] = _get_system_info()
    except Exception as e:
        dump["system"] = {"error": str(e)}

    # Sanitized env
    try:
        raw_env = _parse_env_file(env_path)
        dump["env"] = _redact_env(raw_env)
    except Exception as e:
        dump["env"] = {"error": str(e)}

    # Recent logs
    try:
        dump["logs"] = _get_recent_logs()
    except Exception as e:
        dump["logs"] = f"error: {e}"

    # Scanner/pipeline stats
    if get_stats:
        try:
            dump["stats"] = get_stats()
        except Exception as e:
            dump["stats"] = {"error": str(e)}

    # USB devices (helps diagnose missing SDR dongles)
    try:
        result = subprocess.run(
            ["lsusb"], capture_output=True, text=True, timeout=5,
        )
        dump["usb_devices"] = result.stdout.strip()
    except Exception:
        dump["usb_devices"] = "lsusb not available"

    return dump
