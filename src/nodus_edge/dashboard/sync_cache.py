"""
Sync cache for repeater and net schedule data.

Reads/writes cached JSON files from /data/cache/ and provides
sync-from-Gateway functionality for the connect-once pattern.
"""

import json
import time
from pathlib import Path
from typing import Dict, Any, List, Optional

import httpx
import structlog

logger = structlog.get_logger(__name__)

DEFAULT_CACHE_DIR = Path("/data/cache")


class SyncCache:
    """
    Manages cached repeater and net schedule data for offline use.

    Cache files:
    - /data/cache/repeaters.json — repeater DB from Gateway
    - /data/cache/nets.json — net schedules from Gateway

    Falls back to bundled repeaters.json if no sync has occurred.
    """

    def __init__(
        self,
        cache_dir: Optional[Path] = None,
        gateway_url: Optional[str] = None,
        auth_token: Optional[str] = None,
        bundled_repeaters_path: Optional[Path] = None,
    ):
        self.cache_dir = cache_dir or DEFAULT_CACHE_DIR
        self.gateway_url = gateway_url
        self.auth_token = auth_token
        self.bundled_repeaters_path = bundled_repeaters_path

        self._repeaters: List[Dict[str, Any]] = []
        self._nets: List[Dict[str, Any]] = []
        self._repeaters_synced_at: Optional[float] = None
        self._nets_synced_at: Optional[float] = None

        self._load_cached()

    @property
    def has_repeaters(self) -> bool:
        return len(self._repeaters) > 0

    @property
    def has_nets(self) -> bool:
        return len(self._nets) > 0

    @property
    def can_sync(self) -> bool:
        return bool(self.gateway_url)

    def get_repeaters(self) -> List[Dict[str, Any]]:
        return self._repeaters

    def get_nets(self) -> List[Dict[str, Any]]:
        return self._nets

    def get_repeater_by_frequency(self, freq_hz: int) -> Optional[Dict[str, Any]]:
        """Look up repeater info by frequency (Hz)."""
        for r in self._repeaters:
            # Support both Hz and MHz formats in cache
            r_freq = r.get("frequency_hz") or r.get("Frequency")
            if r_freq is None:
                continue
            # If stored as MHz string like "146.940", convert
            if isinstance(r_freq, str):
                try:
                    r_freq = int(float(r_freq) * 1_000_000)
                except ValueError:
                    continue
            elif isinstance(r_freq, float):
                if r_freq < 1_000_000:
                    r_freq = int(r_freq * 1_000_000)
                else:
                    r_freq = int(r_freq)
            if r_freq == freq_hz:
                return r
        return None

    def get_status(self) -> Dict[str, Any]:
        """Get cache status for the Status tab."""
        status = {
            "repeaters_count": len(self._repeaters),
            "nets_count": len(self._nets),
            "can_sync": self.can_sync,
            "synced": self._repeaters_synced_at is not None,
        }
        if self._repeaters_synced_at:
            status["repeaters_synced_at"] = self._repeaters_synced_at
            status["repeaters_age_hours"] = round(
                (time.time() - self._repeaters_synced_at) / 3600, 1
            )
        if self._nets_synced_at:
            status["nets_synced_at"] = self._nets_synced_at
        return status

    def sync(self) -> Dict[str, Any]:
        """
        Sync repeater and net data from Gateway API.

        Returns dict with sync results.
        """
        if not self.gateway_url:
            return {"error": "No gateway URL configured"}

        results = {"repeaters": False, "nets": False}
        headers = {}
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"

        try:
            with httpx.Client(timeout=15.0) as client:
                # Sync repeaters
                try:
                    url = f"{self.gateway_url.rstrip('/')}/v1/repeaters"
                    resp = client.get(url, headers=headers)
                    if resp.status_code == 200:
                        data = resp.json()
                        repeaters = data if isinstance(data, list) else data.get("repeaters", [])
                        self._repeaters = repeaters
                        self._repeaters_synced_at = time.time()
                        self._save_cache("repeaters.json", repeaters)
                        results["repeaters"] = True
                        results["repeaters_count"] = len(repeaters)
                        logger.info("Synced repeaters", count=len(repeaters))
                    else:
                        results["repeaters_error"] = f"HTTP {resp.status_code}"
                        logger.warning("Repeater sync failed", status=resp.status_code)
                except Exception as e:
                    results["repeaters_error"] = str(e)
                    logger.warning("Repeater sync error", error=str(e))

                # Sync nets
                try:
                    url = f"{self.gateway_url.rstrip('/')}/v1/nets"
                    resp = client.get(url, headers=headers)
                    if resp.status_code == 200:
                        data = resp.json()
                        nets = data if isinstance(data, list) else data.get("nets", [])
                        self._nets = nets
                        self._nets_synced_at = time.time()
                        self._save_cache("nets.json", nets)
                        results["nets"] = True
                        results["nets_count"] = len(nets)
                        logger.info("Synced nets", count=len(nets))
                    else:
                        results["nets_error"] = f"HTTP {resp.status_code}"
                except Exception as e:
                    results["nets_error"] = str(e)
                    logger.warning("Net sync error", error=str(e))

        except Exception as e:
            results["error"] = str(e)
            logger.error("Sync failed", error=str(e))

        return results

    def _load_cached(self) -> None:
        """Load cached data from disk, falling back to bundled data."""
        # Try cached repeaters
        cached_repeaters = self._load_cache("repeaters.json")
        if cached_repeaters is not None:
            self._repeaters = cached_repeaters
            # Get file mtime as sync time
            cache_path = self.cache_dir / "repeaters.json"
            if cache_path.exists():
                self._repeaters_synced_at = cache_path.stat().st_mtime
            logger.info("Loaded cached repeaters", count=len(self._repeaters))
        elif self.bundled_repeaters_path and self.bundled_repeaters_path.exists():
            # Fall back to bundled repeaters
            try:
                data = json.loads(self.bundled_repeaters_path.read_text())
                if isinstance(data, list):
                    self._repeaters = data
                elif isinstance(data, dict):
                    self._repeaters = data.get("repeaters", list(data.values()))
                logger.info("Loaded bundled repeaters", count=len(self._repeaters))
            except Exception as e:
                logger.warning("Failed to load bundled repeaters", error=str(e))

        # Try cached nets
        cached_nets = self._load_cache("nets.json")
        if cached_nets is not None:
            self._nets = cached_nets
            cache_path = self.cache_dir / "nets.json"
            if cache_path.exists():
                self._nets_synced_at = cache_path.stat().st_mtime
            logger.info("Loaded cached nets", count=len(self._nets))

    def _load_cache(self, filename: str) -> Optional[list]:
        """Load a JSON cache file, returns None if not found."""
        path = self.cache_dir / filename
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text())
            return data if isinstance(data, list) else None
        except Exception as e:
            logger.warning("Failed to load cache", file=filename, error=str(e))
            return None

    def _save_cache(self, filename: str, data: list) -> None:
        """Write data to a JSON cache file."""
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        path = self.cache_dir / filename
        try:
            path.write_text(json.dumps(data, indent=2))
            logger.debug("Cache saved", file=filename, items=len(data))
        except Exception as e:
            logger.warning("Failed to save cache", file=filename, error=str(e))
