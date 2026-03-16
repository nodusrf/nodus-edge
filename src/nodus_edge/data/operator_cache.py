"""
Local cache of known operators per frequency.

Fed by Synapse's periodic operator sync (Cortex -> Synapse -> Edge).
Used to build Whisper initial_prompt for improved callsign recognition.
"""

import json
import time
from pathlib import Path
from typing import Dict, List, Optional

import structlog

logger = structlog.get_logger(__name__)


class OperatorCache:
    """
    File-backed cache of known operators grouped by frequency.

    Format: {"146940000": ["KD0FJR", "WA0JYD"], "146520000": ["W1AW"]}

    Reloads from disk when file mtime changes (no polling overhead).
    Graceful degradation: returns empty list if file missing or corrupt.
    """

    def __init__(self, cache_dir: str, filename: str = "known_operators.json"):
        self._cache_path = Path(cache_dir) / filename
        self._data: Dict[str, List[str]] = {}
        self._last_mtime: float = 0.0
        self._load()

    def _load(self) -> None:
        """Load cache from disk if file exists."""
        if not self._cache_path.exists():
            logger.debug("operator_cache_file_not_found", path=str(self._cache_path))
            return

        try:
            mtime = self._cache_path.stat().st_mtime
            if mtime == self._last_mtime:
                return  # No change

            raw = self._cache_path.read_text(encoding="utf-8")
            data = json.loads(raw)
            if not isinstance(data, dict):
                logger.warning("operator_cache_invalid_format", path=str(self._cache_path))
                return

            self._data = data
            self._last_mtime = mtime
            total = sum(len(v) for v in self._data.values())
            logger.info(
                "operator_cache_loaded",
                frequencies=len(self._data),
                total_operators=total,
            )
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("operator_cache_load_error", error=str(exc))

    def get_operators(self, frequency_hz: int) -> List[str]:
        """
        Get known operators for a frequency.

        Checks file mtime and reloads if changed.
        Returns empty list if no data available.
        """
        self._maybe_reload()
        return self._data.get(str(frequency_hz), [])

    def update(self, operators_by_frequency: Dict[str, List[str]]) -> None:
        """
        Write new operator data to cache file.

        Called when Synapse pushes an update.
        """
        try:
            self._cache_path.parent.mkdir(parents=True, exist_ok=True)
            self._cache_path.write_text(
                json.dumps(operators_by_frequency, indent=2),
                encoding="utf-8",
            )
            self._data = operators_by_frequency
            self._last_mtime = self._cache_path.stat().st_mtime
            total = sum(len(v) for v in operators_by_frequency.values())
            logger.info(
                "operator_cache_updated",
                frequencies=len(operators_by_frequency),
                total_operators=total,
            )
        except OSError as exc:
            logger.error("operator_cache_write_error", error=str(exc))

    def _maybe_reload(self) -> None:
        """Reload from disk if file has been modified externally."""
        try:
            if self._cache_path.exists():
                mtime = self._cache_path.stat().st_mtime
                if mtime != self._last_mtime:
                    self._load()
        except OSError:
            pass
