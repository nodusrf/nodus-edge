"""
Offline ham radio data access.

Provides:
- Repeater lookup by frequency
- Callsign lookup for name/location
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class RepeaterDatabase:
    """Offline repeater database from RepeaterBook."""

    def __init__(self, data_path: Optional[Path] = None):
        if data_path is None:
            data_path = Path(__file__).parent / "repeaters.json"

        self.data_path = data_path
        self._data: Dict = {}
        self._by_frequency: Dict[int, List[Dict]] = {}
        self._loaded = False

    def load(self) -> bool:
        """Load repeater data from JSON file."""
        if self._loaded:
            return True

        if not self.data_path.exists():
            logger.warning(f"Repeater database not found: {self.data_path}")
            return False

        try:
            with open(self.data_path) as f:
                self._data = json.load(f)

            # Index by frequency (Hz)
            for rptr in self._data.get("repeaters", []):
                freq_mhz = float(rptr.get("Frequency", 0))
                freq_hz = int(freq_mhz * 1_000_000)

                if freq_hz not in self._by_frequency:
                    self._by_frequency[freq_hz] = []
                self._by_frequency[freq_hz].append(rptr)

            self._loaded = True
            meta = self._data.get("metadata", {})
            count = len(self._data.get("repeaters", []))
            logger.info(
                f"Loaded {count} repeaters from {self.data_path}"
            )
            if count and meta.get("center_lat"):
                logger.info(
                    f"Repeater DB center: ({meta['center_lat']}, {meta['center_lon']}), "
                    f"radius={meta.get('radius_miles', '?')}mi, "
                    f"source={meta.get('source', 'unknown')}"
                )
            return True
        except Exception as e:
            logger.error(f"Failed to load repeater database: {e}")
            return False

    def lookup_frequency(self, frequency_hz: int, tolerance_hz: int = 2500) -> Optional[Dict]:
        """
        Look up repeater by frequency.

        Args:
            frequency_hz: Frequency in Hz
            tolerance_hz: Tolerance for matching (default 2.5kHz)

        Returns:
            Repeater info dict or None
        """
        if not self._loaded:
            self.load()

        # Direct match
        if frequency_hz in self._by_frequency:
            return self._by_frequency[frequency_hz][0]

        # Tolerance match
        for freq, repeaters in self._by_frequency.items():
            if abs(freq - frequency_hz) <= tolerance_hz:
                return repeaters[0]

        return None

    def get_all_frequencies(self) -> List[int]:
        """Get all repeater frequencies in Hz."""
        if not self._loaded:
            self.load()
        return list(self._by_frequency.keys())

    def get_2m_repeaters(self) -> List[Dict]:
        """Get all 2m (144-148 MHz) repeaters."""
        if not self._loaded:
            self.load()
        return [
            rptr for rptr in self._data.get("repeaters", [])
            if 144 <= float(rptr.get("Frequency", 0)) <= 148
        ]

    def get_70cm_repeaters(self) -> List[Dict]:
        """Get all 70cm (420-450 MHz) repeaters."""
        if not self._loaded:
            self.load()
        return [
            rptr for rptr in self._data.get("repeaters", [])
            if 420 <= float(rptr.get("Frequency", 0)) <= 450
        ]

    @property
    def metadata(self) -> Dict:
        """Get database metadata."""
        if not self._loaded:
            self.load()
        return self._data.get("metadata", {})


class CallsignDatabase:
    """Offline callsign lookup database."""

    def __init__(self, data_path: Optional[Path] = None):
        if data_path is None:
            data_path = Path(__file__).parent / "callsigns.json"

        self.data_path = data_path
        self._data: Dict[str, Dict] = {}
        self._loaded = False

    def load(self) -> bool:
        """Load callsign data from JSON file."""
        if self._loaded:
            return True

        if not self.data_path.exists():
            logger.warning(f"Callsign database not found: {self.data_path}")
            return False

        try:
            with open(self.data_path) as f:
                self._data = json.load(f)

            self._loaded = True
            logger.info(f"Loaded {len(self._data)} callsigns from {self.data_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to load callsign database: {e}")
            return False

    def lookup(self, callsign: str) -> Optional[Dict]:
        """
        Look up callsign information.

        Args:
            callsign: Amateur radio callsign (e.g., "W0ABC")

        Returns:
            Dict with name, city, state, grid, etc. or None
        """
        if not self._loaded:
            self.load()

        # Normalize callsign (uppercase, strip whitespace)
        callsign = callsign.upper().strip()

        return self._data.get(callsign)

    def lookup_many(self, callsigns: List[str]) -> Dict[str, Optional[Dict]]:
        """Look up multiple callsigns."""
        return {cs: self.lookup(cs) for cs in callsigns}


# Global instances (lazy-loaded)
_repeater_db: Optional[RepeaterDatabase] = None
_callsign_db: Optional[CallsignDatabase] = None


def get_repeater_db() -> RepeaterDatabase:
    """Get the global repeater database instance."""
    global _repeater_db
    if _repeater_db is None:
        _repeater_db = RepeaterDatabase()
    return _repeater_db


def get_callsign_db() -> CallsignDatabase:
    """Get the global callsign database instance."""
    global _callsign_db
    if _callsign_db is None:
        _callsign_db = CallsignDatabase()
    return _callsign_db
