"""
Tiered keyword scanner for edge-thread awareness.

Scans individual segments and closed threads against a YAML watchlist.
Urgent keywords fire immediately on segment arrival (no thread close wait).
Notable and informational keywords are recorded for stats roll-ups.
"""

import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import structlog

logger = structlog.get_logger(__name__)


class KeywordMatch:
    """A keyword match result."""

    __slots__ = ("tier", "label", "pattern_str", "matched_text")

    def __init__(self, tier: str, label: str, pattern_str: str, matched_text: str):
        self.tier = tier
        self.label = label
        self.pattern_str = pattern_str
        self.matched_text = matched_text

    def to_dict(self) -> Dict[str, str]:
        return {
            "tier": self.tier,
            "label": self.label,
            "matched_text": self.matched_text,
        }


class KeywordScanner:
    """
    Scans transcript text against a tiered keyword watchlist.

    Tiers:
        - urgent: fires immediately on segment, triggers APRS alert
        - notable: included in stats roll-ups
        - informational: logged only

    Usage:
        scanner = KeywordScanner.from_yaml("/app/nodus_edge/data/watchlist.yaml")
        matches = scanner.scan_text("mayday mayday hiker injured")
        # [KeywordMatch(tier="urgent", label="emergency", ...)]
    """

    TIERS = ("urgent", "notable", "informational")

    def __init__(self):
        # tier -> list of (compiled_regex, label, pattern_str)
        self._patterns: Dict[str, List[Tuple[re.Pattern, str, str]]] = {
            tier: [] for tier in self.TIERS
        }
        # Callsign watches: callsign -> tier
        self._watch_callsigns: Dict[str, str] = {}
        self._loaded = False

        # Stats
        self._scans = 0
        self._matches_by_tier: Dict[str, int] = {tier: 0 for tier in self.TIERS}

        # Callbacks
        self._on_urgent_callbacks: List[Callable] = []

    @classmethod
    def from_yaml(cls, path: str) -> "KeywordScanner":
        """Load watchlist from a YAML file."""
        scanner = cls()
        scanner.load_yaml(path)
        return scanner

    def load_yaml(self, path: str) -> None:
        """Load or reload the watchlist YAML."""
        yaml_path = Path(path)
        if not yaml_path.exists():
            logger.warning("Watchlist not found, using empty watchlist", path=path)
            self._loaded = True
            return

        try:
            import yaml
        except ImportError:
            logger.warning("PyYAML not installed, cannot load watchlist")
            self._loaded = True
            return

        try:
            with open(yaml_path) as f:
                data = yaml.safe_load(f) or {}
        except Exception as e:
            logger.error("Failed to load watchlist", path=path, error=str(e))
            self._loaded = True
            return

        # Load tiered patterns
        total_patterns = 0
        for tier in self.TIERS:
            self._patterns[tier] = []
            entries = data.get(tier, [])
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                pattern_str = entry.get("pattern", "")
                label = entry.get("label", "unknown")
                if pattern_str:
                    try:
                        compiled = re.compile(pattern_str, re.IGNORECASE)
                        self._patterns[tier].append((compiled, label, pattern_str))
                        total_patterns += 1
                    except re.error as e:
                        logger.warning(
                            "Invalid watchlist regex",
                            pattern=pattern_str,
                            error=str(e),
                        )

        # Load callsign watches
        self._watch_callsigns = {}
        for entry in data.get("watch_callsigns", []):
            if isinstance(entry, dict):
                cs = entry.get("callsign", "").upper()
                tier = entry.get("tier", "notable")
                if cs and tier in self.TIERS:
                    self._watch_callsigns[cs] = tier

        self._loaded = True
        logger.info(
            "Watchlist loaded",
            path=path,
            patterns=total_patterns,
            watch_callsigns=len(self._watch_callsigns),
        )

    def on_urgent(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """Register a callback for urgent keyword matches."""
        self._on_urgent_callbacks.append(callback)

    def scan_text(self, text: str) -> List[KeywordMatch]:
        """Scan text against all tiers. Returns list of matches."""
        if not text or not self._loaded:
            return []

        self._scans += 1
        matches = []

        for tier in self.TIERS:
            for compiled, label, pattern_str in self._patterns[tier]:
                m = compiled.search(text)
                if m:
                    match = KeywordMatch(
                        tier=tier,
                        label=label,
                        pattern_str=pattern_str,
                        matched_text=m.group(0),
                    )
                    matches.append(match)
                    self._matches_by_tier[tier] += 1

        return matches

    def scan_callsigns(self, callsigns: List[str]) -> List[KeywordMatch]:
        """Check callsigns against the watch list."""
        matches = []
        for cs in callsigns:
            cs_upper = cs.upper()
            if cs_upper in self._watch_callsigns:
                tier = self._watch_callsigns[cs_upper]
                match = KeywordMatch(
                    tier=tier,
                    label=f"watched_callsign:{cs_upper}",
                    pattern_str=cs_upper,
                    matched_text=cs_upper,
                )
                matches.append(match)
                self._matches_by_tier[tier] += 1
        return matches

    def scan_segment(
        self,
        segment_data: Dict[str, Any],
        thread_id: Optional[str] = None,
        frequency_hz: Optional[int] = None,
    ) -> List[KeywordMatch]:
        """
        Scan a segment dict for keyword matches.

        Fires urgent callbacks immediately for urgent matches.

        Returns all matches found.
        """
        text = ""
        transcription = segment_data.get("transcription")
        if isinstance(transcription, dict):
            text = transcription.get("text", "")

        callsigns = segment_data.get("detected_callsigns", [])
        rf = segment_data.get("rf_channel", {})
        freq = frequency_hz or rf.get("frequency_hz", 0)

        # Scan text
        matches = self.scan_text(text)

        # Scan callsigns
        matches.extend(self.scan_callsigns(callsigns))

        # Fire urgent callbacks
        urgent_matches = [m for m in matches if m.tier == "urgent"]
        if urgent_matches:
            alert_info = {
                "segment_id": str(segment_data.get("segment_id", "")),
                "thread_id": thread_id,
                "frequency_hz": freq,
                "text": text[:200],
                "matches": [m.to_dict() for m in urgent_matches],
                "callsigns": callsigns,
            }
            logger.warning(
                "Urgent keyword match",
                frequency_hz=freq,
                labels=[m.label for m in urgent_matches],
                text_preview=text[:80],
            )
            for cb in self._on_urgent_callbacks:
                try:
                    cb(alert_info)
                except Exception as e:
                    logger.debug("Urgent callback error", error=str(e))

        if matches:
            logger.info(
                "Keyword matches",
                frequency_hz=freq,
                matches=[(m.tier, m.label) for m in matches],
            )

        return matches

    def scan_thread(self, thread_info: Dict[str, Any]) -> List[KeywordMatch]:
        """
        Scan a closed thread's concatenated text for keyword matches.

        Called on thread close to catch multi-segment patterns.
        """
        text = thread_info.get("text", "")
        if not text:
            return []

        matches = self.scan_text(text)

        if matches:
            logger.info(
                "Thread keyword matches",
                thread_id=thread_info.get("thread_id"),
                frequency_hz=thread_info.get("frequency_hz"),
                segment_count=thread_info.get("segment_count"),
                matches=[(m.tier, m.label) for m in matches],
            )

        return matches

    def get_stats(self) -> Dict[str, Any]:
        """Return scanner statistics."""
        pattern_counts = {tier: len(self._patterns[tier]) for tier in self.TIERS}
        return {
            "loaded": self._loaded,
            "patterns": pattern_counts,
            "watch_callsigns": len(self._watch_callsigns),
            "scans": self._scans,
            "matches": dict(self._matches_by_tier),
        }
