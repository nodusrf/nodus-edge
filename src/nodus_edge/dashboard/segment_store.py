"""
In-memory segment store for the edge dashboard.

Ring buffer for recent segments, frequency activity tracker,
and hourly traffic aggregator. Also manages SSE broadcast
to connected browser clients.
"""

import asyncio
import json
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from threading import Lock
from typing import Dict, Any, List, Optional, Set

import structlog

logger = structlog.get_logger(__name__)

# Max signal samples per channel (120 = 30 min at one sample per ~15s)
_SIGNAL_HISTORY_MAXLEN = 120
_SIGNAL_HISTORY_WINDOW_SEC = 1800  # 30 minutes


class SegmentStore:
    """
    Thread-safe in-memory store for dashboard data.

    - Ring buffer of recent segments (capped at max_segments)
    - Per-frequency activity tracking (last heard, segment count)
    - Hourly traffic buckets (last 24h)
    - Per-channel signal strength history (spectrum/waterfall)
    - SSE subscriber management for live broadcast
    """

    def __init__(self, max_segments: int = 500):
        self._max_segments = max_segments
        self._segments: deque = deque(maxlen=max_segments)
        self._lock = Lock()

        # Frequency stats: freq_hz -> {last_heard, count, callsigns}
        self._freq_stats: Dict[int, Dict[str, Any]] = defaultdict(
            lambda: {"last_heard": None, "count": 0, "callsigns": set()}
        )

        # Hourly traffic buckets: hour_key -> count
        # hour_key = "YYYY-MM-DD-HH"
        self._hourly_traffic: Dict[str, int] = defaultdict(int)

        # Daily stats (reset at midnight)
        self._today_key: str = ""
        self._today_segments: int = 0
        self._today_callsigns: Set[str] = set()

        # Signal history: freq_hz -> deque of (timestamp, db) tuples
        self._signal_history: Dict[int, deque] = {}
        # Ordered list of all channel frequencies (Hz)
        self._all_channels: List[int] = []

        # Recommended squelch EMA state
        self._recommended_ema: Optional[float] = None
        self._last_reported: Optional[float] = None

        # SSE subscribers: set of asyncio.Queue instances
        self._sse_queues: Set[asyncio.Queue] = set()
        self._sse_lock = Lock()

    def init_channels(self, frequencies: List[int]) -> None:
        """
        Pre-seed the ordered channel list for spectrum display.

        Args:
            frequencies: List of channel frequencies in Hz, sorted.
        """
        with self._lock:
            self._all_channels = sorted(frequencies)
            for freq_hz in self._all_channels:
                if freq_hz not in self._signal_history:
                    self._signal_history[freq_hz] = deque(maxlen=_SIGNAL_HISTORY_MAXLEN)
        logger.info("Spectrum channels initialized", count=len(self._all_channels))

    def add_segment(self, segment_data: Dict[str, Any]) -> None:
        """
        Add a segment to the store and broadcast to SSE clients.

        Args:
            segment_data: Serialized FMTranscriptSegmentV1 dict
        """
        now = datetime.now(timezone.utc)
        today_key = now.strftime("%Y-%m-%d")

        with self._lock:
            self._segments.appendleft(segment_data)

            # Update frequency stats
            freq_hz = segment_data.get("rf_channel", {}).get("frequency_hz", 0)
            if freq_hz:
                stats = self._freq_stats[freq_hz]
                stats["last_heard"] = now.isoformat()
                stats["count"] += 1
                for cs in segment_data.get("detected_callsigns", []):
                    stats["callsigns"].add(cs)

                # Track signal strength for spectrum
                signal_db = segment_data.get("rf_channel", {}).get("signal_strength_db")
                if signal_db is not None:
                    if freq_hz not in self._signal_history:
                        self._signal_history[freq_hz] = deque(maxlen=_SIGNAL_HISTORY_MAXLEN)
                    self._signal_history[freq_hz].append((now.timestamp(), signal_db))

            # Update hourly traffic
            hour_key = now.strftime("%Y-%m-%d-%H")
            self._hourly_traffic[hour_key] += 1
            self._prune_hourly_traffic(now)

            # Update daily stats
            if today_key != self._today_key:
                self._today_key = today_key
                self._today_segments = 0
                self._today_callsigns = set()
            self._today_segments += 1
            for cs in segment_data.get("detected_callsigns", []):
                self._today_callsigns.add(cs)

        # Broadcast to SSE clients (non-blocking)
        self._broadcast_sse(segment_data)

    def get_segments(self, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        """Get recent segments from the ring buffer."""
        with self._lock:
            segments = list(self._segments)
        return segments[offset:offset + limit]

    def get_frequency_stats(self) -> Dict[str, Any]:
        """Get per-frequency activity stats."""
        cutoff = time.time() - _SIGNAL_HISTORY_WINDOW_SEC
        with self._lock:
            result = {}
            for freq_hz, stats in self._freq_stats.items():
                entry = {
                    "frequency_hz": freq_hz,
                    "frequency_mhz": freq_hz / 1_000_000,
                    "last_heard": stats["last_heard"],
                    "count": stats["count"],
                    "callsigns": sorted(stats["callsigns"]),
                }
                # Include signal strength if available
                history = self._signal_history.get(freq_hz)
                if history:
                    recent = [db for ts, db in history if ts >= cutoff]
                    if recent:
                        entry["avg_signal_db"] = round(sum(recent) / len(recent), 1)
                result[str(freq_hz)] = entry
            return result

    def get_traffic_stats(self) -> Dict[str, Any]:
        """Get traffic overview stats."""
        now = datetime.now(timezone.utc)

        with self._lock:
            # Hourly buckets for last 24h
            hourly = {}
            for i in range(24):
                hour = now.replace(minute=0, second=0, microsecond=0)
                from datetime import timedelta
                hour = hour - timedelta(hours=i)
                key = hour.strftime("%Y-%m-%d-%H")
                hourly[hour.strftime("%H:00")] = self._hourly_traffic.get(key, 0)

            # Top frequencies
            sorted_freqs = sorted(
                self._freq_stats.items(),
                key=lambda x: x[1]["count"],
                reverse=True,
            )
            top_freqs = [
                {
                    "frequency_hz": freq,
                    "frequency_mhz": freq / 1_000_000,
                    "count": stats["count"],
                }
                for freq, stats in sorted_freqs[:5]
            ]

            return {
                "hourly": hourly,
                "top_frequencies": top_freqs,
                "today_segments": self._today_segments,
                "today_callsigns": sorted(self._today_callsigns),
                "today_unique_callsigns": len(self._today_callsigns),
                "total_stored": len(self._segments),
            }

    def get_spectrum_events(self, freq_hz: int, from_ts: float, to_ts: float) -> List[Dict[str, Any]]:
        """Return segments matching a frequency and time range."""
        results = []
        with self._lock:
            for seg in self._segments:
                seg_freq = seg.get("rf_channel", {}).get("frequency_hz", 0)
                if seg_freq != freq_hz:
                    continue
                ts_str = seg.get("timestamp")
                if not ts_str:
                    continue
                try:
                    seg_ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00")).timestamp()
                except (ValueError, AttributeError):
                    continue
                if from_ts <= seg_ts <= to_ts:
                    results.append(seg)
        return results

    def get_spectrum_data(self) -> List[Dict[str, Any]]:
        """
        Return all channels with signal history from the last 30 minutes.

        Returns list of dicts with frequency_hz, frequency_mhz, and signals
        (list of {timestamp, db} dicts).
        """
        cutoff = time.time() - _SIGNAL_HISTORY_WINDOW_SEC
        with self._lock:
            channels = self._all_channels if self._all_channels else sorted(self._signal_history.keys())
            result = []
            for freq_hz in channels:
                signals = []
                history = self._signal_history.get(freq_hz)
                if history:
                    signals = [
                        {"timestamp": ts, "db": db}
                        for ts, db in history
                        if ts >= cutoff
                    ]
                result.append({
                    "frequency_hz": freq_hz,
                    "frequency_mhz": freq_hz / 1_000_000,
                    "signals": signals,
                })
            return result

    def get_recommended_squelch_db(self) -> Optional[float]:
        """Compute a recommended squelch threshold in raw dB.

        Algorithm:
        - Collect all signal_db values from the last 30 minutes
        - Require at least 10 readings
        - 15th percentile = noise-signal boundary estimate
        - EMA (alpha=0.1) to smooth jumps
        - Dead band (1.5 dB) to prevent oscillating recommendations
        """
        cutoff = time.time() - _SIGNAL_HISTORY_WINDOW_SEC
        all_db: List[float] = []

        with self._lock:
            for freq_hz, history in self._signal_history.items():
                for ts, db in history:
                    if ts >= cutoff:
                        all_db.append(db)

        if len(all_db) < 10:
            return self._last_reported

        all_db.sort()
        idx = max(0, int(len(all_db) * 0.15) - 1)
        p15 = all_db[idx]

        if self._recommended_ema is None:
            self._recommended_ema = p15
        else:
            self._recommended_ema = 0.1 * p15 + 0.9 * self._recommended_ema

        if self._last_reported is None or abs(self._recommended_ema - self._last_reported) > 1.5:
            self._last_reported = round(self._recommended_ema, 1)

        return self._last_reported

    def get_avg_signal_db(self, freq_hz: int) -> Optional[float]:
        """Get average signal strength for a frequency over the recent window.

        Returns None if no signal data available.
        """
        cutoff = time.time() - _SIGNAL_HISTORY_WINDOW_SEC
        with self._lock:
            history = self._signal_history.get(freq_hz)
            if not history:
                return None
            values = [db for ts, db in history if ts >= cutoff]
        if not values:
            return None
        return round(sum(values) / len(values), 1)

    def subscribe_sse(self) -> asyncio.Queue:
        """Register a new SSE client. Returns queue to read events from."""
        queue: asyncio.Queue = asyncio.Queue(maxsize=64)
        with self._sse_lock:
            self._sse_queues.add(queue)
        logger.debug("SSE client subscribed", total=len(self._sse_queues))
        return queue

    def unsubscribe_sse(self, queue: asyncio.Queue) -> None:
        """Remove an SSE client."""
        with self._sse_lock:
            self._sse_queues.discard(queue)
        logger.debug("SSE client unsubscribed", total=len(self._sse_queues))

    def _broadcast_sse(self, segment_data: Dict[str, Any]) -> None:
        """Push segment to all SSE subscribers (non-blocking)."""
        with self._sse_lock:
            dead = []
            for queue in self._sse_queues:
                try:
                    queue.put_nowait(segment_data)
                except asyncio.QueueFull:
                    dead.append(queue)
            for q in dead:
                self._sse_queues.discard(q)

    def broadcast_notification(self, notif: Dict[str, Any]) -> None:
        """Push a notification event to all SSE subscribers."""
        event = {"__type": "notification", **notif}
        self._broadcast_sse(event)

    def _prune_hourly_traffic(self, now: datetime) -> None:
        """Remove hourly buckets older than 25 hours."""
        from datetime import timedelta
        cutoff = now - timedelta(hours=25)
        cutoff_key = cutoff.strftime("%Y-%m-%d-%H")
        stale = [k for k in self._hourly_traffic if k < cutoff_key]
        for k in stale:
            del self._hourly_traffic[k]
