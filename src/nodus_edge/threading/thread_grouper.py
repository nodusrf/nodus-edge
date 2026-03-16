"""
Edge-thread grouper with SQLite persistence.

Groups segments by frequency and temporal proximity. A thread opens when
a segment arrives with no recent activity on that frequency. Subsequent
segments on the same frequency within the gap threshold append to it.
Silence past the gap threshold closes the thread.

No state machine, no polling beyond a periodic sweep for stale threads.
"""

import json
import sqlite3
import time
from pathlib import Path
from threading import Lock, Thread, Event
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4

import structlog

logger = structlog.get_logger(__name__)

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS threads (
    thread_id       TEXT PRIMARY KEY,
    frequency_hz    INTEGER NOT NULL,
    opened_at       REAL NOT NULL,
    closed_at       REAL,
    segment_count   INTEGER DEFAULT 0,
    last_segment_at REAL NOT NULL,
    keywords_found  TEXT DEFAULT '[]',
    summary         TEXT DEFAULT '',
    alerted         INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_threads_freq_open
    ON threads(frequency_hz, closed_at);

CREATE TABLE IF NOT EXISTS thread_segments (
    segment_id    TEXT PRIMARY KEY,
    thread_id     TEXT NOT NULL REFERENCES threads(thread_id),
    timestamp     REAL NOT NULL,
    frequency_hz  INTEGER NOT NULL,
    text          TEXT DEFAULT '',
    callsigns     TEXT DEFAULT '[]',
    confidence    REAL DEFAULT 0.0
);

CREATE INDEX IF NOT EXISTS idx_thread_segments_thread
    ON thread_segments(thread_id);
"""


class ThreadGrouper:
    """
    Groups FM segments into edge-threads by frequency and temporal proximity.

    Usage:
        grouper = ThreadGrouper(db_path="/data/threads.db", gap_seconds=45)
        grouper.start()
        pipeline.register_segment_callback(grouper.on_segment)
        ...
        grouper.stop()
    """

    def __init__(
        self,
        db_path: str = "/data/threads.db",
        gap_seconds: float = 45.0,
        prune_hours: int = 24,
        sweep_interval: float = 10.0,
    ):
        self._db_path = db_path
        self._gap_seconds = gap_seconds
        self._prune_hours = prune_hours
        self._sweep_interval = sweep_interval

        self._lock = Lock()
        self._conn: Optional[sqlite3.Connection] = None
        self._sweep_thread: Optional[Thread] = None
        self._stop_event = Event()

        # Callbacks for thread lifecycle events
        self._on_thread_close_callbacks: List[Callable] = []
        self._on_thread_open_callbacks: List[Callable] = []

        # Stats
        self._threads_opened = 0
        self._threads_closed = 0
        self._segments_grouped = 0

    def start(self) -> None:
        """Initialize database and start the sweep thread."""
        self._init_db()
        self._stop_event.clear()

        self._sweep_thread = Thread(
            target=self._sweep_loop,
            daemon=True,
            name="thread-sweep",
        )
        self._sweep_thread.start()
        logger.info(
            "Thread grouper started",
            db_path=self._db_path,
            gap_seconds=self._gap_seconds,
        )

    def stop(self) -> None:
        """Stop the sweep thread and close the database."""
        self._stop_event.set()
        if self._sweep_thread and self._sweep_thread.is_alive():
            self._sweep_thread.join(timeout=5)
        if self._conn:
            self._conn.close()
            self._conn = None
        logger.info(
            "Thread grouper stopped",
            threads_opened=self._threads_opened,
            threads_closed=self._threads_closed,
            segments_grouped=self._segments_grouped,
        )

    def on_segment(self, segment_data: Dict[str, Any]) -> None:
        """
        Segment callback. Assigns the segment to an existing thread or opens a new one.

        Expected keys in segment_data:
            - rf_channel.frequency_hz
            - segment_id
            - timestamp (ISO 8601 string)
            - transcription.text (optional)
            - detected_callsigns (optional)
            - confidence (optional)
        """
        rf = segment_data.get("rf_channel", {})
        freq_hz = rf.get("frequency_hz", 0)
        if not freq_hz:
            return

        segment_id = str(segment_data.get("segment_id", uuid4()))
        ts_str = segment_data.get("timestamp", "")
        try:
            from datetime import datetime
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00")).timestamp()
        except (ValueError, AttributeError):
            ts = time.time()

        text = ""
        transcription = segment_data.get("transcription")
        if isinstance(transcription, dict):
            text = transcription.get("text", "")

        callsigns = segment_data.get("detected_callsigns", [])
        confidence = segment_data.get("confidence", 0.0)

        with self._lock:
            self._assign_segment(
                segment_id=segment_id,
                frequency_hz=freq_hz,
                timestamp=ts,
                text=text,
                callsigns=callsigns,
                confidence=confidence,
            )

    def on_thread_close(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """Register a callback for when a thread is closed."""
        self._on_thread_close_callbacks.append(callback)

    def on_thread_open(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        """Register a callback for when a new thread is opened."""
        self._on_thread_open_callbacks.append(callback)

    def get_open_threads(self) -> List[Dict[str, Any]]:
        """Return all currently open threads."""
        with self._lock:
            cursor = self._conn.execute(
                "SELECT thread_id, frequency_hz, opened_at, segment_count, "
                "last_segment_at, keywords_found "
                "FROM threads WHERE closed_at IS NULL "
                "ORDER BY last_segment_at DESC"
            )
            return [
                {
                    "thread_id": row[0],
                    "frequency_hz": row[1],
                    "opened_at": row[2],
                    "segment_count": row[3],
                    "last_segment_at": row[4],
                    "keywords_found": json.loads(row[5]),
                }
                for row in cursor.fetchall()
            ]

    def get_thread_segments(self, thread_id: str) -> List[Dict[str, Any]]:
        """Return all segments for a thread."""
        with self._lock:
            cursor = self._conn.execute(
                "SELECT segment_id, timestamp, frequency_hz, text, callsigns, confidence "
                "FROM thread_segments WHERE thread_id = ? ORDER BY timestamp",
                (thread_id,),
            )
            return [
                {
                    "segment_id": row[0],
                    "timestamp": row[1],
                    "frequency_hz": row[2],
                    "text": row[3],
                    "callsigns": json.loads(row[4]),
                    "confidence": row[5],
                }
                for row in cursor.fetchall()
            ]

    def get_thread_text(self, thread_id: str) -> str:
        """Return concatenated text from all segments in a thread."""
        with self._lock:
            cursor = self._conn.execute(
                "SELECT text FROM thread_segments "
                "WHERE thread_id = ? ORDER BY timestamp",
                (thread_id,),
            )
            return " ".join(row[0] for row in cursor.fetchall() if row[0])

    def update_thread_keywords(self, thread_id: str, keywords: List[str]) -> None:
        """Update the keywords_found list on a thread."""
        with self._lock:
            existing = self._conn.execute(
                "SELECT keywords_found FROM threads WHERE thread_id = ?",
                (thread_id,),
            ).fetchone()
            if existing:
                current = json.loads(existing[0])
                merged = sorted(set(current + keywords))
                self._conn.execute(
                    "UPDATE threads SET keywords_found = ? WHERE thread_id = ?",
                    (json.dumps(merged), thread_id),
                )
                self._conn.commit()

    def mark_alerted(self, thread_id: str) -> None:
        """Mark a thread as having triggered an urgent alert."""
        with self._lock:
            self._conn.execute(
                "UPDATE threads SET alerted = 1 WHERE thread_id = ?",
                (thread_id,),
            )
            self._conn.commit()

    def get_stats(self) -> Dict[str, Any]:
        """Return grouper statistics."""
        open_count = 0
        total_count = 0
        with self._lock:
            if self._conn:
                open_count = self._conn.execute(
                    "SELECT COUNT(*) FROM threads WHERE closed_at IS NULL"
                ).fetchone()[0]
                total_count = self._conn.execute(
                    "SELECT COUNT(*) FROM threads"
                ).fetchone()[0]

        return {
            "open_threads": open_count,
            "total_threads": total_count,
            "threads_opened": self._threads_opened,
            "threads_closed": self._threads_closed,
            "segments_grouped": self._segments_grouped,
            "gap_seconds": self._gap_seconds,
        }

    # -- Internal methods --

    def _init_db(self) -> None:
        """Initialize SQLite database with schema."""
        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        try:
            self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.executescript(_SCHEMA_SQL)
            self._conn.commit()
            logger.info("Thread database initialized", path=self._db_path)
        except sqlite3.DatabaseError:
            logger.warning("Thread database corrupted, recreating", path=self._db_path)
            if self._conn:
                self._conn.close()
            Path(self._db_path).unlink(missing_ok=True)
            self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.executescript(_SCHEMA_SQL)
            self._conn.commit()

    def _assign_segment(
        self,
        segment_id: str,
        frequency_hz: int,
        timestamp: float,
        text: str,
        callsigns: List[str],
        confidence: float,
    ) -> None:
        """Assign a segment to an open thread or create a new one."""
        # Find open thread on this frequency
        row = self._conn.execute(
            "SELECT thread_id, last_segment_at FROM threads "
            "WHERE frequency_hz = ? AND closed_at IS NULL "
            "ORDER BY last_segment_at DESC LIMIT 1",
            (frequency_hz,),
        ).fetchone()

        if row and (timestamp - row[1]) < self._gap_seconds:
            # Append to existing thread
            thread_id = row[0]
            self._conn.execute(
                "UPDATE threads SET segment_count = segment_count + 1, "
                "last_segment_at = ? WHERE thread_id = ?",
                (timestamp, thread_id),
            )
        else:
            # Close any stale open threads on this frequency
            if row:
                self._close_thread(row[0])

            # Open new thread
            thread_id = str(uuid4())
            self._conn.execute(
                "INSERT INTO threads (thread_id, frequency_hz, opened_at, "
                "segment_count, last_segment_at) VALUES (?, ?, ?, 1, ?)",
                (thread_id, frequency_hz, timestamp, timestamp),
            )
            self._threads_opened += 1
            self._conn.commit()

            thread_info = {
                "thread_id": thread_id,
                "frequency_hz": frequency_hz,
                "opened_at": timestamp,
            }
            for cb in self._on_thread_open_callbacks:
                try:
                    cb(thread_info)
                except Exception as e:
                    logger.debug("Thread open callback error", error=str(e))

        # Insert segment
        self._conn.execute(
            "INSERT OR IGNORE INTO thread_segments "
            "(segment_id, thread_id, timestamp, frequency_hz, text, callsigns, confidence) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (segment_id, thread_id, timestamp, frequency_hz, text,
             json.dumps(callsigns), confidence),
        )
        self._conn.commit()
        self._segments_grouped += 1

    def _close_thread(self, thread_id: str) -> None:
        """Close a thread and fire callbacks."""
        row = self._conn.execute(
            "SELECT thread_id, frequency_hz, opened_at, segment_count, "
            "last_segment_at, keywords_found, alerted "
            "FROM threads WHERE thread_id = ?",
            (thread_id,),
        ).fetchone()

        if not row:
            return

        now = time.time()
        self._conn.execute(
            "UPDATE threads SET closed_at = ? WHERE thread_id = ?",
            (now, thread_id),
        )
        self._conn.commit()
        self._threads_closed += 1

        # Gather thread text for callbacks
        text = self.get_thread_text.__wrapped__(self, thread_id) if hasattr(self.get_thread_text, '__wrapped__') else ""
        try:
            cursor = self._conn.execute(
                "SELECT text FROM thread_segments WHERE thread_id = ? ORDER BY timestamp",
                (thread_id,),
            )
            text = " ".join(r[0] for r in cursor.fetchall() if r[0])
        except Exception:
            pass

        thread_info = {
            "thread_id": row[0],
            "frequency_hz": row[1],
            "opened_at": row[2],
            "closed_at": now,
            "segment_count": row[3],
            "last_segment_at": row[4],
            "keywords_found": json.loads(row[5]),
            "alerted": bool(row[6]),
            "text": text,
        }

        for cb in self._on_thread_close_callbacks:
            try:
                cb(thread_info)
            except Exception as e:
                logger.debug("Thread close callback error", error=str(e))

    def _sweep_loop(self) -> None:
        """Periodic sweep to close stale threads and prune old data."""
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=self._sweep_interval)
            if self._stop_event.is_set():
                break

            try:
                with self._lock:
                    self._sweep_stale_threads()
                    self._prune_old_threads()
            except Exception as e:
                logger.warning("Thread sweep error", error=str(e))

    def _sweep_stale_threads(self) -> None:
        """Close threads that have gone silent past the gap threshold."""
        cutoff = time.time() - self._gap_seconds
        cursor = self._conn.execute(
            "SELECT thread_id FROM threads "
            "WHERE closed_at IS NULL AND last_segment_at < ?",
            (cutoff,),
        )
        stale = [row[0] for row in cursor.fetchall()]
        for thread_id in stale:
            self._close_thread(thread_id)

    def _prune_old_threads(self) -> None:
        """Delete threads and segments older than prune_hours."""
        cutoff = time.time() - (self._prune_hours * 3600)
        self._conn.execute(
            "DELETE FROM thread_segments WHERE thread_id IN "
            "(SELECT thread_id FROM threads WHERE closed_at IS NOT NULL AND closed_at < ?)",
            (cutoff,),
        )
        self._conn.execute(
            "DELETE FROM threads WHERE closed_at IS NOT NULL AND closed_at < ?",
            (cutoff,),
        )
        self._conn.commit()
