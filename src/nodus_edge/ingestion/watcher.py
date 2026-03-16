"""
SDRTrunk file watcher for Nodus Edge.

Watches recordings/ and event_logs/ directories for new files
and queues them for processing.
"""

import time
from collections import OrderedDict
from pathlib import Path
from threading import Lock
from typing import Callable, Optional, Set

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileModifiedEvent
import structlog

from ..config import settings

logger = structlog.get_logger(__name__)


class DeduplicationCache:
    """
    Simple LRU cache for deduplication.

    Stateless in the sense that it doesn't persist to disk;
    restarts will reprocess some files, which is acceptable.
    """

    def __init__(self, max_size: int = 10000, ttl_seconds: int = 3600):
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self._cache: OrderedDict[str, float] = OrderedDict()
        self._lock = Lock()

    def add(self, key: str) -> bool:
        """
        Add a key to the cache.

        Returns True if the key was new, False if already present.
        """
        with self._lock:
            now = time.time()
            self._cleanup_expired(now)

            if key in self._cache:
                return False

            self._cache[key] = now
            self._cache.move_to_end(key)

            # Evict oldest if over capacity
            while len(self._cache) > self.max_size:
                self._cache.popitem(last=False)

            return True

    def contains(self, key: str) -> bool:
        """Check if key is in cache and not expired."""
        with self._lock:
            if key not in self._cache:
                return False
            if time.time() - self._cache[key] > self.ttl_seconds:
                del self._cache[key]
                return False
            return True

    def _cleanup_expired(self, now: float) -> None:
        """Remove expired entries."""
        expired = [
            k for k, v in self._cache.items()
            if now - v > self.ttl_seconds
        ]
        for k in expired:
            del self._cache[k]


class RecordingEventHandler(FileSystemEventHandler):
    """Handler for new recording files."""

    def __init__(
        self,
        callback: Callable[[Path], None],
        pattern: str,
        dedup_cache: DeduplicationCache,
    ):
        super().__init__()
        self.callback = callback
        self.pattern = pattern
        self.dedup_cache = dedup_cache

    def on_created(self, event: FileCreatedEvent) -> None:
        if event.is_directory:
            return

        path = Path(event.src_path)
        if not self._matches_pattern(path):
            return

        # Wait briefly for file to be fully written
        time.sleep(0.5)

        if self.dedup_cache.add(str(path)):
            logger.debug("New recording detected", path=path.name)
            self.callback(path)

    def on_modified(self, event: FileModifiedEvent) -> None:
        # Also handle modifications in case file was created before watcher started
        if event.is_directory:
            return

        path = Path(event.src_path)
        if not self._matches_pattern(path):
            return

        if self.dedup_cache.add(str(path)):
            logger.debug("Recording modified", path=path.name)
            self.callback(path)

    def _matches_pattern(self, path: Path) -> bool:
        """Check if path matches the configured pattern."""
        from fnmatch import fnmatch
        return fnmatch(path.name, self.pattern)


class SDRTrunkWatcher:
    """
    Watches SDRTrunk directories for new recordings.

    Monitors:
    - recordings/ for individual call recordings (*_TO_*_FROM_*.wav)
    - Optionally event_logs/ for call event logs
    """

    def __init__(
        self,
        recordings_dir: Optional[Path] = None,
        event_logs_dir: Optional[Path] = None,
        on_new_recording: Optional[Callable[[Path], None]] = None,
        on_new_event_log: Optional[Callable[[Path], None]] = None,
    ):
        self.recordings_dir = recordings_dir or settings.recordings_path
        self.event_logs_dir = event_logs_dir or settings.event_logs_path
        self.on_new_recording = on_new_recording
        self.on_new_event_log = on_new_event_log

        self._dedup_cache = DeduplicationCache(
            max_size=settings.dedup_cache_size,
            ttl_seconds=settings.dedup_ttl_seconds,
        )
        self._observer: Optional[Observer] = None
        self._running = False

    def start(self) -> None:
        """Start watching directories."""
        if self._running:
            return

        self._observer = Observer()

        # Watch recordings directory
        if self.on_new_recording and self.recordings_dir.exists():
            handler = RecordingEventHandler(
                callback=self.on_new_recording,
                pattern=settings.recording_pattern,
                dedup_cache=self._dedup_cache,
            )
            self._observer.schedule(
                handler,
                str(self.recordings_dir),
                recursive=False,
            )
            logger.info(
                "Watching recordings directory",
                path=str(self.recordings_dir),
                pattern=settings.recording_pattern,
            )

        # Watch event logs directory
        if self.on_new_event_log and self.event_logs_dir.exists():
            handler = RecordingEventHandler(
                callback=self.on_new_event_log,
                pattern=settings.event_log_pattern,
                dedup_cache=self._dedup_cache,
            )
            self._observer.schedule(
                handler,
                str(self.event_logs_dir),
                recursive=False,
            )
            logger.info(
                "Watching event logs directory",
                path=str(self.event_logs_dir),
                pattern=settings.event_log_pattern,
            )

        self._observer.start()
        self._running = True
        logger.info("SDRTrunk watcher started")

    def stop(self) -> None:
        """Stop watching directories."""
        if not self._running:
            return

        if self._observer:
            self._observer.stop()
            self._observer.join(timeout=5)
            self._observer = None

        self._running = False
        logger.info("SDRTrunk watcher stopped")

    def scan_existing(self, process_callback: Callable[[Path], None]) -> int:
        """
        Scan for existing files and process them.

        Returns count of files found.
        """
        count = 0

        # Scan recordings
        if self.recordings_dir.exists():
            for path in sorted(
                self.recordings_dir.glob(settings.recording_pattern),
                key=lambda p: p.stat().st_mtime,
            ):
                if self._dedup_cache.add(str(path)):
                    process_callback(path)
                    count += 1

        logger.info("Scanned existing files", count=count)
        return count

    @property
    def is_running(self) -> bool:
        return self._running
