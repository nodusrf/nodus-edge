"""
Trunk Recorder output watcher.

Watches the TR recordings directory for new call JSON files,
waits for them to settle, then invokes a callback for processing.

Adapted for Nodus Edge.
"""

import json
import time
from pathlib import Path
from typing import Optional, Set, Callable

import structlog

from .tr_schema import TRCallJSON

logger = structlog.get_logger(__name__)


class TRWatcher:
    """
    Watches Trunk Recorder output directory for new call recordings.

    TR writes a JSON file alongside each WAV file with call metadata.
    We watch for new JSON files, wait for them to settle, then process.
    """

    def __init__(
        self,
        recordings_dir: Path,
        poll_interval: float = 0.5,
        settle_time: float = 2.0,
        on_call: Optional[Callable[[Path, TRCallJSON], None]] = None,
    ):
        self.recordings_dir = recordings_dir
        self.poll_interval = poll_interval
        self.settle_time = settle_time
        self.on_call = on_call

        # Track processed files to avoid reprocessing
        self._processed: Set[str] = set()
        self._pending: dict[str, float] = {}  # path -> first_seen_time

        # Statistics
        self._calls_processed = 0
        self._calls_failed = 0

    def scan_once(self) -> list[tuple[Path, TRCallJSON]]:
        """
        Scan for new call JSON files once.

        Returns list of (json_path, parsed_call) tuples for newly ready calls.
        """
        ready = []
        now = time.time()

        # Find all JSON files in recordings directory (recursively)
        for json_path in self.recordings_dir.rglob("*.json"):
            path_str = str(json_path)

            # Skip already processed
            if path_str in self._processed:
                continue

            # Check if we've seen this file before
            if path_str not in self._pending:
                self._pending[path_str] = now
                logger.debug("New call JSON detected", path=path_str)
                continue

            # Check if file has settled
            first_seen = self._pending[path_str]
            if now - first_seen < self.settle_time:
                continue

            # File is ready - try to process
            try:
                call_data = self._parse_call_json(json_path)
                if call_data:
                    ready.append((json_path, call_data))
                    self._processed.add(path_str)
                    del self._pending[path_str]
            except Exception as e:
                logger.error("Failed to parse call JSON", path=path_str, error=str(e))
                self._calls_failed += 1
                self._processed.add(path_str)
                if path_str in self._pending:
                    del self._pending[path_str]

        return ready

    def _parse_call_json(self, json_path: Path) -> Optional[TRCallJSON]:
        """Parse a TR call JSON file."""
        try:
            with open(json_path, 'r') as f:
                data = json.load(f)
            return TRCallJSON.model_validate(data)
        except json.JSONDecodeError as e:
            logger.warning("Invalid JSON in call file", path=str(json_path), error=str(e))
            return None
        except Exception as e:
            logger.warning("Failed to parse call JSON", path=str(json_path), error=str(e))
            return None

    def get_audio_path(self, json_path: Path) -> Optional[Path]:
        """
        Get the audio file path for a call JSON file.

        TR names files like: <talkgroup>-<timestamp>_<freq>-call_<num>.json
        The WAV file has the same name but .wav extension.
        """
        wav_path = json_path.with_suffix('.wav')
        if wav_path.exists():
            return wav_path

        # Try m4a (if compression is enabled)
        m4a_path = json_path.with_suffix('.m4a')
        if m4a_path.exists():
            return m4a_path

        logger.warning("Audio file not found for call", json_path=str(json_path))
        return None

    def run(self, callback: Optional[Callable[[Path, TRCallJSON], None]] = None):
        """
        Run the watcher loop.

        Args:
            callback: Function to call for each new call (json_path, call_data)
        """
        callback = callback or self.on_call
        if not callback:
            raise ValueError("No callback provided")

        logger.info(
            "Starting TR watcher",
            recordings_dir=str(self.recordings_dir),
            poll_interval=self.poll_interval,
            settle_time=self.settle_time,
        )

        while True:
            try:
                ready_calls = self.scan_once()
                for json_path, call_data in ready_calls:
                    try:
                        callback(json_path, call_data)
                        self._calls_processed += 1
                    except Exception as e:
                        logger.error(
                            "Callback failed for call",
                            path=str(json_path),
                            error=str(e),
                        )
                        self._calls_failed += 1

            except Exception as e:
                logger.error("Watcher scan error", error=str(e))

            time.sleep(self.poll_interval)

    def get_stats(self) -> dict:
        """Get watcher statistics."""
        return {
            "recordings_dir": str(self.recordings_dir),
            "calls_processed": self._calls_processed,
            "calls_failed": self._calls_failed,
            "pending_count": len(self._pending),
            "processed_count": len(self._processed),
        }
