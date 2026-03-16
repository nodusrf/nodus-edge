"""
Transcription audit log for quality signal instrumentation.

Ring buffer that captures every Whisper transcription attempt with full
quality signals. Exposed via health server for calibration and monitoring.
"""

import json
import threading
from collections import deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List


@dataclass
class AuditEntry:
    """Single transcription attempt with full quality signals."""
    timestamp: str
    frequency_hz: Optional[int]
    duration_seconds: Optional[float]
    modality: str
    # Whisper signals
    text: str
    confidence: Optional[float]
    max_no_speech_prob: Optional[float]
    max_compression_ratio: Optional[float]
    min_segment_confidence: Optional[float]
    # Quality score (populated when quality gate is active)
    quality_score: Optional[float] = None
    # Decision
    outcome: str = ""  # "passed", "rejected_quality", "rejected_structural", "filtered_beacon", "filtered_kerchunk", "filtered_error"
    rejection_reason: Optional[str] = None
    # Audio file reference (for playback in dashboard)
    audio_filename: Optional[str] = None
    # Legacy filter comparison (shadow mode)
    legacy_decision: Optional[bool] = None  # True = would have been filtered
    legacy_reason: Optional[str] = None


class TranscriptionAuditLog:
    """Thread-safe ring buffer for transcription audit entries."""

    def __init__(self, max_entries: int = 2000):
        self._entries: deque[AuditEntry] = deque(maxlen=max_entries)
        self._lock = threading.Lock()
        # Counters
        self._total_count = 0
        self._passed_count = 0
        self._rejected_quality_count = 0
        self._rejected_structural_count = 0
        self._beacon_count = 0
        self._kerchunk_count = 0
        self._error_count = 0

    def log(self, entry: AuditEntry) -> None:
        """Add an audit entry."""
        with self._lock:
            self._entries.append(entry)
            self._total_count += 1
            if entry.outcome == "passed":
                self._passed_count += 1
            elif entry.outcome == "rejected_quality":
                self._rejected_quality_count += 1
            elif entry.outcome == "rejected_structural":
                self._rejected_structural_count += 1
            elif entry.outcome == "filtered_beacon":
                self._beacon_count += 1
            elif entry.outcome == "filtered_kerchunk":
                self._kerchunk_count += 1
            elif entry.outcome == "filtered_error":
                self._error_count += 1

    def log_transcription(
        self,
        *,
        modality: str,
        text: str,
        confidence: Optional[float] = None,
        max_no_speech_prob: Optional[float] = None,
        max_compression_ratio: Optional[float] = None,
        min_segment_confidence: Optional[float] = None,
        quality_score: Optional[float] = None,
        outcome: str,
        rejection_reason: Optional[str] = None,
        frequency_hz: Optional[int] = None,
        duration_seconds: Optional[float] = None,
        audio_filename: Optional[str] = None,
        legacy_decision: Optional[bool] = None,
        legacy_reason: Optional[str] = None,
    ) -> None:
        """Convenience method to log a transcription attempt."""
        entry = AuditEntry(
            timestamp=datetime.now(timezone.utc).isoformat(),
            frequency_hz=frequency_hz,
            duration_seconds=duration_seconds,
            modality=modality,
            text=text[:200],  # Truncate for storage
            confidence=confidence,
            max_no_speech_prob=max_no_speech_prob,
            max_compression_ratio=max_compression_ratio,
            min_segment_confidence=min_segment_confidence,
            quality_score=quality_score,
            outcome=outcome,
            rejection_reason=rejection_reason,
            audio_filename=audio_filename,
            legacy_decision=legacy_decision,
            legacy_reason=legacy_reason,
        )
        self.log(entry)

    def get_recent(self, limit: int = 50, outcome: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get recent audit entries as dicts.

        Args:
            limit: Max entries to return.
            outcome: If set, prefix-match filter on outcome field.
                     e.g. "rejected" matches "rejected_quality" and "rejected_structural".
        """
        with self._lock:
            entries = list(self._entries)
        # Return most recent first
        entries.reverse()
        if outcome:
            entries = [e for e in entries if e.outcome.startswith(outcome)]
        return [asdict(e) for e in entries[:limit]]

    def get_metrics(self) -> Dict[str, Any]:
        """Get aggregate metrics from the audit log."""
        with self._lock:
            entries = list(self._entries)
            total = self._total_count
            passed = self._passed_count
            rejected_quality = self._rejected_quality_count
            rejected_structural = self._rejected_structural_count
            beacon = self._beacon_count
            kerchunk = self._kerchunk_count
            error = self._error_count

        # Quality score distribution
        scores = [e.quality_score for e in entries if e.quality_score is not None]
        nsp_vals = [e.max_no_speech_prob for e in entries if e.max_no_speech_prob is not None]
        cr_vals = [e.max_compression_ratio for e in entries if e.max_compression_ratio is not None]
        conf_vals = [e.min_segment_confidence for e in entries if e.min_segment_confidence is not None]

        # Shadow mode disagreement tracking
        shadow_entries = [e for e in entries if e.legacy_decision is not None]
        disagree_count = sum(
            1 for e in shadow_entries
            if (e.outcome == "passed") != (not e.legacy_decision)
        )

        metrics: Dict[str, Any] = {
            "total_transcriptions": total,
            "passed": passed,
            "rejected_quality": rejected_quality,
            "rejected_structural": rejected_structural,
            "filtered_beacon": beacon,
            "filtered_kerchunk": kerchunk,
            "filtered_error": error,
            "buffer_size": len(entries),
        }

        if scores:
            scores_sorted = sorted(scores)
            metrics["quality_score"] = {
                "mean": sum(scores) / len(scores),
                "min": scores_sorted[0],
                "max": scores_sorted[-1],
                "median": scores_sorted[len(scores_sorted) // 2],
                "p10": scores_sorted[int(len(scores_sorted) * 0.1)],
                "p90": scores_sorted[int(len(scores_sorted) * 0.9)],
                "count": len(scores),
            }

        if nsp_vals:
            metrics["no_speech_prob"] = {
                "mean": sum(nsp_vals) / len(nsp_vals),
                "max": max(nsp_vals),
                "count": len(nsp_vals),
            }

        if cr_vals:
            metrics["compression_ratio"] = {
                "mean": sum(cr_vals) / len(cr_vals),
                "max": max(cr_vals),
                "count": len(cr_vals),
            }

        if conf_vals:
            metrics["min_confidence"] = {
                "mean": sum(conf_vals) / len(conf_vals),
                "min": min(conf_vals),
                "count": len(conf_vals),
            }

        if shadow_entries:
            metrics["shadow_mode"] = {
                "compared": len(shadow_entries),
                "disagreements": disagree_count,
                "agreement_rate": 1.0 - (disagree_count / len(shadow_entries)) if shadow_entries else 1.0,
            }

        return metrics


# Global singleton
audit_log = TranscriptionAuditLog()
