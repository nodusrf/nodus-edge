"""
Whisper transcription client for Nodus Edge.

Refactored from sdrtrunk_log_processor.py to focus on
stateless, edge transcription per Nodus Edge spec.
"""

import math
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any, List

import requests
import structlog

from ..config import settings
from ..schema import Transcription, TranscriptionSegment

logger = structlog.get_logger(__name__)


class WhisperClient:
    """
    Client for the Whisper transcription API.

    Designed for stateless, non-authoritative transcription at the edge.
    Audio remains the source of record; transcription is advisory.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout: Optional[int] = None,
        auth_token: Optional[str] = None,
    ):
        self.base_url = (base_url or settings.whisper_api_url).rstrip('/')
        self.timeout = timeout or settings.whisper_timeout_seconds
        self.auth_token = auth_token or settings.whisper_auth_token
        self.rem_checkin = None  # Set by main.py; used as Bearer fallback
        self._healthy: Optional[bool] = None
        self._last_health_check: Optional[float] = None
        self._health_check_interval = 60  # seconds

    def _auth_headers(self) -> dict:
        """Build auth headers, using compliance token as Bearer fallback."""
        headers: dict[str, str] = {}
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"
        elif self.rem_checkin and self.rem_checkin.compliance_token:
            headers["Authorization"] = f"Bearer {self.rem_checkin.compliance_token}"
        return headers

    def health_check(self, force: bool = False) -> bool:
        """
        Check if the Whisper service is available.

        Caches result for efficiency; use force=True to bypass cache.
        """
        now = time.time()
        if not force and self._last_health_check:
            if now - self._last_health_check < self._health_check_interval:
                return self._healthy or False

        try:
            response = requests.get(
                f"{self.base_url}/health",
                headers=self._auth_headers(),
                timeout=5,
            )
            if response.status_code == 200:
                data = response.json()
                self._healthy = data.get("status") == "healthy"
            else:
                self._healthy = False
        except requests.RequestException as e:
            logger.debug("Whisper health check failed", error=str(e))
            self._healthy = False

        self._last_health_check = now
        return self._healthy or False

    def transcribe(
        self,
        audio_path: Path,
        language: Optional[str] = None,
        vad_filter: Optional[bool] = None,
        word_timestamps: bool = False,
        retries: Optional[int] = None,
        initial_prompt: Optional[str] = None,
        condition_on_previous_text: Optional[bool] = None,
        repetition_penalty: Optional[float] = None,
        no_repeat_ngram_size: Optional[int] = None,
        hallucination_silence_threshold: Optional[float] = None,
    ) -> Optional[Transcription]:
        """
        Transcribe an audio file using the Whisper API.

        Args:
            audio_path: Path to the audio file
            language: Language code (default from settings)
            vad_filter: Enable voice activity detection (default from settings)
            word_timestamps: Include word-level timestamps
            retries: Number of retry attempts on failure
            initial_prompt: Domain vocabulary hint for decoder (e.g. callsigns)
            condition_on_previous_text: Condition on previous segment (False reduces hallucination propagation)
            repetition_penalty: Penalty for repeated tokens (1.0 = off)
            no_repeat_ngram_size: Prevent exact N-gram repetition (0 = off)
            hallucination_silence_threshold: Skip segments with >N seconds of hallucinated silence

        Returns:
            Transcription object or None on failure
        """
        audio_path = Path(audio_path)
        if not audio_path.exists():
            logger.warning("Audio file not found", path=str(audio_path))
            return None

        language = language or settings.whisper_language
        vad_filter = vad_filter if vad_filter is not None else settings.whisper_vad_filter
        retries = retries if retries is not None else settings.max_retries

        # Resolve anti-hallucination params from settings if not explicitly passed
        if condition_on_previous_text is None:
            condition_on_previous_text = settings.whisper_condition_on_previous_text
        if repetition_penalty is None:
            repetition_penalty = settings.whisper_repetition_penalty
        if no_repeat_ngram_size is None:
            no_repeat_ngram_size = settings.whisper_no_repeat_ngram_size
        if hallucination_silence_threshold is None:
            hallucination_silence_threshold = settings.whisper_hallucination_silence_threshold

        url = f"{self.base_url}/transcribe"
        params = {
            "language": language,
            "vad_filter": str(vad_filter).lower(),
            "word_timestamps": str(word_timestamps).lower(),
            "temperature": "0.0",
            "condition_on_previous_text": str(condition_on_previous_text).lower(),
            "repetition_penalty": str(repetition_penalty),
            "no_repeat_ngram_size": str(no_repeat_ngram_size),
        }
        if initial_prompt:
            params["initial_prompt"] = initial_prompt
        if hallucination_silence_threshold is not None:
            params["hallucination_silence_threshold"] = str(hallucination_silence_threshold)

        for attempt in range(retries):
            try:
                with open(audio_path, "rb") as f:
                    files = {"file": (audio_path.name, f, "audio/wav")}
                    response = requests.post(
                        url,
                        files=files,
                        params=params,
                        headers=self._auth_headers(),
                        timeout=self.timeout,
                    )

                if response.status_code == 200:
                    result = response.json()
                    return self._format_transcription(result)

                elif response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", "30"))
                    logger.warning(
                        "Whisper rate limited, backing off",
                        attempt=attempt + 1,
                        retry_after=retry_after,
                    )
                    time.sleep(retry_after)
                    continue

                elif response.status_code == 503:
                    retry_after = response.headers.get("Retry-After")
                    wait_time = int(retry_after) if retry_after else 2 ** attempt
                    logger.debug(
                        "Whisper service busy, retrying",
                        attempt=attempt + 1,
                        wait_seconds=wait_time,
                    )
                    time.sleep(wait_time)
                    continue

                else:
                    try:
                        error_detail = response.json().get("detail", "Unknown error")
                    except Exception:
                        error_detail = response.text[:200]
                    logger.error(
                        "Whisper API error",
                        status_code=response.status_code,
                        detail=error_detail,
                    )
                    return None

            except requests.Timeout:
                logger.warning(
                    "Whisper timeout",
                    attempt=attempt + 1,
                    max_retries=retries,
                    path=str(audio_path),
                )
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                continue

            except requests.RequestException as e:
                logger.error(
                    "Whisper request error",
                    error=str(e),
                    attempt=attempt + 1,
                )
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                continue

        logger.error(
            "Whisper transcription failed after retries",
            path=str(audio_path),
            retries=retries,
        )
        return None

    def _format_transcription(self, result: Dict[str, Any]) -> Transcription:
        """Format Whisper API response into Transcription schema."""
        segments: List[TranscriptionSegment] = []

        for seg in result.get("segments", []):
            segments.append(
                TranscriptionSegment(
                    id=seg.get("id", 0),
                    start=seg.get("start", 0.0),
                    end=seg.get("end", 0.0),
                    text=seg.get("text", "").strip(),
                    confidence=self._logprob_to_confidence(seg.get("avg_logprob")),
                    no_speech_prob=seg.get("no_speech_prob"),
                    compression_ratio=seg.get("compression_ratio"),
                )
            )

        # Calculate overall confidence from segments
        overall_confidence = None
        if segments:
            confidences = [s.confidence for s in segments if s.confidence is not None]
            if confidences:
                overall_confidence = sum(confidences) / len(confidences)

        # Compute aggregate quality signals (worst-case across segments)
        max_no_speech_prob = None
        max_compression_ratio = None
        min_confidence = None
        if segments:
            nsp_vals = [s.no_speech_prob for s in segments if s.no_speech_prob is not None]
            if nsp_vals:
                max_no_speech_prob = max(nsp_vals)
            cr_vals = [s.compression_ratio for s in segments if s.compression_ratio is not None]
            if cr_vals:
                max_compression_ratio = max(cr_vals)
            conf_vals = [s.confidence for s in segments if s.confidence is not None]
            if conf_vals:
                min_confidence = min(conf_vals)

        # Capture raw Whisper output before any processing
        raw_text = result.get("text", "").strip()

        # For now, synthesized text is same as raw (no radio code interpretation yet)
        synthesized_text = raw_text

        # Build model identifier: include device/compute_type if available
        model_name = result.get("model", "unknown")
        device = result.get("device", "")
        compute_type = result.get("compute_type", "")
        if device:
            model_name = f"{model_name}/{device}"
            if compute_type:
                model_name = f"{model_name}/{compute_type}"

        return Transcription(
            engine="whisper",
            model=model_name,
            language=result.get("language", "en"),
            raw_text=raw_text,
            text=synthesized_text,
            confidence=overall_confidence,
            duration_seconds=result.get("duration"),
            segments=segments,
            transcribed_at=datetime.now(timezone.utc),
            max_no_speech_prob=max_no_speech_prob,
            max_compression_ratio=max_compression_ratio,
            min_confidence=min_confidence,
        )

    @staticmethod
    def _logprob_to_confidence(avg_logprob: Optional[float]) -> Optional[float]:
        """
        Convert average log probability to a 0-1 confidence score.

        Log probabilities are negative; closer to 0 = higher confidence.
        Typical range is -1.0 (low confidence) to 0 (high confidence).
        """
        if avg_logprob is None:
            return None
        return min(1.0, max(0.0, math.exp(avg_logprob)))
