#!/usr/bin/env python3
# RELEASE: nodusrf/nodus-edge whisper-cpu/whisper_api.py
"""
Whisper Transcription API Service
Optimized for Police/Fire/EMS radio transcription

Features:
- GPU-accelerated transcription using faster-whisper
- REST API for audio file uploads
- Streaming transcription support
- Health checks and status endpoints
"""

import asyncio
import os
import subprocess
import tempfile
import logging
import time
from collections import defaultdict
from pathlib import Path
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, UploadFile, File, HTTPException, Query, BackgroundTasks, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel
import aiofiles
from faster_whisper import WhisperModel

# Configuration
MODEL_SIZE = os.getenv("WHISPER_MODEL", "medium")
DEVICE = os.getenv("WHISPER_DEVICE", "cuda")
COMPUTE_TYPE = os.getenv("WHISPER_COMPUTE_TYPE", "float16")  # float16 for GPU, int8 for CPU
BEAM_SIZE = int(os.getenv("WHISPER_BEAM_SIZE", "5"))
MAX_FILE_SIZE = int(os.getenv("MAX_FILE_SIZE_MB", "100")) * 1024 * 1024  # 100MB default
TEMP_DIR = Path(os.getenv("TEMP_DIR", "/opt/whisper/temp"))

# Protection settings
RATE_LIMIT_PER_MINUTE = int(os.getenv("WHISPER_RATE_LIMIT_PER_MINUTE", "30"))
GPU_TEMP_LIMIT_C = int(os.getenv("WHISPER_GPU_TEMP_LIMIT_C", "85"))
MAX_QUEUE_DEPTH = int(os.getenv("WHISPER_MAX_QUEUE_DEPTH", "3"))

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("whisper-api")

# Global model instance
model: Optional[WhisperModel] = None


# ---------------------------------------------------------------------------
# Protection: GPU temperature guard
# ---------------------------------------------------------------------------

def get_gpu_temp() -> Optional[int]:
    """Get GPU temperature via nvidia-smi. Returns None if unavailable."""
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=temperature.gpu", "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0:
            return int(result.stdout.strip().split("\n")[0])
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Protection: Per-IP sliding window rate limiter
# ---------------------------------------------------------------------------

class RateLimiter:
    """In-memory per-IP sliding window rate limiter."""

    def __init__(self, max_requests: int, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window = window_seconds
        self._requests: dict[str, list[float]] = defaultdict(list)

    def is_allowed(self, client_ip: str) -> bool:
        now = time.monotonic()
        timestamps = self._requests[client_ip]
        # Prune old entries
        cutoff = now - self.window
        self._requests[client_ip] = [t for t in timestamps if t > cutoff]
        if len(self._requests[client_ip]) >= self.max_requests:
            return False
        self._requests[client_ip].append(now)
        return True

    def retry_after(self, client_ip: str) -> int:
        """Seconds until the oldest request in the window expires."""
        timestamps = self._requests.get(client_ip, [])
        if not timestamps:
            return 0
        oldest = min(timestamps)
        return max(1, int(self.window - (time.monotonic() - oldest)) + 1)


rate_limiter = RateLimiter(max_requests=RATE_LIMIT_PER_MINUTE, window_seconds=60)


# ---------------------------------------------------------------------------
# Protection: Queue depth limiter (semaphore)
# ---------------------------------------------------------------------------

transcription_semaphore = asyncio.Semaphore(MAX_QUEUE_DEPTH)


class TranscriptionResult(BaseModel):
    """Response model for transcription results"""
    text: str
    segments: list
    language: str
    language_probability: float
    duration: float
    model: str = ""
    device: str = ""
    compute_type: str = ""


class HealthResponse(BaseModel):
    """Response model for health check"""
    status: str
    model: str
    device: str
    compute_type: str


class TranscriptionOptions(BaseModel):
    """Options for transcription"""
    language: Optional[str] = None
    task: str = "transcribe"  # "transcribe" or "translate"
    beam_size: int = BEAM_SIZE
    word_timestamps: bool = False
    vad_filter: bool = True  # Voice Activity Detection - helpful for radio audio
    vad_parameters: Optional[dict] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load model on startup, cleanup on shutdown"""
    global model
    logger.info(f"Loading Whisper model: {MODEL_SIZE}")
    logger.info(f"Device: {DEVICE}, Compute Type: {COMPUTE_TYPE}")

    try:
        model = WhisperModel(
            MODEL_SIZE,
            device=DEVICE,
            compute_type=COMPUTE_TYPE,
            download_root="/opt/whisper/models"
        )
        logger.info("Model loaded successfully!")
    except Exception as e:
        logger.error(f"Failed to load model: {e}")
        # Fall back to CPU if GPU fails
        logger.info("Attempting CPU fallback...")
        model = WhisperModel(
            MODEL_SIZE,
            device="cpu",
            compute_type="int8",
            download_root="/opt/whisper/models"
        )
        logger.info("Model loaded on CPU")

    yield

    # Cleanup
    logger.info("Shutting down...")
    model = None


app = FastAPI(
    title="Whisper Transcription API",
    description="GPU-accelerated speech-to-text API optimized for radio communications",
    version="1.1.0",
    lifespan=lifespan
)


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    if model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    return HealthResponse(
        status="healthy",
        model=MODEL_SIZE,
        device=DEVICE,
        compute_type=COMPUTE_TYPE
    )


@app.get("/metrics")
async def metrics():
    """Metrics endpoint for infrastructure monitoring."""
    gpu_temp = get_gpu_temp()
    return {
        "gpu_temp_c": gpu_temp,
        "gpu_temp_limit_c": GPU_TEMP_LIMIT_C,
        "rate_limit_per_minute": RATE_LIMIT_PER_MINUTE,
        "max_queue_depth": MAX_QUEUE_DEPTH,
        "queue_available": transcription_semaphore._value,
        "temp_files": len(list(TEMP_DIR.glob("*"))) if TEMP_DIR.exists() else 0,
    }


@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "service": "Whisper Transcription API",
        "model": MODEL_SIZE,
        "endpoints": {
            "health": "/health",
            "transcribe": "/transcribe (POST)",
            "transcribe_stream": "/transcribe/stream (POST)"
        }
    }


async def save_upload_file(upload_file: UploadFile) -> Path:
    """Save uploaded file to temp directory"""
    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    suffix = Path(upload_file.filename).suffix if upload_file.filename else ".wav"
    temp_file = tempfile.NamedTemporaryFile(
        delete=False,
        suffix=suffix,
        dir=TEMP_DIR
    )

    try:
        async with aiofiles.open(temp_file.name, 'wb') as f:
            total_size = 0
            while chunk := await upload_file.read(1024 * 1024):  # 1MB chunks
                total_size += len(chunk)
                if total_size > MAX_FILE_SIZE:
                    os.unlink(temp_file.name)
                    raise HTTPException(
                        status_code=413,
                        detail=f"File too large. Maximum size is {MAX_FILE_SIZE // (1024*1024)}MB"
                    )
                await f.write(chunk)
    except Exception as e:
        if os.path.exists(temp_file.name):
            os.unlink(temp_file.name)
        raise

    return Path(temp_file.name)


def cleanup_file(path: Path):
    """Background task to clean up temp files"""
    try:
        if path.exists():
            path.unlink()
    except Exception as e:
        logger.error(f"Failed to cleanup temp file {path}: {e}")


@app.post("/transcribe", response_model=TranscriptionResult)
async def transcribe(
    request: Request,
    background_tasks: BackgroundTasks,
    file: UploadFile = File(..., description="Audio file to transcribe"),
    language: Optional[str] = Query(None, description="Language code (e.g., 'en')"),
    task: str = Query("transcribe", description="Task: 'transcribe' or 'translate'"),
    beam_size: int = Query(BEAM_SIZE, description="Beam size for decoding"),
    word_timestamps: bool = Query(False, description="Include word-level timestamps"),
    vad_filter: bool = Query(True, description="Enable Voice Activity Detection"),
    initial_prompt: Optional[str] = Query(None, description="Prime decoder with domain vocabulary"),
    temperature: float = Query(0.0, description="Sampling temperature (0.0 = deterministic greedy)"),
    condition_on_previous_text: bool = Query(True, description="Condition on previous segment text (False reduces hallucination propagation)"),
    repetition_penalty: float = Query(1.0, description="Penalty for repeated tokens (1.0 = off, 1.1 = mild)"),
    no_repeat_ngram_size: int = Query(0, description="Prevent exact N-gram repetition (0 = off, 3 = recommended)"),
    hallucination_silence_threshold: Optional[float] = Query(None, description="Skip segments with >N seconds of hallucinated silence (requires word_timestamps)"),
):
    """
    Transcribe an audio file.

    Optimized for radio communications with VAD filtering enabled by default
    to handle silence and noise common in radio transmissions.

    Supported formats: wav, mp3, m4a, flac, ogg, webm, and most audio formats
    supported by ffmpeg.
    """
    if model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    # Protection: Per-IP rate limiting
    client_ip = request.client.host if request.client else "unknown"
    if not rate_limiter.is_allowed(client_ip):
        retry_after = rate_limiter.retry_after(client_ip)
        logger.warning(f"Rate limited: {client_ip} ({RATE_LIMIT_PER_MINUTE}/min exceeded)")
        return JSONResponse(
            status_code=429,
            content={"detail": f"Rate limit exceeded ({RATE_LIMIT_PER_MINUTE} req/min). Retry after {retry_after}s."},
            headers={"Retry-After": str(retry_after)},
        )

    # Protection: GPU temperature guard
    gpu_temp = get_gpu_temp()
    if gpu_temp is not None and gpu_temp >= GPU_TEMP_LIMIT_C:
        logger.warning(f"GPU too hot: {gpu_temp}C >= {GPU_TEMP_LIMIT_C}C, rejecting request")
        return JSONResponse(
            status_code=503,
            content={"detail": f"GPU temperature {gpu_temp}C exceeds limit {GPU_TEMP_LIMIT_C}C. Cooling down."},
            headers={"Retry-After": "30"},
        )

    # Protection: Queue depth limit
    if transcription_semaphore.locked() and transcription_semaphore._value == 0:
        logger.warning(f"Queue full: {MAX_QUEUE_DEPTH} requests already processing")
        return JSONResponse(
            status_code=503,
            content={"detail": f"Server busy ({MAX_QUEUE_DEPTH} requests queued). Try again shortly."},
            headers={"Retry-After": "5"},
        )

    # Save uploaded file
    temp_path = await save_upload_file(file)
    background_tasks.add_task(cleanup_file, temp_path)

    async with transcription_semaphore:
        try:
            logger.info(f"Transcribing file: {file.filename}")

            # VAD parameters optimized for radio communications
            vad_params = {
                "threshold": 0.5,
                "min_speech_duration_ms": 250,
                "min_silence_duration_ms": 100,
                "speech_pad_ms": 200
            } if vad_filter else None

            # Force word_timestamps on when hallucination_silence_threshold is set
            effective_word_timestamps = word_timestamps or hallucination_silence_threshold is not None

            segments, info = model.transcribe(
                str(temp_path),
                language=language,
                task=task,
                beam_size=beam_size,
                word_timestamps=effective_word_timestamps,
                vad_filter=vad_filter,
                vad_parameters=vad_params,
                initial_prompt=initial_prompt,
                temperature=temperature,
                condition_on_previous_text=condition_on_previous_text,
                repetition_penalty=repetition_penalty,
                no_repeat_ngram_size=no_repeat_ngram_size,
                hallucination_silence_threshold=hallucination_silence_threshold,
            )

            # Collect segments
            segment_list = []
            full_text = []

            for segment in segments:
                seg_data = {
                    "id": segment.id,
                    "start": segment.start,
                    "end": segment.end,
                    "text": segment.text.strip(),
                    "avg_logprob": segment.avg_logprob,
                    "no_speech_prob": segment.no_speech_prob,
                    "compression_ratio": segment.compression_ratio,
                }

                if word_timestamps and segment.words:
                    seg_data["words"] = [
                        {
                            "word": word.word,
                            "start": word.start,
                            "end": word.end,
                            "probability": word.probability
                        }
                        for word in segment.words
                    ]

                segment_list.append(seg_data)
                full_text.append(segment.text.strip())

            result = TranscriptionResult(
                text=" ".join(full_text),
                segments=segment_list,
                language=info.language,
                language_probability=info.language_probability,
                duration=info.duration,
                model=MODEL_SIZE,
                device=DEVICE,
                compute_type=COMPUTE_TYPE,
            )

            logger.info(f"Transcription complete. Duration: {info.duration:.2f}s, Language: {info.language}")

            return result

        except Exception as e:
            logger.error(f"Transcription failed: {e}")
            raise HTTPException(status_code=500, detail=f"Transcription failed: {str(e)}")


@app.post("/transcribe/stream")
async def transcribe_stream(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(..., description="Audio file to transcribe"),
    language: Optional[str] = Query(None, description="Language code"),
    vad_filter: bool = Query(True, description="Enable Voice Activity Detection"),
    initial_prompt: Optional[str] = Query(None, description="Prime decoder with domain vocabulary"),
    condition_on_previous_text: bool = Query(True, description="Condition on previous segment text"),
    repetition_penalty: float = Query(1.0, description="Penalty for repeated tokens"),
    no_repeat_ngram_size: int = Query(0, description="Prevent exact N-gram repetition"),
    hallucination_silence_threshold: Optional[float] = Query(None, description="Skip hallucinated silence segments"),
):
    """
    Stream transcription results segment by segment.

    Returns a stream of JSON objects, one per line (NDJSON format).
    Useful for near real-time processing of longer audio files.
    """
    if model is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    temp_path = await save_upload_file(file)
    background_tasks.add_task(cleanup_file, temp_path)

    async def generate():
        try:
            vad_params = {
                "threshold": 0.5,
                "min_speech_duration_ms": 250,
                "min_silence_duration_ms": 100,
                "speech_pad_ms": 200
            } if vad_filter else None

            segments, info = model.transcribe(
                str(temp_path),
                language=language,
                vad_filter=vad_filter,
                vad_parameters=vad_params,
                initial_prompt=initial_prompt,
                word_timestamps=hallucination_silence_threshold is not None,
                condition_on_previous_text=condition_on_previous_text,
                repetition_penalty=repetition_penalty,
                no_repeat_ngram_size=no_repeat_ngram_size,
                hallucination_silence_threshold=hallucination_silence_threshold,
            )

            # Yield metadata first
            import json
            yield json.dumps({
                "type": "metadata",
                "language": info.language,
                "language_probability": info.language_probability,
                "duration": info.duration
            }) + "\n"

            # Stream segments
            for segment in segments:
                yield json.dumps({
                    "type": "segment",
                    "id": segment.id,
                    "start": segment.start,
                    "end": segment.end,
                    "text": segment.text.strip()
                }) + "\n"

            yield json.dumps({"type": "complete"}) + "\n"

        except Exception as e:
            logger.error(f"Streaming transcription failed: {e}")
            yield json.dumps({"type": "error", "message": str(e)}) + "\n"

    return StreamingResponse(
        generate(),
        media_type="application/x-ndjson"
    )


if __name__ == "__main__":
    import uvicorn

    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))

    uvicorn.run(
        "whisper_api:app",
        host=host,
        port=port,
        workers=1,  # Single worker for GPU model
        log_level="info"
    )
