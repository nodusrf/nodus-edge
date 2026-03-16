# RELEASE: nodusrf/nodus-edge whisper-cpu/Dockerfile
# Nodus Whisper CPU — Edge Docker Image
# Runs the same Whisper API as the GPU server, but configured for CPU inference.
#
# Default model: base (fast on CPU). Override via WHISPER_MODEL env var.
# For better accuracy at ~3x slower speed, use WHISPER_MODEL=small.
#
# Build:  docker build -t nodus-whisper-cpu -f whisper-cpu/Dockerfile whisper-cpu/

FROM python:3.12.3-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
        fastapi \
        uvicorn \
        aiofiles \
        python-multipart \
        faster-whisper \
        pydantic

WORKDIR /app
COPY whisper_api.py /app/whisper_api.py

RUN mkdir -p /opt/whisper/models /opt/whisper/temp

ENV WHISPER_MODEL=base \
    WHISPER_DEVICE=cpu \
    WHISPER_COMPUTE_TYPE=int8 \
    HOST=0.0.0.0 \
    PORT=8000

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

ENTRYPOINT ["python", "whisper_api.py"]
