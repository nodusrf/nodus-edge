# RELEASE: nodusrf/nodus-edge Dockerfile
# NodusNet Edge — Docker Image
# Multi-stage build: compile RTLSDR-Airband + Direwolf from source, install Python package
#
# Build:  docker build -t nodus-edge .
# Run:    docker compose up -d

# ---------------------------------------------------------------------------
# Stage 1a: Build RTLSDR-Airband from source (NFM mode)
# ---------------------------------------------------------------------------
FROM python:3.12.3-slim-bookworm AS rtlsdr-builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        pkg-config \
        git \
        librtlsdr-dev \
        libfftw3-dev \
        libconfig++-dev \
        libmp3lame-dev \
        libshout3-dev \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --depth 1 https://github.com/rtl-airband/RTLSDR-Airband /tmp/rtl-airband \
    && cd /tmp/rtl-airband \
    && mkdir build && cd build \
    && cmake -DNFM=1 -DRTLSDR=1 -DPULSEAUDIO=0 -DPLATFORM=generic .. \
    && make -j"$(nproc)"

# ---------------------------------------------------------------------------
# Stage 1b: Build Direwolf from source (APRS software TNC)
# ---------------------------------------------------------------------------
FROM python:3.12.3-slim-bookworm AS direwolf-builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        git \
        libasound2-dev \
        libudev-dev \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --depth 1 https://github.com/wb2osz/direwolf /tmp/direwolf \
    && cd /tmp/direwolf \
    && mkdir build && cd build \
    && cmake -DUNITTEST=0 .. \
    && make -j"$(nproc)"

# ---------------------------------------------------------------------------
# Stage 2: Runtime image
# ---------------------------------------------------------------------------
FROM python:3.12.3-slim-bookworm

LABEL org.opencontainers.image.source=https://github.com/nodusrf/nodus-edge
LABEL org.opencontainers.image.license=GPL-3.0-only

# Runtime libraries for RTL-SDR, audio processing, and RTLSDR-Airband
RUN apt-get update && apt-get install -y --no-install-recommends \
        librtlsdr0 \
        rtl-sdr \
        libfftw3-single3 \
        libconfig++9v5 \
        libmp3lame0 \
        libshout3 \
        sox \
        libsox-fmt-mp3 \
        ffmpeg \
        usbutils \
    && rm -rf /var/lib/apt/lists/*

# Copy compiled RTLSDR-Airband binary and verify all shared libs resolve
COPY --from=rtlsdr-builder /tmp/rtl-airband/build/src/rtl_airband /usr/local/bin/rtl_airband
RUN ldd /usr/local/bin/rtl_airband && rtl_airband --help || true

# Copy compiled Direwolf binary (APRS software TNC)
COPY --from=direwolf-builder /tmp/direwolf/build/src/direwolf /usr/local/bin/direwolf
RUN ldd /usr/local/bin/direwolf && direwolf --help || true

# Version injection (set by CI via --build-arg)
ARG NODUS_VERSION=dev
ENV NODUS_VERSION=${NODUS_VERSION}

# Install Python package from source
WORKDIR /app
COPY src/pyproject.toml /app/
COPY src/nodus_edge/ /app/nodus_edge/
RUN pip install --no-cache-dir .

# Bake national all-band repeater data as the default. Edge nodes can still
# override via docker-compose volume mount (./repeaters.json:/app/nodus_edge/data/repeaters.json:ro)
RUN cp /app/nodus_edge/data/repeaters_us.json /app/nodus_edge/data/repeaters.json

# Default directories
RUN mkdir -p /data/output /data/fm_capture

# Image digest — set at runtime by the updater or compose env.
# Required for REM compliance check-in. Empty = REM rejects.
ENV NODUS_IMAGE_DIGEST=""

ENV NODUS_EDGE_MODE=fm \
    NODUS_EDGE_OUTPUT_DIR=/data/output \
    NODUS_EDGE_FM_CAPTURE_DIR=/data/fm_capture \
    NODUS_EDGE_FM_SCANNER_BACKEND=airband \
    NODUS_EDGE_WHISPER_VAD_FILTER=false \
    NODUS_EDGE_FM_HALLUCINATION_FILTER_ENABLED=true \
    NODUS_EDGE_FM_MORSE_DETECTION_ENABLED=true \
    NODUS_EDGE_FM_EXTRACT_CALLSIGNS=true \
    NODUS_EDGE_LOG_LEVEL=INFO

ENTRYPOINT ["python", "-m", "nodus_edge"]
