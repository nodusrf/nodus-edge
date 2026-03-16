<!-- RELEASE: nodusrf/nodus-edge ARCHITECTURE.md -->
# Architecture

## Overview

Nodus Edge is a radio monitoring station that runs on commodity hardware. It captures RF signals via an RTL-SDR dongle, transcribes audio to text using Whisper, and outputs structured segments.

## Data Flow

```
RTL-SDR dongle
    │
    ▼
RTLSDR-Airband (multichannel FM scanner)
    │  WAV files per channel
    ▼
FM Pipeline
    ├── Audio normalization (target RMS 0.15)
    ├── Morse detection (CW segments decoded before Whisper)
    ├── Whisper transcription (local CPU or remote GPU)
    ├── Hallucination filtering
    └── Callsign extraction
    │
    ▼
Segment output
    ├── Local JSON files (/data/output/)
    ├── Dashboard live feed (SSE)
    └── Synapse forwarding (optional, NodusNet)
```

## Components

### Scanner Backends

The ingestion layer supports multiple scanner backends:

- **RTLSDR-Airband** (default) -- Multichannel FM scanner. Monitors up to 32 frequencies simultaneously from a single dongle. Outputs WAV files per active channel.
- **rtl_fm** -- Sequential scanner. One frequency at a time. Simpler but less capable.
- **P25** -- Trunked radio decoding via OP25.
- **APRS** -- Packet radio decoding via Direwolf.

### Transcription Pipeline

Audio flows through several stages before becoming a text segment:

1. **Audio normalization** -- Adjusts gain to target RMS, prevents clipping
2. **Morse detection** -- Identifies CW transmissions and decodes them without Whisper
3. **Whisper transcription** -- Sends audio to the local whisper-cpu container (or a remote GPU endpoint)
4. **Hallucination filtering** -- Detects and removes common Whisper hallucinations (repeated phrases, phantom text from silence)
5. **Callsign extraction** -- Identifies amateur radio callsigns in the transcription

### Whisper CPU

A standalone FastAPI service wrapping faster-whisper. Optimized for radio audio:

- Voice Activity Detection (VAD) tuned for radio squelch patterns
- Rate limiting, queue depth limiting, GPU temperature guards
- Streams results segment-by-segment (NDJSON)

### Dashboard

Static HTML/CSS/JS served by the edge's FastAPI server on port 8073. Features:

- Live segment feed with audio playback
- Frequency activity spectrum
- Scanner and system health status
- Settings editor (modifies .env)
- Support request flow (NodusNet only)

## Configuration

All configuration is via environment variables with the `NODUS_EDGE_` prefix, managed through Pydantic settings. See `.env.example` for the full list.

## NodusNet Integration

When connected to NodusNet (`NODUSNET_SERVER` configured), the edge node:

- Checks in with REM (Remote Edge Management) for version policy and compliance tokens
- Forwards verified segments to Synapse for correlation
- Receives remote support diagnostics
- Reports health via periodic heartbeats

All NodusNet features are optional. Without configuration, the edge node runs independently.
