<!-- RELEASE: nodusrf/nodus-edge CONTRIBUTING.md -->
# Contributing to Nodus Edge

Contributions are welcome. This guide covers development setup and PR workflow.

## Development Setup

```bash
git clone https://github.com/nodusrf/nodus-edge.git
cd nodus-edge

# Build locally
docker compose build

# Run with local build
docker compose up -d

# View logs
docker compose logs -f nodus-edge
```

### Python source

The edge application is a Python package in `src/nodus_edge/`. To work on it outside Docker:

```bash
cd src
pip install -e .
python -m nodus_edge --help
```

### Dashboard

The dashboard is static HTML/CSS/JS in `src/nodus_edge/dashboard/static/`. Changes are picked up on container restart (or live if you volume-mount the directory).

## Project Structure

```
nodus-edge/
├── Dockerfile              # Multi-stage build (RTLSDR-Airband + Direwolf + Python)
├── docker-compose.yml      # Edge stack: nodus-edge + whisper-cpu
├── .env.example            # Configuration template
├── src/
│   ├── pyproject.toml
│   └── nodus_edge/
│       ├── main.py         # Entry point
│       ├── config.py       # Pydantic settings
│       ├── schema.py       # Segment output schema
│       ├── fm_pipeline.py  # FM transcription pipeline
│       ├── ingestion/      # Scanner backends (airband, rtl_fm, P25, APRS)
│       ├── transcription/  # Whisper client
│       ├── forwarding/     # Segment delivery (Synapse, file output)
│       ├── dashboard/      # Web dashboard (FastAPI + static)
│       └── data/           # Repeater databases, frequency data
├── whisper-cpu/
│   ├── Dockerfile
│   └── whisper_api.py      # Whisper transcription API (FastAPI)
└── support-sidecar/
    ├── Dockerfile
    └── entrypoint.sh       # SSH + CF tunnel for remote support
```

## Pull Request Workflow

1. Fork the repository
2. Create a feature branch from `main`
3. Make your changes
4. Test locally with `docker compose build && docker compose up -d`
5. Open a PR against `main`

### What makes a good PR

- Focused on a single change
- Includes a clear description of what and why
- Tested locally with a real RTL-SDR dongle if touching scanner code

## Scope

The edge software is open-source under GPLv3. Server-side NodusNet services (scene correlation, alerting, fleet management) are private. PRs should focus on edge functionality: scanning, transcription, dashboard, and local processing.

## License

By contributing, you agree that your contributions will be licensed under the GNU General Public License v3.0.
