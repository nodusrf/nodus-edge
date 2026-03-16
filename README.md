<!-- RELEASE: nodusrf/nodus-edge README.md -->
# Nodus Edge

Open-source radio monitoring station. Captures FM, P25, HF, and APRS traffic using an RTL-SDR dongle, transcribes audio with Whisper, and outputs structured segments.

Runs standalone or as part of a [NodusNet](https://nodusrf.com) fleet for centralized incident awareness.

## License

This project is licensed under the [GNU General Public License v3.0](LICENSE).

## Hardware Requirements

- **RTL-SDR dongle** -- any RTL2832U-based dongle (Nooelec SMArt, RTL-SDR Blog V3/V4, generic)
- **Antenna** -- 2m band antenna (quarter-wave whip, J-pole, or dipole tuned for 144-148 MHz)
- **Host machine** -- Linux (recommended), macOS, or Windows with Docker Desktop
- **CPU** -- any modern x86_64 or arm64 (Whisper CPU runs on 2+ cores)
- **RAM** -- 2 GB minimum (4 GB recommended)

## Quick Start

### One-line install

```bash
curl -fsSL https://raw.githubusercontent.com/nodusrf/nodus-edge/main/install.sh | bash
```

The install script downloads Docker Compose files, runs the setup wizard, and starts the stack.

### Manual setup

```bash
git clone https://github.com/nodusrf/nodus-edge.git
cd nodus-edge

# Configure
cp .env.example .env
# Edit .env -- set your frequencies, node ID, and optionally NodusNet server

# Start
docker compose up -d

# Check logs
docker compose logs -f nodus-edge
```

### Building from source

```bash
git clone https://github.com/nodusrf/nodus-edge.git
cd nodus-edge
docker compose build
docker compose up -d
```

## Configuration

Copy `.env.example` to `.env` and edit:

| Variable | Required | Description |
|----------|----------|-------------|
| `NODUS_EDGE_FM_CORE_FREQUENCIES` | Yes | Frequencies in Hz, comma-separated |
| `NODUS_EDGE_NODE_ID` | Yes | Unique name for this node (e.g., `edge-W1ABC`) |
| `NODUS_EDGE_FM_SCANNER_BACKEND` | No | `airband` (default) or `rtl_fm` |
| `NODUS_EDGE_FM_GAIN` | No | RTL-SDR gain, 0-49 (default: 40) |
| `WHISPER_MODEL` | No | `base` (default), `small`, or `medium` |

### NodusNet integration (optional)

To connect to a NodusNet fleet for centralized monitoring:

| Variable | Description |
|----------|-------------|
| `NODUSNET_SERVER` | NodusNet server URL |
| `NODUSNET_TOKEN` | Authentication token from your NodusNet admin |

Without these, the edge node runs in standalone mode. Scanning, transcription, and the local dashboard all work without NodusNet.

### Using a remote GPU Whisper server

If you have a GPU server running the Whisper API, override the Whisper URL:

```bash
NODUS_EDGE_WHISPER_API_URL=http://your-gpu-server:8000
```

You can then disable the local Whisper container:

```bash
docker compose up -d --scale whisper=0
```

## Dashboard

The edge dashboard is available at `http://localhost:8073`. It shows:

- Live segment feed with transcriptions
- Scanner status and frequency activity
- Audio playback
- System health

## USB Permissions (Linux)

RTL-SDR dongles need USB access. Create a udev rule:

```bash
sudo tee /etc/udev/rules.d/20-rtlsdr.rules << 'EOF'
SUBSYSTEM=="usb", ATTRS{idVendor}=="0bda", ATTRS{idProduct}=="2838", MODE="0666"
SUBSYSTEM=="usb", ATTRS{idVendor}=="0bda", ATTRS{idProduct}=="2832", MODE="0666"
EOF
sudo udevadm control --reload-rules
sudo udevadm trigger
```

Also blacklist the DVB kernel modules that claim RTL-SDR devices:

```bash
sudo tee /etc/modprobe.d/blacklist-rtlsdr-dvb.conf << 'EOF'
blacklist dvb_usb_rtl28xxu
blacklist rtl2832
blacklist rtl2830
EOF
sudo modprobe -r dvb_usb_rtl28xxu rtl2832 rtl2830 2>/dev/null
```

Unplug and replug the dongle after applying these changes.

## Verifying It Works

```bash
# Check container status
docker compose ps

# Whisper should show "healthy"
docker compose logs whisper | tail -5

# Edge should show scanner startup and frequency list
docker compose logs nodus-edge | tail -20

# Test Whisper health endpoint
curl http://localhost:8000/health

# Open the dashboard
open http://localhost:8073
```

## Upgrading

```bash
docker compose pull
docker compose up -d
```

## Architecture

The edge stack has two containers:

- **nodus-edge** -- Radio capture, transcription pipeline, and web dashboard. Modes: FM (RTLSDR-Airband multichannel scanner), P25, HF, APRS.
- **whisper-cpu** -- Local Whisper transcription API (faster-whisper). Downloads and caches the model on first run.

An optional **support-sidecar** container (dormant by default, Docker Compose profile `support`) provides remote diagnostic access for NodusNet fleet operators.

## Troubleshooting

**"No RTL-SDR devices found"** -- Check USB passthrough. On Linux, verify udev rules and DVB blacklist above. On Docker Desktop (macOS/Windows), USB passthrough requires additional setup.

**Whisper container slow to start** -- First run downloads the model (~150 MB for `base`). Subsequent starts use the cached model from the `whisper-models` volume.

**No transcriptions appearing** -- Check that `NODUS_EDGE_FM_CORE_FREQUENCIES` has active frequencies in your area. Use a frequency scanner app or local repeater directory to find active frequencies.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and guidelines.
