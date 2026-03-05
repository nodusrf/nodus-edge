# Nodus Edge FM

Run your own Nodus FM monitoring station with an RTL-SDR dongle and Docker.

## Hardware Requirements

- **RTL-SDR dongle** — any RTL2832U-based dongle (Nooelec SMArt, RTL-SDR Blog V3/V4, generic)
- **Antenna** — 2m band antenna (quarter-wave whip, J-pole, or dipole tuned for 144–148 MHz)
- **Host machine** — Linux (recommended), macOS, or Windows with Docker Desktop
- **CPU** — any modern x86_64 (Whisper CPU runs on 2+ cores)
- **RAM** — 2 GB minimum (4 GB recommended)

## Quick Start (One-Liner)

```bash
bash -c "$(curl -fsSL https://raw.githubusercontent.com/nodusrf/nodus-edge-fm/main/install.sh)"
```

You'll need a **NodusNet account** — [sign up at nodusrf.com/edge](https://nodusrf.com/edge) to get your credentials. The installer will prompt you to sign in before downloading anything.

### Manual Setup

If you prefer to set up manually:

```bash
# 1. Clone this repo
git clone https://github.com/nodusrf/nodus-edge-fm.git
cd nodus-edge-fm

# 2. Sign in with your NodusNet credentials
docker login registry.nodusrf.com

# 3. Run the setup wizard (recommended)
python3 setup.py

# Or configure manually:
# cp .env.example .env
# Edit .env — set your frequencies, API key, metro, and node ID

# 4. Start
docker compose up -d

# 5. Check logs
docker compose logs -f recept-fm
```

## Setup Wizard

The interactive setup wizard auto-detects your metro area, finds nearby 2m repeaters from RepeaterBook, and generates `.env` and `repeaters.json`. No dependencies beyond Python 3.8+ stdlib.

```bash
# Interactive mode (recommended for first-time setup)
python3 setup.py

# Scripted mode (for automation)
python3 setup.py --zip 85001 --callsign KF0ASB --non-interactive

# Preview without writing files
python3 setup.py --dry-run

# Custom search radius
python3 setup.py --radius 75
```

The wizard will:
1. Ask for your NodusNet server connection and API key
2. Resolve your location and metro area from your zip code
3. Collect your callsign (optional)
4. Fetch nearby 2m repeaters from RepeaterBook
5. Detect RTL-SDR devices
6. Configure RF environment (squelch thresholds)
7. Configure Whisper transcription
8. Generate `.env` and `repeaters.json`

## Configuration

Copy `.env.example` to `.env` and edit:

| Variable | Required | Description |
|----------|----------|-------------|
| `RECEPT_FM_CORE_FREQUENCIES` | Yes | Frequencies in Hz, comma-separated |
| `RECEPT_SYNAPSE_ENDPOINT` | Yes | Central Nodus endpoint (`https://api.nodusalert.ai`) |
| `RECEPT_SYNAPSE_AUTH_TOKEN` | Yes | API key from your Nodus admin |
| `RECEPT_NODE_ID` | Yes | Unique name for this node (e.g., `edge-W1ABC`) |
| `RECEPT_METRO` | Yes | Metro area slug (e.g., `phoenix`, `omaha`) — isolates scenes by metro |
| `RECEPT_FM_SCANNER_BACKEND` | No | `airband` (default) or `rtl_fm` |
| `RECEPT_FM_GAIN` | No | RTL-SDR gain, 0–49 (default: 40) |
| `WHISPER_MODEL` | No | `base` (default), `small`, or `medium` |

### Edge Dashboard

A local web dashboard is included at **http://localhost:8073** — it works fully offline and shows live transcripts, frequency stats, and traffic graphs.

### Using a Remote GPU Whisper Server

If you have a GPU server running the Nodus Whisper API, override the Whisper URL:

```bash
RECEPT_WHISPER_API_URL=http://your-gpu-server:8000
```

You can then disable the local Whisper container by scaling it to zero:

```bash
docker compose up -d --scale whisper=0
```

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

# Recept should show scanner startup and frequency list
docker compose logs recept-fm | tail -20

# Test Whisper health endpoint
curl http://localhost:8000/health

# Open the edge dashboard
open http://localhost:8073
```

## Upgrading

```bash
docker compose pull
docker compose up -d
```

## Troubleshooting

**"No RTL-SDR devices found"** — Check USB passthrough. On Linux, verify udev rules and DVB blacklist above. On Docker Desktop (macOS/Windows), USB passthrough requires additional setup.

**Whisper container slow to start** — First run downloads the model (~150 MB for `base`). Subsequent starts use the cached model from the `whisper-models` volume.

**"Cannot reach Synapse"** — Check your `RECEPT_SYNAPSE_ENDPOINT` and `RECEPT_SYNAPSE_AUTH_TOKEN` in `.env`. Verify network access to `api.nodusalert.ai`.

## License

[MIT](LICENSE)
