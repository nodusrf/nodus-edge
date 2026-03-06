# Nodus Recept FM — Edge Node

Run your own Nodus FM monitoring station with an RTL-SDR dongle and Docker.

## Hardware Requirements

- **RTL-SDR dongle** — any RTL2832U-based dongle (Nooelec SMArt, RTL-SDR Blog V3/V4, generic)
- **Antenna** — 2m band antenna (quarter-wave whip, J-pole, or dipole tuned for 144–148 MHz)
- **Host machine** — Linux (recommended), macOS, or Windows with Docker Desktop
- **CPU** — any modern x86_64 (Whisper CPU runs on 2+ cores)
- **RAM** — 2 GB minimum (4 GB recommended)

## Quick Start

```bash
# 1. Clone or download the edge directory
git clone https://github.com/nodusrf/nodus.git
cd nodus/edge/recept-fm

# 2. Configure
cp .env.example .env
# Edit .env — set your frequencies, API key, and node ID

# 3. Start
docker compose up -d

# 4. Check logs
docker compose logs -f nodus-edge
```

## Configuration

Copy `.env.example` to `.env` and edit:

| Variable | Required | Description |
|----------|----------|-------------|
| `RECEPT_FM_CORE_FREQUENCIES` | Yes | Frequencies in Hz, comma-separated |
| `RECEPT_SYNAPSE_ENDPOINT` | Yes | Central Nodus endpoint (`https://api.nodusalert.ai`) |
| `RECEPT_SYNAPSE_AUTH_TOKEN` | Yes | API key from your Nodus admin |
| `RECEPT_NODE_ID` | Yes | Unique name for this node (e.g., `edge-W1ABC`) |
| `RECEPT_FM_SCANNER_BACKEND` | No | `airband` (default) or `rtl_fm` |
| `RECEPT_FM_GAIN` | No | RTL-SDR gain, 0–49 (default: 40) |
| `WHISPER_MODEL` | No | `base` (default), `small`, or `medium` |

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
docker compose logs nodus-edge | tail -20

# Test Whisper health endpoint
curl http://localhost:8000/health
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
