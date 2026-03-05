#!/usr/bin/env bash
# NodusNet FM Edge Node — One-Command Installer
#
# Install from anywhere:
#   bash -c "$(curl -fsSL https://raw.githubusercontent.com/nodusrf/nodus-edge-fm/main/install.sh)"
#
# Or from the repo:
#   ./install.sh
#
# Options:
#   --dry-run    Preview without making changes
#
# Prerequisites: Linux (x86_64 or arm64), internet access, RTL-SDR dongle
# Installs to: ~/nodusnet/
#
# Fixes #3

set -euo pipefail

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

INSTALL_DIR="$HOME/nodusnet"
GITHUB_RAW="https://raw.githubusercontent.com/nodusrf/nodus-edge-fm/main"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m'

DRY_RUN=false
for arg in "$@"; do
    [ "$arg" = "--dry-run" ] && DRY_RUN=true
done

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

info()  { echo -e "  ${GREEN}[INFO]${NC} $*"; }
warn()  { echo -e "  ${YELLOW}[WARN]${NC} $*"; }
err()   { echo -e "  ${RED}[ERROR]${NC} $*"; }
step()  { echo -e "\n  ${BOLD}$1${NC}\n  $(printf '─%.0s' {1..50})"; }
die()   { err "$1"; exit 1; }

run() {
    if $DRY_RUN; then
        info "[dry-run] $*"
    else
        "$@"
    fi
}

# Resolve a file: use local repo copy if available, otherwise download from GitHub
resolve_file() {
    local repo_path="$1"
    local dest="$2"
    local desc="$3"

    # Check if we're inside the nodus-edge-fm repo
    local script_dir
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}" 2>/dev/null || echo ".")" && pwd 2>/dev/null || echo "")"
    local local_file="$script_dir/$repo_path"

    if [ -f "$local_file" ] 2>/dev/null; then
        cp "$local_file" "$dest"
        info "Copied $desc (local)"
    else
        info "Downloading $desc..."
        curl -fsSL "$GITHUB_RAW/$repo_path" -o "$dest" || die "Failed to download $desc"
        info "Downloaded $desc"
    fi
}

# ---------------------------------------------------------------------------
# Banner
# ---------------------------------------------------------------------------

echo ""
echo "============================================================"
echo -e "  ${BOLD}NodusNet FM Edge Node — Installer${NC}"
echo "============================================================"
echo ""
echo "  This script will:"
echo "    1. Install Docker (if needed)"
echo "    2. Configure USB permissions for RTL-SDR"
echo "    3. Sign in to your NodusNet account"
echo "    4. Run the setup wizard (location, callsign, frequencies)"
echo "    5. Deploy containers to ~/nodusnet/"
echo ""
echo -e "  ${DIM}Don't have an account? Sign up at https://nodusrf.com/edge${NC}"
echo ""
echo -e "  ${DIM}https://github.com/nodusrf/nodus-edge-fm${NC}"
echo ""

if $DRY_RUN; then
    warn "DRY RUN MODE — no system changes will be made."
    echo ""
fi

# ---------------------------------------------------------------------------
# Step 1: Platform check
# ---------------------------------------------------------------------------

step "Step 1: Platform Check"

UNAME_S="$(uname -s)"
ARCH="$(uname -m)"

if [ "$UNAME_S" != "Linux" ]; then
    die "This installer requires Linux. Detected: $UNAME_S"
fi

if [ "$ARCH" = "x86_64" ] || [ "$ARCH" = "aarch64" ]; then
    info "Platform: Linux $ARCH"
else
    warn "Untested architecture: $ARCH. Proceeding anyway."
fi

# Check Python 3 early (needed for setup wizard)
if ! command -v python3 &>/dev/null; then
    info "python3 not found — installing..."
    if command -v apt-get &>/dev/null; then
        run sudo apt-get update -qq
        run sudo apt-get install -y -qq python3
    elif command -v dnf &>/dev/null; then
        run sudo dnf install -y python3
    elif command -v pacman &>/dev/null; then
        run sudo pacman -S --noconfirm python
    else
        die "python3 is required. Install it with your package manager."
    fi
fi
info "Python: $(python3 --version 2>&1)"

# ---------------------------------------------------------------------------
# Step 2: Docker Engine
# ---------------------------------------------------------------------------

step "Step 2: Docker Engine"

if command -v docker &>/dev/null; then
    DOCKER_VER="$(docker --version 2>/dev/null || echo "unknown")"
    info "Docker already installed: $DOCKER_VER"
else
    info "Docker not found — installing via get.docker.com..."
    if $DRY_RUN; then
        info "[dry-run] curl -fsSL https://get.docker.com | sh"
    else
        curl -fsSL https://get.docker.com | sh
        info "Docker installed."
    fi
fi

# Add user to docker group (if not already)
if ! groups "$USER" 2>/dev/null | grep -qw docker; then
    info "Adding $USER to docker group..."
    run sudo usermod -aG docker "$USER"
    warn "You may need to log out and back in for group changes to take effect."
    warn "If 'docker compose' fails below, run: newgrp docker"
fi

# Check Docker Compose v2 plugin
if docker compose version &>/dev/null 2>&1; then
    COMPOSE_VER="$(docker compose version --short 2>/dev/null || echo "v2")"
    info "Docker Compose plugin: $COMPOSE_VER"
elif $DRY_RUN; then
    info "[dry-run] Docker Compose not checked"
elif command -v docker-compose &>/dev/null; then
    warn "Found docker-compose (v1) but not 'docker compose' (v2 plugin)."
    die "Docker Compose v2 plugin required. See: https://docs.docker.com/compose/install/"
else
    die "Docker Compose not found. See: https://docs.docker.com/compose/install/"
fi

# ---------------------------------------------------------------------------
# Step 3: RTL-SDR USB permissions
# ---------------------------------------------------------------------------

step "Step 3: RTL-SDR USB Permissions"

UDEV_RULE="/etc/udev/rules.d/20-rtlsdr.rules"
BLACKLIST_CONF="/etc/modprobe.d/blacklist-rtlsdr-dvb.conf"

UDEV_CONTENT='# RTL-SDR USB device — allow non-root access
SUBSYSTEM=="usb", ATTRS{idVendor}=="0bda", ATTRS{idProduct}=="2838", MODE="0666"
SUBSYSTEM=="usb", ATTRS{idVendor}=="0bda", ATTRS{idProduct}=="2832", MODE="0666"'

BLACKLIST_CONTENT='# Prevent kernel DVB-T driver from claiming RTL-SDR dongles
blacklist dvb_usb_rtl28xxu
blacklist rtl2832
blacklist rtl2830'

UDEV_CHANGED=false

if [ -f "$UDEV_RULE" ]; then
    info "udev rule already exists: $UDEV_RULE"
else
    info "Creating udev rule for RTL-SDR..."
    if $DRY_RUN; then
        info "[dry-run] Would write $UDEV_RULE"
    else
        echo "$UDEV_CONTENT" | sudo tee "$UDEV_RULE" > /dev/null
        UDEV_CHANGED=true
    fi
fi

if [ -f "$BLACKLIST_CONF" ]; then
    info "DVB blacklist already exists: $BLACKLIST_CONF"
else
    info "Blacklisting DVB-T kernel drivers..."
    if $DRY_RUN; then
        info "[dry-run] Would write $BLACKLIST_CONF"
    else
        echo "$BLACKLIST_CONTENT" | sudo tee "$BLACKLIST_CONF" > /dev/null
        UDEV_CHANGED=true
    fi
fi

if $UDEV_CHANGED; then
    info "Reloading udev rules..."
    run sudo udevadm control --reload-rules
    run sudo udevadm trigger
    info "USB permissions configured. Replug your RTL-SDR if it was already connected."
fi

# ---------------------------------------------------------------------------
# Step 4: NodusNet Account
# ---------------------------------------------------------------------------

step "Step 4: NodusNet Account"

REGISTRY_HOST="registry.nodusrf.com"
NODUSNET_USERNAME=""

if $DRY_RUN; then
    info "[dry-run] docker login $REGISTRY_HOST"
    NODUSNET_USERNAME="dry-run-user"
else
    # Check if already logged in from a previous install
    if docker pull "$REGISTRY_HOST/nodus-edge-fm:latest" --quiet &>/dev/null 2>&1; then
        info "Already authenticated with NodusNet registry."
        # Try to extract username from docker config
        NODUSNET_USERNAME="$(python3 -c "
import json, base64, pathlib
cfg = json.loads(pathlib.Path.home().joinpath('.docker/config.json').read_text())
auth = cfg.get('auths',{}).get('$REGISTRY_HOST',{}).get('auth','')
if auth: print(base64.b64decode(auth).decode().split(':')[0])
" 2>/dev/null || echo "")"
    else
        echo ""
        echo -e "  Sign in with your NodusNet credentials."
        echo -e "  ${DIM}Don't have an account? Sign up at https://nodusrf.com/edge${NC}"
        echo ""

        MAX_ATTEMPTS=3
        ATTEMPT=0

        while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
            ATTEMPT=$((ATTEMPT + 1))

            read -r -p "  NodusNet username: " NODUSNET_USERNAME
            if [ -z "$NODUSNET_USERNAME" ]; then
                warn "Username cannot be empty."
                continue
            fi

            read -r -s -p "  NodusNet password: " NODUSNET_PASSWORD
            echo ""

            if echo "$NODUSNET_PASSWORD" | docker login "$REGISTRY_HOST" -u "$NODUSNET_USERNAME" --password-stdin &>/dev/null 2>&1; then
                info "Signed in as ${BOLD}${NODUSNET_USERNAME}${NC}"
                break
            else
                if [ $ATTEMPT -lt $MAX_ATTEMPTS ]; then
                    warn "Invalid credentials. Please try again. ($ATTEMPT/$MAX_ATTEMPTS)"
                else
                    echo ""
                    err "Authentication failed after $MAX_ATTEMPTS attempts."
                    echo ""
                    echo -e "  Need an account? Sign up at ${BOLD}https://nodusrf.com/edge${NC}"
                    echo ""
                    exit 1
                fi
            fi
        done

        # Clear password from memory
        unset NODUSNET_PASSWORD
    fi
fi

# Export username so the setup wizard can pre-fill node ID / callsign
export NODUSNET_USERNAME

# ---------------------------------------------------------------------------
# Step 5: Download files + prepare install directory
# ---------------------------------------------------------------------------

step "Step 5: Prepare ~/nodusnet/"

run mkdir -p "$INSTALL_DIR/data"

COMPOSE_DST="$INSTALL_DIR/docker-compose.yml"
WIZARD_PATH="$INSTALL_DIR/.setup-wizard.py"
ZIPMETA_PATH="$INSTALL_DIR/.zip_metro.json"

if $DRY_RUN; then
    info "[dry-run] Would download/copy files to $INSTALL_DIR/"
else
    # docker-compose.yml (public repo already has no build: context)
    resolve_file "docker-compose.yml" "$COMPOSE_DST" "docker-compose.yml"

    # Setup wizard
    resolve_file "setup.py" "$WIZARD_PATH" "setup wizard"

    # CBSA zip-to-metro mapping
    resolve_file "data/zip_metro.json" "$ZIPMETA_PATH" "zip-to-metro data (CBSA)"
fi

# ---------------------------------------------------------------------------
# Step 6: Run setup wizard
# ---------------------------------------------------------------------------

step "Step 6: Setup Wizard"

info "Launching setup wizard..."
echo ""

WIZARD_ARGS=(--output-dir "$INSTALL_DIR")

if $DRY_RUN; then
    WIZARD_ARGS+=(--dry-run)
fi

# Point the wizard at the downloaded zip_metro.json
export NODUSNET_ZIP_METRO_PATH="$ZIPMETA_PATH"

python3 "$WIZARD_PATH" "${WIZARD_ARGS[@]}"

# Verify wizard output (skip for dry run)
if ! $DRY_RUN; then
    if [ ! -f "$INSTALL_DIR/.env" ]; then
        die "Setup wizard did not create $INSTALL_DIR/.env — something went wrong."
    fi
    info "Configuration files ready in $INSTALL_DIR/"
fi

# Clean up wizard files (keep zip_metro for future re-runs)
rm -f "$WIZARD_PATH"

# ---------------------------------------------------------------------------
# Step 7: Deploy Containers
# ---------------------------------------------------------------------------

step "Step 7: Deploy Containers"

if $DRY_RUN; then
    info "[dry-run] docker compose -f $COMPOSE_DST pull"
    info "[dry-run] docker compose -f $COMPOSE_DST up -d"
else
    info "Pulling container images (this may take a few minutes on first run)..."
    docker compose -f "$COMPOSE_DST" pull

    info "Starting containers..."
    docker compose -f "$COMPOSE_DST" up -d
fi

# ---------------------------------------------------------------------------
# Step 8: OTA Updater
# ---------------------------------------------------------------------------

step "Step 8: OTA Updater (Auto-Update)"

UPDATER_PATH="$INSTALL_DIR/nodusnet-updater.sh"

if $DRY_RUN; then
    info "[dry-run] Would install OTA updater"
else
    resolve_file "nodusnet-updater.sh" "$UPDATER_PATH" "OTA updater"
    chmod +x "$UPDATER_PATH"

    # Install systemd user timer if systemd user session is available
    if systemctl --user status &>/dev/null 2>&1; then
        UNIT_DIR="$HOME/.config/systemd/user"
        mkdir -p "$UNIT_DIR"

        cat > "$UNIT_DIR/nodusnet-updater.service" <<SVCEOF
[Unit]
Description=NodusNet OTA Updater

[Service]
Type=oneshot
ExecStart=$UPDATER_PATH
Environment=HOME=$HOME
WorkingDirectory=$INSTALL_DIR
SVCEOF

        cat > "$UNIT_DIR/nodusnet-updater.timer" <<TMREOF
[Unit]
Description=NodusNet OTA Update Check (every 5 min)

[Timer]
OnBootSec=60
OnUnitActiveSec=5min
RandomizedDelaySec=60

[Install]
WantedBy=timers.target
TMREOF

        systemctl --user daemon-reload
        systemctl --user enable --now nodusnet-updater.timer
        loginctl enable-linger "$USER" 2>/dev/null || true
        info "OTA updater installed (systemd timer, every 5 min)"
    else
        # Fallback to cron
        CRON_LINE="*/5 * * * * $UPDATER_PATH >> $INSTALL_DIR/.updater.log 2>&1"
        (crontab -l 2>/dev/null | grep -v "nodusnet-updater"; echo "$CRON_LINE") | crontab -
        info "OTA updater installed (cron, every 5 min)"
    fi
fi

# ---------------------------------------------------------------------------
# Step 9: Dashboard Restart Watcher
# ---------------------------------------------------------------------------

step "Step 9: Dashboard Restart Watcher"

if $DRY_RUN; then
    info "[dry-run] Would install restart watcher"
else
    if systemctl --user status &>/dev/null 2>&1; then
        UNIT_DIR="$HOME/.config/systemd/user"
        mkdir -p "$UNIT_DIR"

        cat > "$UNIT_DIR/nodusnet-restart.path" <<PATHEOF
[Path]
PathModified=$INSTALL_DIR/data/.restart-signal

[Install]
WantedBy=default.target
PATHEOF

        cat > "$UNIT_DIR/nodusnet-restart.service" <<RSTEOF
[Unit]
Description=NodusNet Edge Restart (triggered by dashboard)

[Service]
Type=oneshot
WorkingDirectory=$INSTALL_DIR
ExecStart=docker compose up -d
ExecStartPost=rm -f $INSTALL_DIR/data/.restart-signal
RSTEOF

        systemctl --user daemon-reload
        systemctl --user enable --now nodusnet-restart.path
        info "Dashboard restart watcher installed (systemd path unit)"
    else
        warn "systemd user session not available — dashboard restart trigger will not auto-apply."
        warn "After saving settings in the dashboard, run: cd $INSTALL_DIR && docker compose up -d"
    fi
fi

# ---------------------------------------------------------------------------
# Step 10: Wait for health
# ---------------------------------------------------------------------------

step "Step 10: Health Check"

if $DRY_RUN; then
    info "[dry-run] Would wait for Whisper model download + health check"
else
    info "Waiting for Whisper to download model and become healthy..."
    info "(First run may take 1-3 minutes while the model downloads)"
    echo ""

    MAX_WAIT=180
    ELAPSED=0
    INTERVAL=5

    while [ $ELAPSED -lt $MAX_WAIT ]; do
        HEALTH="$(docker compose -f "$COMPOSE_DST" ps whisper --format '{{.Health}}' 2>/dev/null || echo "")"
        if [ "$HEALTH" = "healthy" ]; then
            info "Whisper is healthy!"
            break
        fi

        STATE="$(docker compose -f "$COMPOSE_DST" ps whisper --format '{{.State}}' 2>/dev/null || echo "")"
        if [ "$STATE" = "exited" ]; then
            warn "Whisper container exited unexpectedly."
            warn "Check logs: docker compose -f $COMPOSE_DST logs whisper"
            break
        fi

        printf "    Waiting... (%ds / %ds)\r" "$ELAPSED" "$MAX_WAIT"
        sleep $INTERVAL
        ELAPSED=$((ELAPSED + INTERVAL))
    done

    if [ $ELAPSED -ge $MAX_WAIT ]; then
        warn "Timed out waiting for Whisper (${MAX_WAIT}s). It may still be downloading."
        warn "Check status: docker compose -f $COMPOSE_DST ps"
    fi

    echo ""
    RECEPT_STATE="$(docker compose -f "$COMPOSE_DST" ps recept-fm --format '{{.State}}' 2>/dev/null || echo "")"
    if [ "$RECEPT_STATE" = "running" ]; then
        info "Recept FM is running!"
    else
        warn "Recept FM state: $RECEPT_STATE"
        warn "Check logs: docker compose -f $COMPOSE_DST logs recept-fm"
    fi
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

NODE_ID="$(grep '^RECEPT_NODE_ID=' "$INSTALL_DIR/.env" 2>/dev/null | cut -d= -f2 || echo "unknown")"

echo ""
echo "============================================================"
echo -e "  ${GREEN}${BOLD}NodusNet FM Edge Node — Installed!${NC}"
echo "============================================================"
echo ""
echo -e "  Node:          ${BOLD}${NODE_ID}${NC}"
echo "  Install dir:   $INSTALL_DIR/"
echo ""
echo -e "  ${BOLD}Dashboard:${NC}   http://localhost:8073"
echo -e "  ${BOLD}Logs:${NC}        cd $INSTALL_DIR && docker compose logs -f"
echo -e "  ${BOLD}Stop:${NC}        cd $INSTALL_DIR && docker compose down"
echo -e "  ${BOLD}Restart:${NC}     cd $INSTALL_DIR && docker compose up -d"
echo -e "  ${BOLD}Status:${NC}      cd $INSTALL_DIR && docker compose ps"
echo -e "  ${BOLD}Update now:${NC}  $INSTALL_DIR/nodusnet-updater.sh"
echo ""
echo -e "  ${DIM}GPU Whisper? Edit .env, set RECEPT_WHISPER_API_URL, then:${NC}"
echo -e "  ${DIM}cd $INSTALL_DIR && docker compose up -d --scale whisper=0${NC}"
echo ""
echo "============================================================"
echo ""
