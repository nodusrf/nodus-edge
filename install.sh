#!/usr/bin/env bash
# NodusNet Edge Node — One-Command Installer
#
# Install from anywhere:
#   curl -fsSL https://raw.githubusercontent.com/nodusrf/nodus-edge/main/install.sh | bash
#
# Or from the repo:
#   ./install.sh
#
# Options:
#   --dry-run    Preview without making changes
#
# Prerequisites: Linux (x86_64 or arm64), internet access, RTL-SDR dongle
# Installs to: ~/nodusedge/
#
# Refs #203

set -euo pipefail

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

INSTALL_DIR="$HOME/nodusedge"
GITHUB_RAW="https://raw.githubusercontent.com/nodusrf/nodus-edge/main"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m'

DRY_RUN=false
WIZARD_EXTRA_ARGS=()
SKIP_NEXT=false
for arg in "$@"; do
    if $SKIP_NEXT; then
        SKIP_NEXT=false
        continue
    fi
    case "$arg" in
        --dry-run) DRY_RUN=true ;;
    esac
done
# Forward wizard-compatible args (everything except --dry-run)
for arg in "$@"; do
    [ "$arg" = "--dry-run" ] && continue
    WIZARD_EXTRA_ARGS+=("$arg")
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

# Use sudo only when not already root
SUDO=""
[ "$(id -u)" -ne 0 ] && SUDO="sudo"

# Resolve a file: use local repo copy if available, otherwise download from GitHub
resolve_file() {
    local repo_path="$1"
    local dest="$2"
    local desc="$3"

    info "Downloading $desc..."
    curl -fsSL "$GITHUB_RAW/$repo_path" -o "$dest" || die "Failed to download $desc"
    info "Downloaded $desc"
}

# ---------------------------------------------------------------------------
# Banner
# ---------------------------------------------------------------------------

echo ""
echo "============================================================"
echo -e "  ${BOLD}NodusNet Edge Node — Installer${NC}"
echo "============================================================"
echo ""
echo "  This script will:"
echo "    1. Install Docker (if needed)"
echo "    2. Configure USB permissions for RTL-SDR"
echo "    3. Run the setup wizard (server, location, callsign)"
echo "    4. Deploy containers to ~/nodusedge/"
echo ""
echo -e "  ${DIM}https://github.com/nodusrf/nodus-edge${NC}"
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
        run $SUDO apt-get update -qq
        run $SUDO apt-get install -y -qq python3
    elif command -v dnf &>/dev/null; then
        run $SUDO dnf install -y python3
    elif command -v pacman &>/dev/null; then
        run $SUDO pacman -S --noconfirm python
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
    run $SUDO usermod -aG docker "$USER"
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
# Step 2b: Clean up existing installation (if any)
# ---------------------------------------------------------------------------

EXISTING_COMPOSE="$INSTALL_DIR/docker-compose.yml"
if [ -f "$EXISTING_COMPOSE" ] && command -v docker &>/dev/null; then
    step "Existing Installation Detected"
    info "Found existing NodusNet at $INSTALL_DIR/"
    info "Stopping old containers..."
    if $DRY_RUN; then
        info "[dry-run] docker compose -f $EXISTING_COMPOSE down"
    else
        docker compose -f "$EXISTING_COMPOSE" down 2>/dev/null || true
        info "Old containers stopped."
    fi

    # Back up .env so the wizard can offer to reuse settings
    if [ -f "$INSTALL_DIR/.env" ]; then
        if $DRY_RUN; then
            info "[dry-run] Would back up .env to .env.bak"
        else
            cp "$INSTALL_DIR/.env" "$INSTALL_DIR/.env.bak"
            info "Backed up existing .env to .env.bak"
        fi
    fi
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
        echo "$UDEV_CONTENT" | $SUDO tee "$UDEV_RULE" > /dev/null
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
        echo "$BLACKLIST_CONTENT" | $SUDO tee "$BLACKLIST_CONF" > /dev/null
        UDEV_CHANGED=true
    fi
fi

if $UDEV_CHANGED; then
    info "Reloading udev rules..."
    $SUDO udevadm control --reload-rules 2>/dev/null && $SUDO udevadm trigger 2>/dev/null \
        && info "USB permissions configured. Replug your RTL-SDR if it was already connected." \
        || warn "Could not reload udev (container environment?). USB rules applied on next boot."
fi

# ---------------------------------------------------------------------------
# Step 4: Download files + prepare install directory
# ---------------------------------------------------------------------------

step "Step 4: Prepare ~/nodusedge/"

run mkdir -p "$INSTALL_DIR/data"

COMPOSE_DST="$INSTALL_DIR/docker-compose.yml"
WIZARD_PATH="$INSTALL_DIR/.setup-wizard.py"
ZIPMETA_PATH="$INSTALL_DIR/.zip_metro.json"
REPEATERS_PATH="$INSTALL_DIR/.repeaters.json"

# Detect Raspberry Pi (aarch64 + Pi hardware)
IS_PI=false
if [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "armv7l" ]; then
    if grep -qi "raspberry\|BCM2" /proc/cpuinfo 2>/dev/null || \
       grep -qi "raspberry" /proc/device-tree/model 2>/dev/null; then
        IS_PI=true
        info "Raspberry Pi detected — will use remote Whisper (no local container)"
    fi
fi

if $DRY_RUN; then
    info "[dry-run] Would download/copy files to $INSTALL_DIR/"
else
    # docker-compose.yml — strip build: context (not needed for image-based deploys)
    resolve_file "docker-compose.yml" "$COMPOSE_DST.tmp" "docker-compose.yml"
    sed '/^    build:/,/^    [a-z]/{ /^    build:/d; /^      context:/d; /^      dockerfile:/d; }' \
        "$COMPOSE_DST.tmp" > "$COMPOSE_DST.tmp2"
    rm -f "$COMPOSE_DST.tmp"

    # On Pi, remove depends_on whisper (whisper container won't run)
    if $IS_PI; then
        sed '/depends_on:/,/condition:/d' "$COMPOSE_DST.tmp2" > "$COMPOSE_DST"
    else
        mv "$COMPOSE_DST.tmp2" "$COMPOSE_DST"
    fi
    rm -f "$COMPOSE_DST.tmp2"

    # Setup wizard
    resolve_file "setup.py" "$WIZARD_PATH" "setup wizard"

    # CBSA zip-to-metro mapping
    resolve_file "data/zip_metro.json" "$ZIPMETA_PATH" "zip-to-metro data (CBSA)"

    # Offline repeater bundle (all bands — wizard filters by selected band)
    resolve_file "data/repeaters_us.json" "$REPEATERS_PATH" "repeater database (RepeaterBook)"
fi

# ---------------------------------------------------------------------------
# Step 5: Run setup wizard
# ---------------------------------------------------------------------------

step "Step 5: Setup Wizard"

if $DRY_RUN; then
    info "[dry-run] Would run setup wizard"
else
    info "Launching setup wizard..."
    echo ""

    WIZARD_ARGS=(--output-dir "$INSTALL_DIR")

    # Append any wizard args forwarded from the command line
    WIZARD_ARGS+=("${WIZARD_EXTRA_ARGS[@]}")

    # Point the wizard at downloaded data files
    export NODUSNET_ZIP_METRO_PATH="$ZIPMETA_PATH"
    export NODUSNET_REPEATERS_PATH="$REPEATERS_PATH"

    if exec 3</dev/tty 2>/dev/null; then
        exec 3<&-
        PYTHONUNBUFFERED=1 python3 -u "$WIZARD_PATH" "${WIZARD_ARGS[@]}" </dev/tty
    else
        PYTHONUNBUFFERED=1 python3 -u "$WIZARD_PATH" "${WIZARD_ARGS[@]}"
    fi
fi

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
# Step 6: Pull images and start containers
# ---------------------------------------------------------------------------

step "Step 6: Deploy Containers"

# On Pi, scale whisper to 0 (user must provide a remote GPU endpoint)
COMPOSE_EXTRA_ARGS=""
if $IS_PI; then
    info "Raspberry Pi: skipping local Whisper container (use remote GPU endpoint)"
    COMPOSE_EXTRA_ARGS="--scale whisper=0"
fi

if $DRY_RUN; then
    info "[dry-run] docker compose -f $COMPOSE_DST pull"
    info "[dry-run] docker compose -f $COMPOSE_DST up -d $COMPOSE_EXTRA_ARGS"
else
    info "Pulling container images (this may take a few minutes on first run)..."
    if $IS_PI; then
        docker compose -f "$COMPOSE_DST" pull nodus-edge support-sidecar
    else
        docker compose -f "$COMPOSE_DST" pull
    fi

    info "Starting containers..."
    docker compose -f "$COMPOSE_DST" up -d $COMPOSE_EXTRA_ARGS
fi

# ---------------------------------------------------------------------------
# Step 7: Auto-Update
# ---------------------------------------------------------------------------

# (no user-facing step header — this is internal setup)

UPDATER_PATH="$INSTALL_DIR/nodusnet-updater.sh"

if $DRY_RUN; then
    info "[dry-run] Would install auto-updater"
else
    resolve_file "nodusnet-updater.sh" "$UPDATER_PATH" "auto-updater"
    chmod +x "$UPDATER_PATH"

    # Install systemd user timer if systemd user session is available
    if systemctl --user status &>/dev/null 2>&1; then
        UNIT_DIR="$HOME/.config/systemd/user"
        mkdir -p "$UNIT_DIR"

        cat > "$UNIT_DIR/nodusnet-updater.service" <<SVCEOF
[Unit]
Description=NodusNet Auto-Updater

[Service]
Type=oneshot
ExecStart=$UPDATER_PATH
Environment=HOME=$HOME
WorkingDirectory=$INSTALL_DIR
SVCEOF

        cat > "$UNIT_DIR/nodusnet-updater.timer" <<TMREOF
[Unit]
Description=NodusNet Auto-Update Check (every 5 min)

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
        info "Auto-updater installed (checks every 5 min)"
    else
        # Fallback to cron
        CRON_LINE="*/5 * * * * $UPDATER_PATH >> $INSTALL_DIR/.updater.log 2>&1"
        (crontab -l 2>/dev/null | grep -v "nodusnet-updater"; echo "$CRON_LINE") | crontab -
        info "Auto-updater installed (checks every 5 min)"
    fi
fi

# ---------------------------------------------------------------------------
# Step 8: Dashboard Restart Watcher
# ---------------------------------------------------------------------------

step "Step 8: Dashboard Restart Watcher"

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
# Step 9: Wait for health
# ---------------------------------------------------------------------------

step "Step 9: Health Check"

if $DRY_RUN; then
    info "[dry-run] Would wait for health check"
elif $IS_PI; then
    info "Raspberry Pi: skipping local Whisper health check (using remote endpoint)"
    echo ""
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
    EDGE_STATE="$(docker compose -f "$COMPOSE_DST" ps nodus-edge --format '{{.State}}' 2>/dev/null || echo "")"
    if [ "$EDGE_STATE" = "running" ]; then
        info "NodusEdge is running!"
    else
        warn "NodusEdge state: $EDGE_STATE"
        warn "Check logs: docker compose -f $COMPOSE_DST logs nodus-edge"
    fi
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

NODE_ID="$(grep -E '^(RECEPT_NODE_ID|NODUS_EDGE_NODE_ID)=' "$INSTALL_DIR/.env" 2>/dev/null | head -1 | cut -d= -f2 || echo "unknown")"

echo ""
echo "============================================================"
echo -e "  ${GREEN}${BOLD}NodusNet Edge Node — Installed!${NC}"
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
echo -e "  ${DIM}GPU Whisper? Edit .env, set WHISPER_API_URL endpoint, then:${NC}"
echo -e "  ${DIM}cd $INSTALL_DIR && docker compose up -d --scale whisper=0${NC}"
echo ""
echo "============================================================"
echo ""
