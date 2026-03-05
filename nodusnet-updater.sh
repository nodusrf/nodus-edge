#!/usr/bin/env bash
# NodusNet Edge Node — OTA Updater
#
# Pull-based update system: fetches version manifest from Gateway, pulls new
# Docker images when available, restarts containers, and reports status.
#
# Intended to run via systemd user timer every 5 minutes.
# Can also be run manually: ./nodusnet-updater.sh [--force]
#
# Requires: curl, docker compose, sha256sum, jq (optional, uses Python fallback)
#
# Refs #207

set -euo pipefail

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

INSTALL_DIR="${NODUSNET_DIR:-$HOME/nodusnet}"
STATE_FILE="$INSTALL_DIR/.updater-state.json"
LOCK_FILE="$INSTALL_DIR/.updater.lock"
COMPOSE_FILE="$INSTALL_DIR/docker-compose.yml"
LOG_FILE="$INSTALL_DIR/.updater.log"
UPDATER_SCRIPT="$(realpath "${BASH_SOURCE[0]}")"

# Gateway URL from .env or environment
if [ -z "${NODUSNET_SERVER:-}" ] && [ -f "$INSTALL_DIR/.env" ]; then
    NODUSNET_SERVER="$(grep '^NODUSNET_SERVER=' "$INSTALL_DIR/.env" 2>/dev/null | cut -d= -f2 || echo "")"
fi
NODUSNET_SERVER="${NODUSNET_SERVER:-}"

# Device token from .env or environment
if [ -z "${NODUSNET_TOKEN:-}" ] && [ -f "$INSTALL_DIR/.env" ]; then
    NODUSNET_TOKEN="$(grep '^NODUSNET_TOKEN=' "$INSTALL_DIR/.env" 2>/dev/null | cut -d= -f2 || echo "")"
fi
NODUSNET_TOKEN="${NODUSNET_TOKEN:-}"

# Node ID for status reporting
if [ -z "${RECEPT_NODE_ID:-}" ] && [ -f "$INSTALL_DIR/.env" ]; then
    RECEPT_NODE_ID="$(grep '^RECEPT_NODE_ID=' "$INSTALL_DIR/.env" 2>/dev/null | cut -d= -f2 || echo "")"
fi
RECEPT_NODE_ID="${RECEPT_NODE_ID:-unknown}"

FORCE=false
for arg in "$@"; do
    [ "$arg" = "--force" ] && FORCE=true
done

UPDATER_VERSION="1"  # Bump when making breaking changes to the updater

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*" | tee -a "$LOG_FILE"; }
die() { log "FATAL: $*"; report_status "failed" "$*"; exit 1; }

# JSON field extraction — use jq if available, otherwise Python
json_get() {
    local file="$1" path="$2"
    if command -v jq &>/dev/null; then
        jq -r "$path" "$file" 2>/dev/null || echo ""
    else
        python3 -c "
import json, sys
data = json.load(open('$file'))
keys = '$path'.strip('.').split('.')
for k in keys:
    data = data.get(k, '') if isinstance(data, dict) else ''
print(data if data else '')
" 2>/dev/null || echo ""
    fi
}

# Write JSON state file
write_state() {
    local recept_tag="$1" whisper_tag="$2" updater_ver="$3" compose_ver="$4"
    cat > "$STATE_FILE" <<STATEEOF
{
  "recept_fm_tag": "$recept_tag",
  "whisper_cpu_tag": "$whisper_tag",
  "updater_version": "$updater_ver",
  "compose_version": "$compose_ver",
  "last_update": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
STATEEOF
}

report_status() {
    local status="$1" error="${2:-}"
    [ -z "$NODUSNET_SERVER" ] && return 0
    [ -z "$NODUSNET_TOKEN" ] && return 0

    local body
    body=$(python3 -c "
import json
print(json.dumps({
    'node_id': '$RECEPT_NODE_ID',
    'updater_version': '$UPDATER_VERSION',
    'recept_fm_tag': '${NEW_RECEPT_TAG:-unknown}',
    'whisper_tag': '${NEW_WHISPER_TAG:-unknown}',
    'status': '$status',
    'error': '$error'
}))
" 2>/dev/null) || return 0

    curl -sf -X POST \
        -H "Authorization: Bearer $NODUSNET_TOKEN" \
        -H "Content-Type: application/json" \
        -d "$body" \
        "$NODUSNET_SERVER/v1/edge/update-status" &>/dev/null || true
}

verify_sha256() {
    local file="$1" expected="$2"
    [ -z "$expected" ] && return 0  # No checksum to verify
    local actual
    actual="$(sha256sum "$file" | cut -d' ' -f1)"
    [ "$actual" = "$expected" ]
}

# ---------------------------------------------------------------------------
# Locking (flock prevents concurrent runs)
# ---------------------------------------------------------------------------

exec 200>"$LOCK_FILE"
if ! flock -n 200; then
    echo "Another updater instance is running. Exiting."
    exit 0
fi

# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------

[ -d "$INSTALL_DIR" ] || die "Install directory not found: $INSTALL_DIR"
[ -f "$COMPOSE_FILE" ] || die "docker-compose.yml not found: $COMPOSE_FILE"

if [ -z "$NODUSNET_SERVER" ]; then
    log "NODUSNET_SERVER not set — skipping update check"
    exit 0
fi

# Trim trailing log (keep last 500 lines)
if [ -f "$LOG_FILE" ]; then
    tail -n 500 "$LOG_FILE" > "$LOG_FILE.tmp" 2>/dev/null && mv "$LOG_FILE.tmp" "$LOG_FILE" || true
fi

log "--- Update check starting (node=$RECEPT_NODE_ID) ---"

# ---------------------------------------------------------------------------
# Step 1: Fetch manifest from Gateway
# ---------------------------------------------------------------------------

MANIFEST_TMP="$(mktemp)"
trap 'rm -f "$MANIFEST_TMP"' EXIT

HTTP_CODE=$(curl -sf -w "%{http_code}" \
    -H "Authorization: Bearer $NODUSNET_TOKEN" \
    -o "$MANIFEST_TMP" \
    "$NODUSNET_SERVER/v1/edge/manifest" 2>/dev/null) || HTTP_CODE="000"

if [ "$HTTP_CODE" != "200" ]; then
    log "Failed to fetch manifest (HTTP $HTTP_CODE) — will retry next cycle"
    exit 0
fi

log "Manifest fetched successfully"

# ---------------------------------------------------------------------------
# Step 2: Parse manifest and current state
# ---------------------------------------------------------------------------

NEW_RECEPT_TAG="$(json_get "$MANIFEST_TMP" ".images.recept-fm.tag")"
NEW_WHISPER_TAG="$(json_get "$MANIFEST_TMP" ".images.whisper-cpu.tag")"
NEW_RECEPT_DIGEST="$(json_get "$MANIFEST_TMP" ".images.recept-fm.digest")"
NEW_WHISPER_DIGEST="$(json_get "$MANIFEST_TMP" ".images.whisper-cpu.digest")"
MANIFEST_UPDATER_VER="$(json_get "$MANIFEST_TMP" ".updater.version")"
MANIFEST_UPDATER_SHA="$(json_get "$MANIFEST_TMP" ".updater.sha256")"
MANIFEST_COMPOSE_VER="$(json_get "$MANIFEST_TMP" ".compose.version")"
MANIFEST_COMPOSE_SHA="$(json_get "$MANIFEST_TMP" ".compose.sha256")"

# Read current state
CUR_RECEPT_TAG=""
CUR_WHISPER_TAG=""
CUR_UPDATER_VER=""
CUR_COMPOSE_VER=""

if [ -f "$STATE_FILE" ]; then
    CUR_RECEPT_TAG="$(json_get "$STATE_FILE" ".recept_fm_tag")"
    CUR_WHISPER_TAG="$(json_get "$STATE_FILE" ".whisper_cpu_tag")"
    CUR_UPDATER_VER="$(json_get "$STATE_FILE" ".updater_version")"
    CUR_COMPOSE_VER="$(json_get "$STATE_FILE" ".compose_version")"
fi

# ---------------------------------------------------------------------------
# Step 3: Self-update check
# ---------------------------------------------------------------------------

if [ -n "$MANIFEST_UPDATER_VER" ] && [ "$MANIFEST_UPDATER_VER" != "$CUR_UPDATER_VER" ] && [ "$MANIFEST_UPDATER_VER" != "$UPDATER_VERSION" ]; then
    log "Updater version changed: $UPDATER_VERSION -> $MANIFEST_UPDATER_VER"
    UPDATER_TMP="$(mktemp)"

    if curl -sf -H "Authorization: Bearer $NODUSNET_TOKEN" \
        -o "$UPDATER_TMP" \
        "$NODUSNET_SERVER/v1/edge/updater.sh"; then

        if verify_sha256 "$UPDATER_TMP" "$MANIFEST_UPDATER_SHA"; then
            cp "$UPDATER_TMP" "$UPDATER_SCRIPT"
            chmod +x "$UPDATER_SCRIPT"
            rm -f "$UPDATER_TMP"
            log "Self-update complete — re-executing"
            exec "$UPDATER_SCRIPT" "$@"
        else
            log "WARN: Updater sha256 mismatch — skipping self-update"
            rm -f "$UPDATER_TMP"
        fi
    else
        log "WARN: Failed to download new updater — skipping self-update"
        rm -f "$UPDATER_TMP"
    fi
fi

# ---------------------------------------------------------------------------
# Step 4: Compose file update check
# ---------------------------------------------------------------------------

COMPOSE_UPDATED=false

if [ -n "$MANIFEST_COMPOSE_VER" ] && [ "$MANIFEST_COMPOSE_VER" != "$CUR_COMPOSE_VER" ]; then
    log "Compose version changed: $CUR_COMPOSE_VER -> $MANIFEST_COMPOSE_VER"
    COMPOSE_TMP="$(mktemp)"

    if curl -sf -H "Authorization: Bearer $NODUSNET_TOKEN" \
        -o "$COMPOSE_TMP" \
        "$NODUSNET_SERVER/v1/edge/docker-compose.yml"; then

        if verify_sha256 "$COMPOSE_TMP" "$MANIFEST_COMPOSE_SHA"; then
            # Back up current compose file for rollback
            cp "$COMPOSE_FILE" "$COMPOSE_FILE.bak"
            cp "$COMPOSE_TMP" "$COMPOSE_FILE"
            COMPOSE_UPDATED=true
            log "Compose file updated"
        else
            log "WARN: Compose sha256 mismatch — skipping compose update"
        fi

        rm -f "$COMPOSE_TMP"
    else
        log "WARN: Failed to download new compose file"
        rm -f "$COMPOSE_TMP"
    fi
fi

# ---------------------------------------------------------------------------
# Step 5: Image update check
# ---------------------------------------------------------------------------

IMAGES_CHANGED=false

if [ "$NEW_RECEPT_TAG" != "$CUR_RECEPT_TAG" ] || [ "$NEW_WHISPER_TAG" != "$CUR_WHISPER_TAG" ] || $FORCE; then
    IMAGES_CHANGED=true
    if $FORCE; then
        log "Forced update — pulling images"
    else
        log "Image tags changed: recept-fm=$CUR_RECEPT_TAG->$NEW_RECEPT_TAG whisper-cpu=$CUR_WHISPER_TAG->$NEW_WHISPER_TAG"
    fi
fi

if ! $IMAGES_CHANGED && ! $COMPOSE_UPDATED; then
    log "Everything up to date"
    # Still write state in case this is first run
    write_state "$NEW_RECEPT_TAG" "$NEW_WHISPER_TAG" "$UPDATER_VERSION" "${MANIFEST_COMPOSE_VER:-$CUR_COMPOSE_VER}"
    exit 0
fi

# ---------------------------------------------------------------------------
# Step 6: Pull new images (old containers keep running)
# ---------------------------------------------------------------------------

log "Pulling new images (existing containers stay up)..."

# Export tags so docker compose can use ${RECEPT_FM_TAG} and ${WHISPER_CPU_TAG}
export RECEPT_FM_TAG="$NEW_RECEPT_TAG"
export WHISPER_CPU_TAG="$NEW_WHISPER_TAG"

if ! docker compose -f "$COMPOSE_FILE" pull 2>>"$LOG_FILE"; then
    log "ERROR: docker compose pull failed — old containers untouched"
    if $COMPOSE_UPDATED; then
        log "Rolling back compose file..."
        cp "$COMPOSE_FILE.bak" "$COMPOSE_FILE"
    fi
    report_status "failed" "docker compose pull failed"
    exit 1
fi

log "New images downloaded. Old containers still serving."

# ---------------------------------------------------------------------------
# Step 7: Canary pre-flight — validate new images before swap
# ---------------------------------------------------------------------------
#
# Spin up throwaway containers from the NEW images on alternate ports, without
# hardware (no USB passthrough). If the app boots and the health endpoint
# responds, the image is good. The production containers remain untouched
# during this entire step.
# ---------------------------------------------------------------------------

CANARY_RECEPT="nodusnet-canary-recept"
CANARY_WHISPER="nodusnet-canary-whisper"
CANARY_RECEPT_PORT=18082   # health on alt port (prod is 8082)
CANARY_WHISPER_PORT=18000   # health on alt port (prod is 8000)

# Cleanup helper — always remove canary containers
cleanup_canaries() {
    docker rm -f "$CANARY_RECEPT" "$CANARY_WHISPER" &>/dev/null || true
}
trap 'cleanup_canaries; rm -f "$MANIFEST_TMP"' EXIT

RECEPT_IMAGE="$(json_get "$MANIFEST_TMP" ".images.recept-fm.image"):$NEW_RECEPT_TAG"
WHISPER_IMAGE="$(json_get "$MANIFEST_TMP" ".images.whisper-cpu.image"):$NEW_WHISPER_TAG"

log "Canary pre-flight: testing $RECEPT_IMAGE"

# Start canary recept-fm — no USB, alternate port, just verify it boots
docker run -d --rm \
    --name "$CANARY_RECEPT" \
    -p "127.0.0.1:${CANARY_RECEPT_PORT}:8082" \
    -e RECEPT_MODE=fm \
    -e RECEPT_DRY_RUN=true \
    "$RECEPT_IMAGE" 2>>"$LOG_FILE" || true

# Start canary whisper — alternate port, verify model loads
docker run -d --rm \
    --name "$CANARY_WHISPER" \
    -p "127.0.0.1:${CANARY_WHISPER_PORT}:8000" \
    -e WHISPER_MODEL="${WHISPER_MODEL:-base}" \
    -e WHISPER_DEVICE=cpu \
    "$WHISPER_IMAGE" 2>>"$LOG_FILE" || true

# Health-check canaries (90s timeout — generous for model download)
CANARY_OK=true
CANARY_MAX=90
CANARY_ELAPSED=0

log "Waiting for canary health checks (${CANARY_MAX}s timeout)..."

RECEPT_HEALTHY=false
WHISPER_HEALTHY=false

while [ $CANARY_ELAPSED -lt $CANARY_MAX ]; do
    if ! $RECEPT_HEALTHY; then
        if curl -sf "http://127.0.0.1:${CANARY_RECEPT_PORT}/health" &>/dev/null; then
            RECEPT_HEALTHY=true
            log "  canary recept-fm healthy (${CANARY_ELAPSED}s)"
        fi
    fi

    if ! $WHISPER_HEALTHY; then
        if curl -sf "http://127.0.0.1:${CANARY_WHISPER_PORT}/health" &>/dev/null; then
            WHISPER_HEALTHY=true
            log "  canary whisper healthy (${CANARY_ELAPSED}s)"
        fi
    fi

    if $RECEPT_HEALTHY && $WHISPER_HEALTHY; then
        break
    fi

    # Check if canary containers crashed
    if ! docker ps -q -f "name=$CANARY_RECEPT" 2>/dev/null | grep -q .; then
        if ! $RECEPT_HEALTHY; then
            log "  canary recept-fm exited prematurely"
            CANARY_OK=false
            break
        fi
    fi
    if ! docker ps -q -f "name=$CANARY_WHISPER" 2>/dev/null | grep -q .; then
        if ! $WHISPER_HEALTHY; then
            log "  canary whisper exited prematurely"
            CANARY_OK=false
            break
        fi
    fi

    sleep 5
    CANARY_ELAPSED=$((CANARY_ELAPSED + 5))
done

if ! $RECEPT_HEALTHY || ! $WHISPER_HEALTHY; then
    CANARY_OK=false
fi

# Tear down canaries regardless
cleanup_canaries

if ! $CANARY_OK; then
    log "CANARY FAILED — new images are broken. Old containers untouched."
    if $COMPOSE_UPDATED && [ -f "$COMPOSE_FILE.bak" ]; then
        log "Rolling back compose file..."
        cp "$COMPOSE_FILE.bak" "$COMPOSE_FILE"
    fi
    FAIL_DETAIL="canary failed:"
    $RECEPT_HEALTHY || FAIL_DETAIL="$FAIL_DETAIL recept-fm"
    $WHISPER_HEALTHY || FAIL_DETAIL="$FAIL_DETAIL whisper"
    report_status "failed" "$FAIL_DETAIL"
    exit 1
fi

log "Canary passed — both images verified healthy"

# ---------------------------------------------------------------------------
# Step 8: Swap — replace production containers with verified images
# ---------------------------------------------------------------------------

log "Swapping to new images (old containers stopping)..."

if ! docker compose -f "$COMPOSE_FILE" up -d 2>>"$LOG_FILE"; then
    log "ERROR: docker compose up failed after canary passed"
    if $COMPOSE_UPDATED; then
        log "Rolling back compose file..."
        cp "$COMPOSE_FILE.bak" "$COMPOSE_FILE"
        docker compose -f "$COMPOSE_FILE" up -d 2>>"$LOG_FILE" || true
    fi
    report_status "failed" "docker compose up failed"
    exit 1
fi

# ---------------------------------------------------------------------------
# Step 9: Post-swap health check (120s timeout)
# ---------------------------------------------------------------------------

log "Post-swap health check (production containers)..."

MAX_WAIT=120
ELAPSED=0
HEALTHY=false

while [ $ELAPSED -lt $MAX_WAIT ]; do
    HEALTH="$(docker compose -f "$COMPOSE_FILE" ps recept-fm --format '{{.Health}}' 2>/dev/null || echo "")"

    if [ "$HEALTH" = "healthy" ]; then
        HEALTHY=true
        break
    fi

    STATE="$(docker compose -f "$COMPOSE_FILE" ps recept-fm --format '{{.State}}' 2>/dev/null || echo "")"
    if [ "$STATE" = "exited" ]; then
        log "ERROR: recept-fm container exited after swap"
        break
    fi

    sleep 5
    ELAPSED=$((ELAPSED + 5))
done

# ---------------------------------------------------------------------------
# Step 10: Finalize — commit state or rollback
# ---------------------------------------------------------------------------

if $HEALTHY; then
    log "Update successful — production recept-fm is healthy"
    write_state "$NEW_RECEPT_TAG" "$NEW_WHISPER_TAG" "$UPDATER_VERSION" "${MANIFEST_COMPOSE_VER:-$CUR_COMPOSE_VER}"
    report_status "success"
    rm -f "$COMPOSE_FILE.bak"
else
    log "ERROR: Post-swap health check failed after ${ELAPSED}s"

    # Rollback
    if $COMPOSE_UPDATED && [ -f "$COMPOSE_FILE.bak" ]; then
        log "Rolling back compose file..."
        cp "$COMPOSE_FILE.bak" "$COMPOSE_FILE"
    fi

    if [ -n "$CUR_RECEPT_TAG" ] && [ -n "$CUR_WHISPER_TAG" ]; then
        log "Rolling back to previous tags: recept-fm=$CUR_RECEPT_TAG whisper-cpu=$CUR_WHISPER_TAG"
        export RECEPT_FM_TAG="$CUR_RECEPT_TAG"
        export WHISPER_CPU_TAG="$CUR_WHISPER_TAG"
        docker compose -f "$COMPOSE_FILE" up -d 2>>"$LOG_FILE" || true
    fi

    report_status "failed" "post-swap health check timeout after ${ELAPSED}s"
    exit 1
fi

log "--- Update check complete ---"
