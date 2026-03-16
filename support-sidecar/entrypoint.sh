#!/bin/bash
# RELEASE: nodusrf/nodus-edge support-sidecar/entrypoint.sh
set -e

# --- Configure SSH authorized keys ---
SUPPORT_HOME="/home/nodus-support"
SSH_DIR="${SUPPORT_HOME}/.ssh"

mkdir -p "${SSH_DIR}"
chmod 700 "${SSH_DIR}"

if [ -n "${AUTHORIZED_KEYS}" ]; then
    echo "${AUTHORIZED_KEYS}" > "${SSH_DIR}/authorized_keys"
fi

# Fall back to baked-in support agent key if no override provided
if [ ! -s "${SSH_DIR}/authorized_keys" ] && [ -f /support-agent-key.pub ]; then
    cp /support-agent-key.pub "${SSH_DIR}/authorized_keys"
    echo "Using baked-in support agent key"
fi

# Ensure correct permissions (may have been volume-mounted)
chmod 600 "${SSH_DIR}/authorized_keys" 2>/dev/null || true
chown -R nodus-support:nodus-support "${SSH_DIR}"

# --- Match Docker socket GID so nodus-support can access it ---
if [ -S /var/run/docker.sock ]; then
    SOCK_GID=$(stat -c "%g" /var/run/docker.sock)
    if ! id -G nodus-support | tr ' ' '\n' | grep -qx "${SOCK_GID}"; then
        # Check if a group with this GID already exists
        EXISTING_GROUP=$(getent group "${SOCK_GID}" | cut -d: -f1 || true)
        if [ -n "${EXISTING_GROUP}" ]; then
            # GID exists under another name — just add user to it
            usermod -aG "${EXISTING_GROUP}" nodus-support
        else
            groupadd -g "${SOCK_GID}" hostdocker
            usermod -aG hostdocker nodus-support
        fi
        echo "Granted nodus-support access to Docker socket (GID ${SOCK_GID})"
    fi
fi

# --- Start dropbear SSH on port 2222 ---
# Generate host keys if missing
mkdir -p /etc/dropbear
[ -f /etc/dropbear/dropbear_ed25519_host_key ] || dropbearkey -t ed25519 -f /etc/dropbear/dropbear_ed25519_host_key
[ -f /etc/dropbear/dropbear_rsa_host_key ]     || dropbearkey -t rsa -s 2048 -f /etc/dropbear/dropbear_rsa_host_key

echo "Starting dropbear SSH on port 2222..."
dropbear -F -E -p 2222 -s -g &
DROPBEAR_PID=$!

# --- Start cloudflared tunnel ---
if [ -z "${TUNNEL_TOKEN}" ]; then
    echo "WARNING: TUNNEL_TOKEN not set — cloudflared will not start"
    # Just wait on dropbear
    wait ${DROPBEAR_PID}
    exit $?
fi

echo "Starting cloudflared tunnel..."
cloudflared tunnel --no-autoupdate run --token "${TUNNEL_TOKEN}" &
CLOUDFLARED_PID=$!

# --- Wait for either process to exit ---
wait -n ${DROPBEAR_PID} ${CLOUDFLARED_PID}
EXIT_CODE=$?

echo "A process exited with code ${EXIT_CODE}, shutting down..."
kill ${DROPBEAR_PID} ${CLOUDFLARED_PID} 2>/dev/null || true
wait 2>/dev/null || true
exit ${EXIT_CODE}
