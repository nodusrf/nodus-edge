# RELEASE: nodusrf/nodus-edge support-sidecar/Dockerfile
FROM python:3.12.3-slim-bookworm

ARG TARGETARCH

# Install base utilities and dropbear
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        gnupg \
        jq \
        dropbear \
    && rm -rf /var/lib/apt/lists/*

# Install cloudflared from official .deb
RUN ARCH=$(case "${TARGETARCH}" in \
        amd64) echo "amd64" ;; \
        arm64) echo "arm64" ;; \
        arm)   echo "arm"   ;; \
        *)     echo "amd64" ;; \
    esac) \
    && curl -fsSL "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-${ARCH}.deb" \
        -o /tmp/cloudflared.deb \
    && dpkg -i /tmp/cloudflared.deb \
    && rm /tmp/cloudflared.deb

# Install Docker CLI and compose plugin from official Docker repo
RUN install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg \
        -o /etc/apt/keyrings/docker.asc \
    && chmod a+r /etc/apt/keyrings/docker.asc \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
        https://download.docker.com/linux/debian bookworm stable" \
        > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        docker-ce-cli \
        docker-compose-plugin \
    && rm -rf /var/lib/apt/lists/*

# Create support user (Docker socket GID is matched at runtime by entrypoint)
RUN useradd -m -s /bin/bash nodus-support \
    && mkdir -p /home/nodus-support/.ssh \
    && chmod 700 /home/nodus-support/.ssh \
    && chown -R nodus-support:nodus-support /home/nodus-support/.ssh

# Env vars for runtime configuration
ENV TUNNEL_TOKEN=""
ENV AUTHORIZED_KEYS=""

COPY support-agent-key.pub /support-agent-key.pub
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 2222

ENTRYPOINT ["/entrypoint.sh"]
