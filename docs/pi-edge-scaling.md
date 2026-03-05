# Scaling Strategy: Pi Edge Nodes + Proxmox Brain (Advisory)

## Context

Currently, all radio hardware (RTL-SDR dongles) may be attached to a central server via USB passthrough into LXC containers. This can cause reliability issues (dongle contention between containers). As we scale to more bands/modes at the current site AND remote locations, we need an architecture that is reliable and minimal-ops ("flash an SD card, plug in, forget").

## Recommendation: Hybrid Edge/Core Architecture

**Use Raspberry Pis as dedicated "ears" (Recept only). Keep Proxmox for the "brain" (everything else).**

```
Edge Nodes (Pis, bare metal)          Core (Proxmox, LXC containers)
┌──────────────────┐                 ┌────────────────────────────┐
│ Pi #1 — 2m FM    │                 │ Synapse (correlation)      │
│  1x RTL-SDR      │──local net──→   │ Cortex (LLM reasoning)     │
│  Recept (airband)│                 │ Campus (geospatial)        │
├──────────────────┤                 │ Diagnostics (health)       │
│ Pi #2 — P25      │                 │ Whisper GPU (pve1)         │
│  1x RTL-SDR      │──local net──→   │ Reflex (alerting)          │
│  Recept          │                 │ FM Agent (Discord)         │
├──────────────────┤                 │ Ollama (LLM)               │
│ Pi #3 — APRS     │                 └────────────────────────────┘
│  1x RTL-SDR      │──local net──→
│  Recept           │
├──────────────────┤
│ Pi #4 — Remote   │
│  1x RTL-SDR      │──Tailscale──→  (same core services via VPN)
│  Recept           │
└──────────────────┘
```

## Why This Works

### Reliability wins
- **No USB passthrough** — bare metal has direct, stable USB access
- **Physical isolation** — a hung dongle on Pi #1 can't affect Pi #2
- **No dongle contention** — each Pi owns exactly one SDR, no enumeration races
- **Watchdog still works** — Recept's existing USB reset logic works better on bare metal (no cgroup layer)

### Existing architecture already supports it
- Recept is stateless, auto-registers via heartbeat to Diagnostics
- Multiple Recepts feeding one Synapse is the designed pattern
- Config is pure env vars: `RECEPT_NODE_ID`, `RECEPT_SYNAPSE_ENDPOINT`, `RECEPT_DIAGNOSTICS_ENDPOINT`
- No code changes required for multi-node

### Proxmox stays where it's strong
- Brain services (Cortex, Synapse, Campus) need CPU/RAM, benefit from LXC management
- GPU services (Whisper) need PCIe passthrough, which Proxmox handles well
- Snapshots, backup, fleet visibility for the parts that matter
- No USB hardware to pass through = no USB fragility

### Minimal ops path
- **Standard SD card image**: Raspberry Pi OS Lite + Python venv + Recept + systemd unit
- **Config via `/boot/recept.env`**: mount SD card on any PC, edit one file, boot
- **Auto-register**: heartbeat tells Diagnostics "I'm alive" — no manual registration
- **Self-healing**: systemd `Restart=always` + Recept's built-in watchdog (USB reset on hang)
- **Monitoring**: existing Diagnostics dashboard shows all nodes' health
- **Updates**: `scp` or `rsync` the Recept package + restart (could wrap in a simple script)

## Scaling by band/mode

| Band/Mode | Hardware per node | Recept mode | Notes |
|-----------|-------------------|-------------|-------|
| 2m FM (current) | Pi 4/5 + Nooelec SMArt v5 | `fm` (airband backend) | Replaces central FM container |
| P25 (current) | Pi 4/5 + RTL-SDR | `p25` (trunk-recorder) | Replaces central P25 container |
| APRS (144.390) | Pi + RTL-SDR | New mode needed | Would need `direwolf` or `rtl_fm` decode |
| Airband (118-137 MHz) | Pi + RTL-SDR | New mode needed | Different antenna, AM demod |
| GMRS/FRS | Pi + RTL-SDR | `fm` variant | UHF antenna needed |
| 70cm ham | Pi + RTL-SDR | `fm` variant | Different freq list |

Each band = 1 Pi + 1 dongle + 1 antenna. Total cost per node: ~$50-80.

## Remote deployment (geographic scaling)

For nodes outside your LAN:
- **Tailscale** for zero-config VPN (NAT traversal, no port forwarding)
- Pi joins Tailscale network on first boot
- Recept pushes segments to Synapse via Tailscale IP
- Heartbeats flow back to Diagnostics via same VPN
- Audio segments are small (~100-500KB WAV per transmission) — fine over any broadband

## What to watch out for

| Risk | Mitigation |
|------|------------|
| SD card wear/failure | Use high-endurance cards (Samsung PRO Endurance), minimize writes, or USB boot |
| Pi power instability | Use official Pi PSU, consider UPS hat for remote sites |
| Whisper latency for remote nodes | Acceptable — transcription is async, not real-time. Could add local Whisper (Pi 5 can run tiny/base models) |
| Software updates across fleet | Simple rsync script for now. Ansible if fleet grows past ~5 nodes |
| Pi 4 vs Pi 5 | Pi 4 is fine for Recept (CPU-light). Pi 5 if running trunk-recorder (heavier decode) |
| Network dependency | Recept buffers segments locally in output dir — survives brief outages |

## What stays on the central server

The brain services (Synapse, Cortex, Whisper GPU, Campus, Reflex, etc.) stay on your central infrastructure. The containers that currently do radio capture would be decommissioned as their functions move to dedicated Pis.

## When NOT to use a Pi

- **trunk-recorder** may prefer x86 (some decoder libraries are x86-optimized). Test on Pi first.
- **If you want Proxmox management on edge nodes**, consider Intel N100 mini PCs (~$120) — they run Proxmox and have USB 3.0, but cost 2-3x a Pi.
- **If you only need one more band at the same site**, a single additional Pi is simpler than any other approach.

## Summary

The architecture naturally splits into "ears" (cheap, dedicated, bare-metal Pis with SDR dongles) and "brain" (Proxmox LXC containers running Synapse/Cortex/Whisper/etc.). This plays to each platform's strengths: Pis get reliable USB and physical isolation, Proxmox gets fleet management and GPU passthrough. No code changes needed — Recept already supports multi-node deployment via env var configuration.
