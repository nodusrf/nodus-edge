"""
FM segment validation for Edge nodes.

Defines what a "complete" FM segment looks like and provides
validation at two layers:
1. Startup config validation — catch config issues before first segment
2. Per-segment validation — tag incomplete segments for dashboard/alerting
"""

from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional


# Well-known simplex frequencies (Hz) exempt from repeater callsign check
SIMPLEX_FREQUENCIES = {
    146520000,  # 2m national calling
    446000000,  # 70cm national calling
}

# 2m and 70cm repeater output sub-bands (Hz)
# Repeater outputs fall in these ranges; simplex and input freqs are outside.
REPEATER_BANDS_HZ = [
    (145_100_000, 145_500_000),   # 2m repeater outputs (145.1–145.5)
    (146_610_000, 147_000_000),   # 2m repeater outputs (146.61–147.00)
    (147_000_000, 147_400_000),   # 2m repeater outputs (147.00–147.40)
    (442_000_000, 445_000_000),   # 70cm repeater outputs (442–445)
    (447_000_000, 450_000_000),   # 70cm repeater outputs (447–450)
]


def is_repeater_band(frequency_hz: int) -> bool:
    """Check if a frequency falls within a known repeater output band."""
    if frequency_hz in SIMPLEX_FREQUENCIES:
        return False
    for low, high in REPEATER_BANDS_HZ:
        if low <= frequency_hz <= high:
            return True
    return False


@dataclass
class FixAction:
    """A one-button fix action for a validation warning."""
    label: str           # e.g. "Sync Repeaters"
    endpoint: str        # e.g. "/api/sync"
    method: str = "POST" # "POST" or "PUT"
    payload: Optional[Dict] = None  # For PUT /api/env
    restart: bool = False  # Show "restart required" after fix

    def to_dict(self) -> dict:
        d = {"label": self.label, "endpoint": self.endpoint, "method": self.method}
        if self.payload:
            d["payload"] = self.payload
        if self.restart:
            d["restart"] = True
        return d


@dataclass
class ValidationWarning:
    """A validation issue found in a segment or startup config."""
    code: str
    message: str
    severity: str  # "warning" or "error"
    fix: Optional[FixAction] = field(default=None)

    def to_dict(self) -> dict:
        d = {"code": self.code, "message": self.message, "severity": self.severity}
        if self.fix:
            d["fix"] = self.fix.to_dict()
        return d


def validate_fm_segment(
    segment: dict,
    transcription_enabled: bool = True,
) -> List[ValidationWarning]:
    """
    Validate a single FM segment for completeness.

    Args:
        segment: Segment dict (FMTranscriptSegmentV1 serialized)
        transcription_enabled: Whether transcription is enabled on this node

    Returns:
        List of validation warnings (empty = segment is complete).
    """
    warnings: List[ValidationWarning] = []

    rf = segment.get("rf_channel") or {}
    freq_hz = rf.get("frequency_hz", 0)

    # Frequency must be positive
    if freq_hz <= 0:
        warnings.append(ValidationWarning(
            code="invalid_frequency",
            message="Segment has no valid frequency",
            severity="error",
        ))

    # Repeater callsign required on repeater-band frequencies
    repeater_callsign = rf.get("repeater_callsign")
    if freq_hz > 0 and is_repeater_band(freq_hz) and not repeater_callsign:
        freq_mhz = freq_hz / 1_000_000
        warnings.append(ValidationWarning(
            code="missing_repeater_callsign",
            message=f"No repeater callsign for {freq_mhz:.3f} MHz (repeater band)",
            severity="warning",
            fix=FixAction(label="Sync Repeaters", endpoint="/api/sync"),
        ))

    # Metro must be non-empty
    metro = segment.get("metro")
    if not metro:
        warnings.append(ValidationWarning(
            code="missing_metro",
            message="Segment has no metro area set",
            severity="warning",
        ))

    # Source node ID must be non-empty and not default
    node_id = segment.get("source_node_id", "")
    if not node_id or node_id in ("unknown", "default"):
        warnings.append(ValidationWarning(
            code="missing_node_id",
            message="Segment has no valid source_node_id",
            severity="warning",
        ))

    # Transcription should be present when enabled
    if transcription_enabled:
        tx = segment.get("transcription")
        if tx is None:
            warnings.append(ValidationWarning(
                code="missing_transcription",
                message="Transcription enabled but segment has no transcription",
                severity="warning",
            ))

    return warnings


def validate_startup_config(
    repeater_db_loaded: bool,
    repeater_count: int,
    frequencies: List[int],
    synapse_endpoint: Optional[str],
    node_id: str,
    metro: Optional[str],
) -> List[ValidationWarning]:
    """
    Validate startup configuration for an FM edge node.

    Returns:
        List of validation warnings (empty = config looks good).
    """
    warnings: List[ValidationWarning] = []

    # Check repeater database
    if not repeater_db_loaded or repeater_count == 0:
        # Count how many configured frequencies are in repeater bands
        repeater_band_freqs = [f for f in frequencies if is_repeater_band(f)]
        if repeater_band_freqs:
            warnings.append(ValidationWarning(
                code="empty_repeater_db",
                message=(
                    f"Repeater database empty but {len(repeater_band_freqs)} "
                    f"configured frequencies are in repeater bands — "
                    f"segments will have null repeater_callsign"
                ),
                severity="error",
                fix=FixAction(label="Sync Repeaters", endpoint="/api/sync"),
            ))

    # Check metro
    if not metro:
        warnings.append(ValidationWarning(
            code="missing_metro",
            message="NODUS_EDGE_METRO not set — segments will have no metro area",
            severity="warning",
            fix=FixAction(
                label="Open Settings",
                endpoint="#settings",
                method="NAV",
            ),
        ))

    # Check node ID
    if not node_id or node_id in ("unknown", "default"):
        import socket
        hostname = socket.gethostname()
        warnings.append(ValidationWarning(
            code="missing_node_id",
            message="NODUS_EDGE_NODE_ID not set — using default node identifier",
            severity="warning",
            fix=FixAction(
                label="Set Node ID",
                endpoint="/api/env",
                method="PUT",
                payload={"fields": {"NODUS_EDGE_NODE_ID": hostname}},
                restart=True,
            ),
        ))

    # Check frequencies
    if not frequencies:
        warnings.append(ValidationWarning(
            code="no_frequencies",
            message="No FM frequencies configured",
            severity="error",
            # Manual — needs frequency list from user
        ))

    # Check Synapse connectivity
    if not synapse_endpoint:
        warnings.append(ValidationWarning(
            code="no_synapse",
            message="NODUSNET_SERVER not set — segments will not be forwarded",
            severity="warning",
            # Manual — needs endpoint URL from user
        ))

    return warnings
