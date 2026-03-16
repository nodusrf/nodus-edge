"""Diagnostic event logger.

Previously published events to MQTT. Now logs them locally via structlog.
Diagnostic telemetry reaches the server via the standard heartbeat/check-in path.
"""

import structlog

logger = structlog.get_logger(__name__)


def publish_diagnostic(node_id: str, event_type: str, payload: dict) -> None:
    """Log a diagnostic event (best-effort, non-blocking)."""
    logger.debug("diagnostic_event", node_id=node_id, event_type=event_type, **payload)
