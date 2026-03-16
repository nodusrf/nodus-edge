"""
APRS Packet Processing Pipeline for Nodus Edge.

Processes decoded APRS packets from Direwolf:
1. Parse raw APRS packet string via aprslib
2. Classify packet type (position, weather, status, message, telemetry)
3. Build APRSPacketSegmentV1
4. Emit to output directory and Synapse
"""

import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4

import structlog

from . import __version__
from .config import settings
from .forwarding.emitter import SegmentEmitter
from .forwarding.synapse_publisher import SynapsePublisher
from .schema import APRSPacketSegmentV1, APRSPosition, APRSWeather

logger = structlog.get_logger(__name__)

# APRS packet type classification based on data type identifier
_POSITION_DTYPES = {"!", "/", "=", "@"}
_WEATHER_DTYPE = "_"
_STATUS_DTYPE = ">"
_MESSAGE_DTYPE = ":"
_OBJECT_DTYPE = ";"
_ITEM_DTYPE = ")"
_TELEMETRY_DTYPE = "T"


def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _celsius_to_f(c: Optional[float]) -> Optional[float]:
    """Convert Celsius (aprslib output) to Fahrenheit (APRS native)."""
    if c is None:
        return None
    return round(c * 9.0 / 5.0 + 32.0, 1)


def _mps_to_mph(mps: Optional[float]) -> Optional[float]:
    """Convert m/s (aprslib output) to mph (APRS native)."""
    if mps is None:
        return None
    return round(mps * 2.23694, 1)


def _mm_to_inches(mm: Optional[float]) -> Optional[float]:
    """Convert mm (aprslib output) to inches (APRS native)."""
    if mm is None:
        return None
    return round(mm / 25.4, 3)


def _classify_packet(raw: str) -> str:
    """Classify APRS packet type from the raw string."""
    # Find the info field (after the last ':' in the path)
    parts = raw.split(":", 1)
    if len(parts) < 2 or not parts[1]:
        return "unknown"

    info = parts[1]
    dtype = info[0]

    if dtype in _POSITION_DTYPES:
        # Check if it's a weather position (contains '_' weather data)
        if "_" in info and any(c in info for c in ("g", "t", "r", "p", "P", "h", "b")):
            return "weather"
        return "position"
    elif dtype == _WEATHER_DTYPE:
        return "weather"
    elif dtype == _STATUS_DTYPE:
        return "status"
    elif dtype == _MESSAGE_DTYPE:
        return "message"
    elif dtype == _OBJECT_DTYPE:
        return "object"
    elif dtype == _ITEM_DTYPE:
        return "item"
    elif dtype == _TELEMETRY_DTYPE:
        return "telemetry"
    else:
        return "unknown"


def _parse_aprs_packet(raw: str) -> Optional[Dict[str, Any]]:
    """
    Parse a raw APRS packet string into structured data.

    Uses aprslib if available, falls back to basic parsing.
    """
    try:
        import aprslib
        parsed = aprslib.parse(raw)
        return parsed
    except ImportError:
        return _basic_parse(raw)
    except Exception as e:
        logger.debug("aprslib parse failed", error=str(e), raw=raw[:80])
        return _basic_parse(raw)


def _basic_parse(raw: str) -> Optional[Dict[str, Any]]:
    """Basic APRS packet parser (fallback when aprslib unavailable)."""
    try:
        # Format: CALLSIGN>PATH:INFO
        header, info = raw.split(":", 1)
        from_call, path = header.split(">", 1)

        return {
            "from": from_call.strip(),
            "to": path.split(",")[0].strip(),
            "path": [p.strip() for p in path.split(",")],
            "raw": raw,
            "via": "",
            "format": "unknown",
            "raw_info": info,
        }
    except Exception:
        return None


class APRSPipeline:
    """
    Processing pipeline for APRS packets decoded by Direwolf.

    No Whisper transcription — packets are already structured data.
    Extracts position, weather, status, and message information.
    """

    def __init__(self, node_id: Optional[str] = None):
        self.node_id = node_id or settings.node_id
        self.emitter = SegmentEmitter()
        self.synapse_publisher = SynapsePublisher()

        # Dedup: track recently seen packets by (from_call, raw_info_hash)
        self._recent_packets: Dict[str, float] = {}
        self._dedup_ttl = 30.0  # seconds

        # Stats
        self._packet_count = 0
        self._position_count = 0
        self._weather_count = 0
        self._message_count = 0
        self._status_count = 0
        self._other_count = 0
        self._duplicate_count = 0
        self._parse_error_count = 0
        self._synapse_published_count = 0

        # Segment callbacks (e.g., dashboard)
        self._segment_callbacks: List[Callable] = []

        logger.info(
            "APRS pipeline initialized",
            node_id=self.node_id,
            synapse_enabled=self.synapse_publisher.enabled,
        )

    def process_packet(self, raw_packet: str, timestamp: float) -> Optional[APRSPacketSegmentV1]:
        """
        Process a single decoded APRS packet.

        Args:
            raw_packet: Raw APRS packet string from Direwolf.
            timestamp: Unix timestamp when packet was received.

        Returns:
            The emitted segment, or None if filtered/errored.
        """
        # Deduplicate (APRS digipeaters repeat packets)
        dedup_key = raw_packet.strip()
        now = time.time()

        # Clean expired entries periodically
        if self._packet_count % 100 == 0:
            cutoff = now - self._dedup_ttl
            self._recent_packets = {
                k: v for k, v in self._recent_packets.items() if v > cutoff
            }

        if dedup_key in self._recent_packets:
            self._duplicate_count += 1
            return None
        self._recent_packets[dedup_key] = now

        # Parse packet
        parsed = _parse_aprs_packet(raw_packet)
        if not parsed:
            self._parse_error_count += 1
            logger.debug("Failed to parse APRS packet", raw=raw_packet[:80])
            return None

        self._packet_count += 1

        # Classify
        packet_type = _classify_packet(raw_packet)
        from_call = parsed.get("from", "UNKNOWN").upper()
        to_call = parsed.get("to", "").upper()
        path = parsed.get("path", [])
        comment = parsed.get("comment", "") or parsed.get("raw_info", "")

        # Extract position if available
        position = None
        lat = _safe_float(parsed.get("latitude"))
        lng = _safe_float(parsed.get("longitude"))
        if lat is not None and lng is not None:
            position = APRSPosition(
                latitude=lat,
                longitude=lng,
                altitude_m=_safe_float(parsed.get("altitude")),
                speed_kmh=_safe_float(parsed.get("speed")),
                course=_safe_float(parsed.get("course")),
                symbol=parsed.get("symbol", ""),
                symbol_table=parsed.get("symbol_table", "/"),
                posambiguity=parsed.get("posambiguity"),
            )

        # Extract weather if available
        # aprslib converts APRS native units to metric. Convert back to imperial
        # since APRS transmits in Fahrenheit/mph/inches and operators expect those.
        weather = None
        if packet_type == "weather":
            wx = parsed.get("weather", {}) if isinstance(parsed.get("weather"), dict) else {}
            weather = APRSWeather(
                temperature_f=_celsius_to_f(_safe_float(wx.get("temperature"))),
                humidity_pct=_safe_float(wx.get("humidity")),
                pressure_mbar=_safe_float(wx.get("pressure")),
                wind_speed_mph=_mps_to_mph(_safe_float(wx.get("wind_speed"))),
                wind_direction=_safe_float(wx.get("wind_direction")),
                wind_gust_mph=_mps_to_mph(_safe_float(wx.get("wind_gust"))),
                rain_1h_inches=_mm_to_inches(_safe_float(wx.get("rain_1h"))),
                rain_24h_inches=_mm_to_inches(_safe_float(wx.get("rain_24h"))),
                rain_since_midnight_inches=_mm_to_inches(_safe_float(wx.get("rain_since_midnight"))),
            )

        # Packet metadata from aprslib
        packet_format = parsed.get("format")
        raw_ts = parsed.get("raw_timestamp")
        is_message_capable = parsed.get("messagecapable")

        # Object/item fields
        object_name = parsed.get("object_name", "").strip() if parsed.get("object_name") else None
        is_alive = parsed.get("alive") if packet_type in ("object", "item") else None

        # Extract message fields
        message_to = None
        message_text = None
        message_id = None
        if packet_type == "message":
            message_to = parsed.get("addresse", "").strip() or parsed.get("message_to", "")
            message_text = parsed.get("message_text", "") or comment
            message_id = parsed.get("msgNo", "")

        # Build segment
        ts = datetime.fromtimestamp(timestamp, tz=timezone.utc)

        segment = APRSPacketSegmentV1(
            segment_id=uuid4(),
            source_node_id=self.node_id,
            source_node_version=__version__,
            metro=settings.metro or None,
            timestamp=ts,
            from_callsign=from_call,
            to_callsign=to_call,
            path=path if isinstance(path, list) else [str(path)],
            packet_type=packet_type,
            packet_format=packet_format,
            raw_packet=raw_packet,
            raw_timestamp=raw_ts,
            is_message_capable=is_message_capable,
            position=position,
            weather=weather,
            comment=str(comment)[:500] if comment else None,
            object_name=object_name,
            is_alive=is_alive,
            message_to=message_to,
            message_text=message_text,
            message_id=message_id,
        )

        # Track stats
        if packet_type == "position":
            self._position_count += 1
        elif packet_type == "weather":
            self._weather_count += 1
        elif packet_type == "message":
            self._message_count += 1
        elif packet_type == "status":
            self._status_count += 1
        else:
            self._other_count += 1

        # Emit to local file
        output_path = self.emitter.emit_aprs(segment)

        # Publish to Synapse
        synapse_success = False
        if self.synapse_publisher.enabled:
            synapse_success = self.synapse_publisher.publish_aprs(segment)
            if synapse_success:
                self._synapse_published_count += 1

        # Notify segment callbacks
        for cb in self._segment_callbacks:
            try:
                cb(segment.model_dump(mode="json"))
            except Exception as e:
                logger.debug("Segment callback error", error=str(e))

        logger.info(
            "APRS packet processed",
            from_call=from_call,
            packet_type=packet_type,
            has_position=position is not None,
            has_weather=weather is not None,
            synapse=synapse_success if self.synapse_publisher.enabled else "disabled",
        )

        return segment

    def register_segment_callback(self, callback: Callable) -> None:
        """Register a callback invoked with each emitted segment dict."""
        self._segment_callbacks.append(callback)

    def get_stats(self) -> Dict[str, Any]:
        """Get pipeline statistics."""
        return {
            "node_id": self.node_id,
            "mode": "aprs",
            "packet_count": self._packet_count,
            "position_count": self._position_count,
            "weather_count": self._weather_count,
            "message_count": self._message_count,
            "status_count": self._status_count,
            "other_count": self._other_count,
            "duplicate_count": self._duplicate_count,
            "parse_error_count": self._parse_error_count,
            "synapse_published_count": self._synapse_published_count,
            "emitter": self.emitter.get_stats(),
            "synapse": self.synapse_publisher.get_stats(),
        }
