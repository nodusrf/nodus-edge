"""
TranscriptSegment.v1 Schema

The canonical output of the Nodus Edge. Each segment represents
a discrete radio transmission with optional transcription.

This schema is versioned (v1) to enable evolution while maintaining
compatibility with downstream consumers (Synapse).

Rich P25 metadata is preserved from SDRTrunk event logs including:
- Protocol and event type information
- Encryption status and phase
- LRRP location data
- ARS registration events
- Network/data channel information
"""

from datetime import datetime
from enum import Enum
from typing import Optional, List, Dict, Any, Literal, Union, Annotated
from pydantic import BaseModel, Field, ConfigDict, model_validator
from uuid import UUID, uuid4


class TalkgroupMetadata(BaseModel):
    """Enriched talkgroup metadata from ORION lookup."""
    tgid: int = Field(..., description="Decimal talkgroup ID")
    alpha_tag: str = Field(..., description="Short alpha tag (e.g., 'OFD Dispatch')")
    description: str = Field(..., description="Full description")
    tag: str = Field(..., description="Category tag (e.g., 'Fire Dispatch', 'Law Tac')")
    category: str = Field(..., description="Agency category (e.g., 'Omaha Fire', 'Douglas County')")
    mode: str = Field(..., description="Mode: D, DE, T, TE")
    encrypted: bool = Field(False, description="True if mode is DE or TE")
    service_type: str = Field("unknown", description="Service: law, fire, ems, multi, other, unknown")
    is_dispatch: bool = Field(False, description="True if this is a dispatch channel")
    is_tactical: bool = Field(False, description="True if this is a tactical channel")


class SiteMetadata(BaseModel):
    """Site/tower metadata from ORION lookup."""
    site_id: int = Field(..., description="Site ID")
    name: str = Field(..., description="Site name")
    county: str = Field(..., description="County")
    type: str = Field(..., description="Site type: site or simulcast")
    control_channels: List[float] = Field(default_factory=list, description="Control channel frequencies")


class RFChannel(BaseModel):
    """RF channel metadata."""
    frequency_hz: int = Field(..., description="Frequency in Hz")
    channel_number: Optional[str] = Field(None, description="Logical channel number")
    talkgroup_id: Optional[str] = Field(None, description="P25 talkgroup ID")
    talkgroup_name: Optional[str] = Field(None, description="Human-readable talkgroup name")
    talkgroup: Optional[TalkgroupMetadata] = Field(None, description="Enriched talkgroup metadata")
    system_name: Optional[str] = Field(None, description="Radio system name")
    channel_type: Optional[str] = Field(None, description="Channel type (e.g., T-Control)")
    timeslot: Optional[int] = Field(None, description="TDMA timeslot (1 or 2)")


class AudioMetadata(BaseModel):
    """Metadata about the source audio."""
    filename: str = Field(..., description="Original audio filename")
    filepath: Optional[str] = Field(None, description="Full path to audio file")
    duration_seconds: Optional[float] = Field(None, description="Audio duration in seconds")
    duration_ms: Optional[int] = Field(None, description="Duration in milliseconds from event log")
    file_size_bytes: Optional[int] = Field(None, description="File size in bytes")
    sample_rate_hz: int = Field(8000, description="Audio sample rate")
    format: str = Field("wav", description="Audio format")
    audio_data_base64: Optional[str] = Field(None, description="Base64-encoded MP3 for pipeline transport")


class P25Metadata(BaseModel):
    """P25-specific protocol metadata from event logs."""
    protocol: Optional[str] = Field(None, description="Protocol (e.g., APCO-25)")
    event_type: Optional[str] = Field(None, description="Event type (Voice Call, Data Call, etc.)")
    event_id: Optional[str] = Field(None, description="SDRTrunk event ID")
    phase: Optional[int] = Field(None, description="P25 Phase (1 or 2)")
    encrypted: bool = Field(False, description="Whether transmission was encrypted")
    is_emergency: bool = Field(False, description="Whether this is an emergency call (P25 emergency button)")
    priority: Optional[int] = Field(None, description="Call priority level")
    grant_type: Optional[str] = Field(None, description="Channel grant type")


class LRRPData(BaseModel):
    """Location Request/Response Protocol data."""
    request_type: Optional[str] = Field(None, description="LRRP request type")
    request_id: Optional[int] = Field(None, description="LRRP request ID")
    trigger_distance: Optional[int] = Field(None, description="Trigger distance in meters")
    requested_tokens: List[str] = Field(default_factory=list, description="Requested location tokens")
    latitude: Optional[float] = Field(None, description="Reported latitude")
    longitude: Optional[float] = Field(None, description="Reported longitude")
    altitude: Optional[float] = Field(None, description="Reported altitude")
    speed: Optional[float] = Field(None, description="Reported speed")
    heading: Optional[float] = Field(None, description="Reported heading")


class ARSData(BaseModel):
    """Automatic Registration Service data."""
    status: Optional[str] = Field(None, description="Registration status")
    refresh_minutes: Optional[int] = Field(None, description="Refresh interval in minutes")


class NetworkData(BaseModel):
    """Network/data channel information."""
    source_ip: Optional[str] = Field(None, description="Source IP address")
    destination_ip: Optional[str] = Field(None, description="Destination IP address")
    source_port: Optional[int] = Field(None, description="Source UDP port")
    destination_port: Optional[int] = Field(None, description="Destination UDP port")
    protocol: str = Field("UDP", description="Network protocol")


class CallEvent(BaseModel):
    """Individual call event from SDRTrunk event log."""
    timestamp: datetime = Field(..., description="Event timestamp")
    event_type: str = Field(..., description="Type of event")
    duration_ms: Optional[int] = Field(None, description="Call duration in ms")
    protocol: Optional[str] = Field(None, description="Protocol")
    source_radio_id: Optional[str] = Field(None, description="Source radio ID")
    source_ip: Optional[str] = Field(None, description="Source IP if network call")
    destination_talkgroup_id: Optional[str] = Field(None, description="Destination talkgroup")
    destination_talkgroup_name: Optional[str] = Field(None, description="Talkgroup name")
    destination_radio_id: Optional[str] = Field(None, description="Destination radio for unit-to-unit")
    channel_number: Optional[str] = Field(None, description="Channel number")
    frequency_hz: Optional[int] = Field(None, description="Frequency")
    timeslot: Optional[int] = Field(None, description="TDMA timeslot")
    phase: Optional[int] = Field(None, description="P25 phase")
    encrypted: bool = Field(False, description="Encrypted flag")
    is_emergency: bool = Field(False, description="Emergency call flag (P25 emergency button)")
    priority: Optional[int] = Field(None, description="Priority level")
    grant_type: Optional[str] = Field(None, description="Grant type")
    event_id: Optional[str] = Field(None, description="Event ID")
    raw_details: Optional[str] = Field(None, description="Raw details string")
    lrrp: Optional[LRRPData] = Field(None, description="LRRP data if present")
    ars: Optional[ARSData] = Field(None, description="ARS data if present")
    network: Optional[NetworkData] = Field(None, description="Network data if present")


class TranscriptionSegment(BaseModel):
    """Individual transcription segment with timing."""
    id: int = Field(..., description="Segment index")
    start: float = Field(..., description="Start time in seconds")
    end: float = Field(..., description="End time in seconds")
    text: str = Field(..., description="Transcribed text")
    confidence: Optional[float] = Field(None, ge=0.0, le=1.0, description="Segment confidence 0-1")
    no_speech_prob: Optional[float] = Field(None, description="Probability of no speech")
    compression_ratio: Optional[float] = Field(None, description="Text compression ratio (high = repetitive)")


class Transcription(BaseModel):
    """Transcription result from speech-to-text."""
    engine: str = Field("whisper", description="Transcription engine")
    model: str = Field(..., description="Model used for transcription")
    language: str = Field("en", description="Detected/specified language")
    raw_text: Optional[str] = Field(None, description="Original Whisper output (unprocessed)")
    text: str = Field(..., description="Synthesized text (radio codes interpreted)")
    confidence: Optional[float] = Field(None, ge=0.0, le=1.0, description="Overall confidence 0-1")
    duration_seconds: Optional[float] = Field(None, description="Audio duration processed")
    segments: List[TranscriptionSegment] = Field(default_factory=list, description="Timed segments")
    transcribed_at: datetime = Field(default_factory=datetime.utcnow, description="When transcription occurred")
    # Aggregate quality signals across segments (worst-case values)
    max_no_speech_prob: Optional[float] = Field(None, description="Highest no_speech_prob across segments")
    max_compression_ratio: Optional[float] = Field(None, description="Highest compression_ratio across segments")
    min_confidence: Optional[float] = Field(None, ge=0.0, le=1.0, description="Lowest segment confidence")


class TranscriptSegmentV1(BaseModel):
    """
    TranscriptSegment.v1 - The canonical Nodus Edge output schema.

    Each instance represents a single radio transmission captured by this Edge node.
    This is the contract between Edge and SYNAPSE domains.

    Rich P25 metadata is preserved including encryption status, LRRP location data,
    ARS registration events, and network/data channel information.
    """
    # Schema metadata
    schema_version: str = Field("1.0", description="Schema version for compatibility")
    modality: Literal["p25"] = Field("p25", description="Radio modality (p25 or fm)")

    # Identity
    segment_id: UUID = Field(default_factory=uuid4, description="Unique segment identifier")
    source_node_id: str = Field(..., description="Identifier of the Edge node that captured this")
    source_node_version: Optional[str] = Field(None, description="Software version of the source Edge node")
    metro: Optional[str] = Field(None, description="Metro area slug (e.g., 'phoenix', 'omaha') for pipeline isolation")

    # Temporal
    timestamp: datetime = Field(..., description="When the transmission was captured (UTC)")
    captured_at: datetime = Field(default_factory=datetime.utcnow, description="When this segment was created")

    # RF Channel
    rf_channel: RFChannel = Field(..., description="RF channel metadata")

    # Source identifiers
    source_radio_id: Optional[str] = Field(None, description="Transmitting radio ID (FROM)")
    destination_radio_id: Optional[str] = Field(None, description="Destination radio ID (if unit-to-unit)")

    # Audio
    audio: AudioMetadata = Field(..., description="Source audio metadata")

    # P25 Protocol Metadata
    p25: Optional[P25Metadata] = Field(None, description="P25-specific protocol metadata")

    # Rich event data from SDRTrunk logs
    call_events: List[CallEvent] = Field(default_factory=list, description="Associated call events from log")

    # Location data (from LRRP if available)
    lrrp: Optional[LRRPData] = Field(None, description="LRRP location data if present")

    # ARS data
    ars: Optional[ARSData] = Field(None, description="ARS registration data if present")

    # Network data (for data calls)
    network: Optional[NetworkData] = Field(None, description="Network/data channel info if present")

    # Transcription (optional - depends on Edge configuration)
    transcription: Optional[Transcription] = Field(None, description="Speech-to-text result if enabled")

    # Confidence (overall segment confidence for triplex verification)
    confidence: float = Field(
        1.0,
        ge=0.0,
        le=1.0,
        description="Overall segment confidence (signal quality, transcription confidence)"
    )

    # Source file tracking (for debugging/audit)
    source_files: Dict[str, str] = Field(
        default_factory=dict,
        description="Source file paths (audio, event_log, etc.)"
    )

    @model_validator(mode='after')
    def require_transcription(self):
        is_encrypted = self.p25 and self.p25.encrypted
        if not is_encrypted and (not self.transcription or not self.transcription.text):
            raise ValueError("P25 segment requires non-empty transcription")
        return self

    class Config:
        json_schema_extra = {
            "example": {
                "schema_version": "1.0",
                "segment_id": "550e8400-e29b-41d4-a716-446655440000",
                "source_node_id": "nodus-edge-node-01",
                "timestamp": "2025-12-27T18:50:46Z",
                "rf_channel": {
                    "frequency_hz": 853950000,
                    "talkgroup_id": "333",
                    "talkgroup_name": "DC MAT 4",
                    "system_name": "ORION_Douglas",
                    "timeslot": 1
                },
                "source_radio_id": "7000",
                "p25": {
                    "protocol": "APCO-25",
                    "event_type": "Voice Call",
                    "phase": 2,
                    "encrypted": False,
                    "priority": 4,
                    "grant_type": "CHANNEL GRANT"
                },
                "audio": {
                    "filename": "20251227_185046_call.wav",
                    "duration_seconds": 4.5,
                    "duration_ms": 4500,
                    "file_size_bytes": 72000
                },
                "transcription": {
                    "engine": "whisper",
                    "model": "medium",
                    "text": "Unit 42, respond to 123 Main Street",
                    "confidence": 0.92
                },
                "confidence": 0.92
            }
        }


class SegmentBatch(BaseModel):
    """Batch of segments for efficient forwarding."""
    batch_id: UUID = Field(default_factory=uuid4)
    source_node_id: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    segments: List[TranscriptSegmentV1]
    segment_count: int = Field(0)

    def __init__(self, **data):
        super().__init__(**data)
        self.segment_count = len(self.segments)


# =============================================================================
# FM Ham Radio Schema
# =============================================================================

class FMRFChannel(BaseModel):
    """RF channel metadata for FM ham radio."""
    model_config = ConfigDict(extra='ignore')

    frequency_hz: int = Field(..., description="Frequency in Hz")
    signal_strength_db: Optional[float] = Field(None, description="Signal strength in dB (if available)")
    ctcss_tone_hz: Optional[float] = Field(None, description="CTCSS/PL tone frequency if detected")
    dcs_code: Optional[str] = Field(None, description="DCS code if detected")
    bandwidth_khz: float = Field(12.5, description="FM channel bandwidth (typically 12.5 or 25 kHz)")
    repeater_callsign: Optional[str] = Field(None, description="Repeater trustee callsign (from repeater DB lookup, not a participant)")


class FMTranscriptSegmentV1(BaseModel):
    """
    FMTranscriptSegment.v1 - FM ham radio transcript schema.

    Simplified metadata compared to P25 - no talkgroups, radio IDs, or protocol data.
    Ham operators identify verbally via callsigns which are extracted from transcripts.
    """
    model_config = ConfigDict(
        extra='ignore',
        json_schema_extra={
            "example": {
                "schema_version": "1.0",
                "modality": "fm",
                "segment_id": "550e8400-e29b-41d4-a716-446655440001",
                "source_node_id": "nodus-edge-fm-01",
                "timestamp": "2026-01-25T14:30:00Z",
                "rf_channel": {
                    "frequency_hz": 146520000,
                    "signal_strength_db": -65.0,
                    "ctcss_tone_hz": 100.0,
                    "bandwidth_khz": 12.5
                },
                "audio": {
                    "filename": "20260125_143000_146520000Hz.wav",
                    "duration_seconds": 15.2,
                    "file_size_bytes": 486400
                },
                "transcription": {
                    "engine": "whisper",
                    "model": "medium",
                    "text": "This is W1ABC calling CQ on 2 meters",
                    "confidence": 0.95
                },
                "detected_callsigns": ["W1ABC"],
                "confidence": 0.95
            }
        }
    )

    # Schema metadata
    schema_version: Literal["1.0"] = Field("1.0", description="Schema version")
    modality: Literal["fm"] = Field("fm", description="Radio modality")

    # Identity
    segment_id: UUID = Field(default_factory=uuid4, description="Unique segment identifier")
    source_node_id: str = Field(..., description="Identifier of the Edge node that captured this")
    source_node_version: Optional[str] = Field(None, description="Software version of the source Edge node")
    metro: Optional[str] = Field(None, description="Metro area slug (e.g., 'phoenix', 'omaha') for pipeline isolation")

    # Temporal
    timestamp: datetime = Field(..., description="When the transmission was captured (UTC)")
    captured_at: datetime = Field(default_factory=datetime.utcnow, description="When this segment was created")

    # RF Channel (FM-specific)
    rf_channel: FMRFChannel = Field(..., description="FM RF channel metadata")

    # Audio
    audio: AudioMetadata = Field(..., description="Source audio metadata")

    # Transcription
    transcription: Optional[Transcription] = Field(None, description="Speech-to-text result")

    # Callsigns extracted from transcript (FM-specific)
    detected_callsigns: List[str] = Field(
        default_factory=list,
        description="Ham radio callsigns extracted from transcript (e.g., W1ABC, KD9XYZ)"
    )

    # Signal type (voice, morse, voice+morse)
    signal_type: Optional[str] = Field(
        None,
        description="Detected signal type: 'voice', 'morse', 'voice+morse', or None (legacy/unknown)"
    )

    # Confidence
    confidence: float = Field(
        1.0,
        ge=0.0,
        le=1.0,
        description="Overall segment confidence"
    )

    # Source file tracking
    source_files: Dict[str, str] = Field(
        default_factory=dict,
        description="Source file paths (audio)"
    )

    @model_validator(mode='after')
    def require_transcription(self):
        if not self.transcription or not self.transcription.text:
            raise ValueError("FM segment requires non-empty transcription")
        return self


# =============================================================================
# HF Amateur Radio Schema
# =============================================================================

class HFBand(str, Enum):
    """ITU amateur HF bands."""
    BAND_160M = "160m"
    BAND_80M = "80m"
    BAND_60M = "60m"
    BAND_40M = "40m"
    BAND_30M = "30m"
    BAND_20M = "20m"
    BAND_17M = "17m"
    BAND_15M = "15m"
    BAND_12M = "12m"
    BAND_10M = "10m"
    BAND_6M = "6m"


class HFMode(str, Enum):
    """Common HF operating modes."""
    SSB = "ssb"
    CW = "cw"
    FT8 = "ft8"
    FT4 = "ft4"
    JS8 = "js8"
    AM = "am"
    FM = "fm"
    RTTY = "rtty"
    PSK31 = "psk31"


# Band edge frequencies (lower bound Hz) for frequency-to-band derivation
HF_BAND_EDGES = {
    HFBand.BAND_160M: (1_800_000, 2_000_000),
    HFBand.BAND_80M: (3_500_000, 4_000_000),
    HFBand.BAND_60M: (5_330_500, 5_406_400),
    HFBand.BAND_40M: (7_000_000, 7_300_000),
    HFBand.BAND_30M: (10_100_000, 10_150_000),
    HFBand.BAND_20M: (14_000_000, 14_350_000),
    HFBand.BAND_17M: (18_068_000, 18_168_000),
    HFBand.BAND_15M: (21_000_000, 21_450_000),
    HFBand.BAND_12M: (24_890_000, 24_990_000),
    HFBand.BAND_10M: (28_000_000, 29_700_000),
    HFBand.BAND_6M: (50_000_000, 54_000_000),
}


def frequency_to_band(frequency_hz: int) -> Optional[HFBand]:
    """Derive amateur band from frequency in Hz."""
    for band, (lower, upper) in HF_BAND_EDGES.items():
        if lower <= frequency_hz <= upper:
            return band
    return None


class HFRFChannel(BaseModel):
    """RF channel metadata for HF amateur radio."""
    model_config = ConfigDict(extra='ignore')

    frequency_hz: int = Field(..., description="Dial frequency in Hz")
    band: Optional[HFBand] = Field(None, description="Amateur band (derived from frequency)")
    mode: Optional[HFMode] = Field(None, description="Operating mode from CAT")
    sideband: Optional[Literal["usb", "lsb"]] = Field(None, description="Sideband for SSB")
    bandwidth_hz: Optional[int] = Field(None, description="Passband width from CAT")
    s_meter: Optional[int] = Field(None, description="S-meter reading (0-9 for S0-S9, 10+ for S9+10 etc.)")
    s_meter_dbm: Optional[float] = Field(None, description="S-meter in dBm if available")
    power_watts: Optional[float] = Field(None, description="TX power setting")


class HFTranscriptSegmentV1(BaseModel):
    """
    HFTranscriptSegment.v1 - HF amateur radio transcript schema.

    HF is global (no metro). station_callsign identifies the source station
    for pipeline isolation. No repeaters — all simplex/direct.
    """
    model_config = ConfigDict(
        extra='ignore',
        json_schema_extra={
            "example": {
                "schema_version": "1.0",
                "modality": "hf",
                "segment_id": "550e8400-e29b-41d4-a716-446655440002",
                "source_node_id": "nodus-edge-hf-01",
                "station_callsign": "W1AW",
                "timestamp": "2026-03-03T20:00:00Z",
                "rf_channel": {
                    "frequency_hz": 14074000,
                    "band": "20m",
                    "mode": "ft8",
                },
                "audio": {
                    "filename": "20260303_200000_14074000Hz_ft8.wav",
                    "duration_seconds": 15.0,
                    "file_size_bytes": 1440000,
                },
                "detected_callsigns": ["W1AW", "N0CALL"],
                "confidence": 0.85,
            }
        }
    )

    # Schema metadata
    schema_version: Literal["1.0"] = Field("1.0", description="Schema version")
    modality: Literal["hf"] = Field("hf", description="Radio modality")

    # Identity
    segment_id: UUID = Field(default_factory=uuid4, description="Unique segment identifier")
    source_node_id: str = Field(..., description="Identifier of the Edge node that captured this")
    source_node_version: Optional[str] = Field(None, description="Software version of the source Edge node")
    station_callsign: Optional[str] = Field(None, description="Station callsign (replaces metro for HF)")

    # Temporal
    timestamp: datetime = Field(..., description="When the transmission was captured (UTC)")
    captured_at: datetime = Field(default_factory=datetime.utcnow, description="When this segment was created")

    # RF Channel (HF-specific)
    rf_channel: HFRFChannel = Field(..., description="HF RF channel metadata")

    # Audio
    audio: AudioMetadata = Field(..., description="Source audio metadata")

    # Transcription
    transcription: Optional[Transcription] = Field(None, description="Speech-to-text result")

    # Callsigns extracted from transcript
    detected_callsigns: List[str] = Field(
        default_factory=list,
        description="Ham radio callsigns extracted from transcript"
    )

    # Signal type (voice, cw, digital)
    signal_type: Optional[str] = Field(
        None,
        description="Detected signal type: 'voice', 'cw', 'digital', or None"
    )

    # Confidence
    confidence: float = Field(
        1.0,
        ge=0.0,
        le=1.0,
        description="Overall segment confidence"
    )

    # Source file tracking
    source_files: Dict[str, str] = Field(
        default_factory=dict,
        description="Source file paths (audio)"
    )

    @model_validator(mode='after')
    def require_transcription(self):
        if not self.transcription or not self.transcription.text:
            raise ValueError("HF segment requires non-empty transcription")
        return self


# =============================================================================
# APRS Packet Schema
# =============================================================================

class APRSPosition(BaseModel):
    """APRS position data."""
    model_config = ConfigDict(extra='ignore')

    latitude: float = Field(..., description="Latitude in decimal degrees")
    longitude: float = Field(..., description="Longitude in decimal degrees")
    altitude_m: Optional[float] = Field(None, description="Altitude in meters")
    speed_kmh: Optional[float] = Field(None, description="Speed in km/h")
    course: Optional[float] = Field(None, description="Course in degrees")
    symbol: Optional[str] = Field(None, description="APRS symbol character")
    symbol_table: Optional[str] = Field(None, description="APRS symbol table ('/' or '\\')")
    posambiguity: Optional[int] = Field(None, description="Position ambiguity 0-4 (0=exact, 4=~60nm)")


class APRSWeather(BaseModel):
    """APRS weather station data."""
    model_config = ConfigDict(extra='ignore')

    temperature_f: Optional[float] = Field(None, description="Temperature in Fahrenheit")
    humidity_pct: Optional[float] = Field(None, description="Humidity percentage")
    pressure_mbar: Optional[float] = Field(None, description="Barometric pressure in mbar")
    wind_speed_mph: Optional[float] = Field(None, description="Wind speed in mph")
    wind_direction: Optional[float] = Field(None, description="Wind direction in degrees")
    wind_gust_mph: Optional[float] = Field(None, description="Wind gust in mph")
    rain_1h_inches: Optional[float] = Field(None, description="Rain in last hour (inches)")
    rain_24h_inches: Optional[float] = Field(None, description="Rain in last 24h (inches)")
    rain_since_midnight_inches: Optional[float] = Field(None, description="Rain since midnight (inches)")


class APRSPacketSegmentV1(BaseModel):
    """
    APRSPacketSegment.v1 - Decoded APRS packet schema.

    Each instance represents a single APRS packet decoded from RF by Direwolf.
    No audio transcription — APRS is digital packet data.
    """
    model_config = ConfigDict(
        extra='ignore',
        json_schema_extra={
            "example": {
                "schema_version": "1.0",
                "modality": "aprs",
                "segment_id": "550e8400-e29b-41d4-a716-446655440003",
                "source_node_id": "nodus-edge-aprs-01",
                "timestamp": "2026-03-05T15:30:00Z",
                "from_callsign": "W1AW-9",
                "to_callsign": "APRS",
                "path": ["WIDE1-1", "WIDE2-1"],
                "packet_type": "position",
                "position": {
                    "latitude": 41.7144,
                    "longitude": -72.7272,
                    "speed_kmh": 45.0,
                    "course": 180.0,
                    "symbol": ">",
                    "symbol_table": "/",
                },
                "comment": "Nodus APRS IGate",
            }
        }
    )

    # Schema metadata
    schema_version: Literal["1.0"] = Field("1.0", description="Schema version")
    modality: Literal["aprs"] = Field("aprs", description="Radio modality")

    # Identity
    segment_id: UUID = Field(default_factory=uuid4, description="Unique segment identifier")
    source_node_id: str = Field(..., description="Identifier of the Edge node that decoded this")
    source_node_version: Optional[str] = Field(None, description="Software version of the source Edge node")
    metro: Optional[str] = Field(None, description="Metro area slug for pipeline isolation")

    # Temporal
    timestamp: datetime = Field(..., description="When the packet was received (UTC)")
    captured_at: datetime = Field(default_factory=datetime.utcnow, description="When this segment was created")

    # APRS packet fields
    from_callsign: str = Field(..., description="Source callsign (with SSID, e.g. W1AW-9)")
    to_callsign: str = Field(..., description="Destination/tocall field")
    path: List[str] = Field(default_factory=list, description="Digipeater path")
    packet_type: str = Field(..., description="Packet type: position, weather, status, message, object, item, telemetry, unknown")
    packet_format: Optional[str] = Field(None, description="Encoding format: uncompressed, compressed, mic-e, object, wx, message")
    raw_packet: str = Field(..., description="Raw APRS packet string")
    raw_timestamp: Optional[str] = Field(None, description="APRS timestamp from the packet (e.g. 092345z)")

    # Capabilities
    is_message_capable: Optional[bool] = Field(None, description="Station can receive APRS messages")

    # Position (if available)
    position: Optional[APRSPosition] = Field(None, description="Position data")

    # Weather (if weather packet)
    weather: Optional[APRSWeather] = Field(None, description="Weather station data")

    # Comment/status text
    comment: Optional[str] = Field(None, description="APRS comment or status text")

    # Object/item fields
    object_name: Optional[str] = Field(None, description="Object/item name (e.g. SHELTER, HAZMAT)")
    is_alive: Optional[bool] = Field(None, description="Object active (True) or killed/removed (False)")

    # Message fields (if message packet)
    message_to: Optional[str] = Field(None, description="Message addressee callsign")
    message_text: Optional[str] = Field(None, description="Message text content")
    message_id: Optional[str] = Field(None, description="Message sequence number")

    # Reserved for future use
    reserved_1: Optional[str] = Field(None, description="Reserved")
    reserved_2: Optional[str] = Field(None, description="Reserved")
    reserved_3: Optional[str] = Field(None, description="Reserved")
    reserved_4: Optional[str] = Field(None, description="Reserved")

    # Source file tracking
    source_files: Dict[str, str] = Field(
        default_factory=dict,
        description="Source file paths"
    )


# =============================================================================
# Edge-Thread / Keyword Alert Schema
# =============================================================================

class KeywordAlert(BaseModel):
    """Alert generated when urgent keywords are detected in a segment or thread."""
    model_config = ConfigDict(extra='ignore')

    segment_id: Optional[str] = Field(None, description="Triggering segment ID")
    thread_id: Optional[str] = Field(None, description="Associated thread ID (if any)")
    frequency_hz: int = Field(..., description="Frequency where keyword was detected")
    tier: str = Field(..., description="Keyword tier: urgent, notable, informational")
    label: str = Field(..., description="Keyword label (e.g., 'emergency')")
    matched_text: str = Field(..., description="The text that matched the keyword pattern")
    text_excerpt: str = Field("", description="Truncated transcript excerpt (max 200 chars)")
    callsigns: List[str] = Field(default_factory=list, description="Callsigns from the segment")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="When the alert was generated")


# Discriminated union for downstream consumers
TranscriptSegment = Annotated[
    Union[TranscriptSegmentV1, FMTranscriptSegmentV1, HFTranscriptSegmentV1, APRSPacketSegmentV1],
    Field(discriminator="modality")
]
