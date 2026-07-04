"""
Configuration for Nodus Edge.

Settings can be provided via environment variables (prefixed with NODUS_EDGE_)
or via a .env file.
"""

import os
import re
import socket
from pathlib import Path
from typing import Optional, List, Literal
from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def get_default_node_id() -> str:
    """Generate a default node ID from hostname."""
    hostname = socket.gethostname()
    return f"nodus-edge-{hostname}"


class Settings(BaseSettings):
    """Nodus Edge configuration."""

    model_config = SettingsConfigDict(
        env_prefix="NODUS_EDGE_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Node identity
    node_id: str = get_default_node_id()
    metro: str = ""  # Metro area slug (e.g., "phoenix", "omaha")
    callsign: str = ""  # Operator callsign (empty = anonymous node)

    # Radio mode: p25 (SDRTrunk/Trunk Recorder), fm (ham radio), gmrs (GMRS/FRS),
    # hf (HF amateur), or aprs (packet). gmrs uses the same FM scanner pipeline.
    mode: Literal["p25", "fm", "gmrs", "hf", "aprs"] = "p25"

    # Canary boot: health server only. No hardware access, no REM check-in,
    # no Synapse publish, no heartbeat. Used by the OTA updater to verify a
    # new image boots on this host before swapping production containers.
    canary_mode: bool = False

    # P25 source: sdrtrunk (filename-based) or trunk-recorder (JSON-based)
    p25_source: Literal["sdrtrunk", "trunk-recorder"] = "sdrtrunk"

    # SDRTrunk directories to watch (P25/sdrtrunk mode)
    recordings_dir: str = "/home/nodus-edge/SDRTrunk/recordings"
    event_logs_dir: str = "/home/nodus-edge/SDRTrunk/event_logs"

    # Trunk Recorder settings (P25/trunk-recorder mode)
    tr_capture_dir: str = "/var/lib/trunk-recorder/recordings"
    tr_settle_time_seconds: float = 2.0

    # Output directory for TranscriptSegment.v1 JSON files
    output_dir: str = "/home/nodus-edge/nodus-edge/output"

    # Synapse endpoint (optional - if set, segments are POSTed to Synapse)
    synapse_endpoint: Optional[str] = None
    synapse_timeout_seconds: int = 10
    synapse_auth_token: Optional[str] = None  # Bearer token for remote Synapse (via Gateway)

    # Diagnostics endpoint for heartbeat emission
    diagnostics_endpoint: Optional[str] = None

    # REM endpoint for device check-in and compliance token
    rem_endpoint: Optional[str] = None

    # Gateway endpoint for coverage coordination, OTA, etc.
    gateway_url: Optional[str] = None

    @model_validator(mode='after')
    def _validate_all_fields(self) -> 'Settings':
        """Comprehensive validation of all settings at startup.

        Catches bad values from .env files early with clear error messages
        instead of letting them cause confusing crashes deep in the code.
        """
        errors: list[str] = []

        # Positive numeric fields (timeouts, intervals, sizes, thresholds)
        positive_fields = {
            "fm_dwell_seconds": self.fm_dwell_seconds,
            "fm_idle_timeout_seconds": self.fm_idle_timeout_seconds,
            "fm_segment_max_seconds": self.fm_segment_max_seconds,
            "fm_segment_min_seconds": self.fm_segment_min_seconds,
            "fm_watchdog_timeout_seconds": self.fm_watchdog_timeout_seconds,
            "fm_segment_watchdog_timeout_seconds": self.fm_segment_watchdog_timeout_seconds,
            "fm_max_freq_dwell_seconds": self.fm_max_freq_dwell_seconds,
            "fm_whisper_retry_interval_seconds": self.fm_whisper_retry_interval_seconds,
            "fm_airband_fft_size": self.fm_airband_fft_size,
            "fm_airband_min_mp3_bytes": self.fm_airband_min_mp3_bytes,
            "fm_audio_retention_hours": self.fm_audio_retention_hours,
            "whisper_timeout_seconds": self.whisper_timeout_seconds,
            "whisper_queue_maxsize": self.whisper_queue_maxsize,
            "synapse_timeout_seconds": self.synapse_timeout_seconds,
            "poll_interval_seconds": self.poll_interval_seconds,
            "batch_size": self.batch_size,
            "max_retries": self.max_retries,
            "dedup_cache_size": self.dedup_cache_size,
            "dedup_ttl_seconds": self.dedup_ttl_seconds,
            "hf_audio_sample_rate": self.hf_audio_sample_rate,
            "hf_segment_max_seconds": self.hf_segment_max_seconds,
            "hf_segment_min_seconds": self.hf_segment_min_seconds,
            "aprs_sample_rate": self.aprs_sample_rate,
            "connectivity_probe_interval_sec": self.connectivity_probe_interval_sec,
            "connectivity_fail_threshold": self.connectivity_fail_threshold,
            "dashboard_max_segments": self.dashboard_max_segments,
            "thread_gap_seconds": self.thread_gap_seconds,
            "thread_prune_hours": self.thread_prune_hours,
        }
        for name, val in positive_fields.items():
            if val <= 0:
                errors.append(f"{name}={val} must be positive.")

        # Non-negative numeric fields
        non_negative_fields = {
            "fm_squelch_threshold": self.fm_squelch_threshold,
            "fm_rtl_device_index": self.fm_rtl_device_index,
            "fm_airband_center_freq_hz": self.fm_airband_center_freq_hz,
            "fm_airband_squelch_snr_db": self.fm_airband_squelch_snr_db,
            "aprs_device_index": self.aprs_device_index,
            "quality_score_threshold": self.quality_score_threshold,
            "fm_min_confidence": self.fm_min_confidence,
            "fm_morse_min_snr_db": self.fm_morse_min_snr_db,
            "fm_morse_min_confidence": self.fm_morse_min_confidence,
        }
        for name, val in non_negative_fields.items():
            if val < 0:
                errors.append(f"{name}={val} must be non-negative.")

        # Range checks
        if not (0 <= self.fm_squelch_threshold <= 100):
            errors.append(f"fm_squelch_threshold={self.fm_squelch_threshold} out of range (0-100).")
        if not (0.0 <= self.quality_score_threshold <= 1.0):
            errors.append(f"quality_score_threshold={self.quality_score_threshold} out of range (0.0-1.0).")
        if not (0.0 <= self.fm_min_confidence <= 1.0):
            errors.append(f"fm_min_confidence={self.fm_min_confidence} out of range (0.0-1.0).")
        if not (0.0 <= self.fm_morse_min_confidence <= 1.0):
            errors.append(f"fm_morse_min_confidence={self.fm_morse_min_confidence} out of range (0.0-1.0).")
        if self.whisper_repetition_penalty < 1.0:
            errors.append(f"whisper_repetition_penalty={self.whisper_repetition_penalty} must be >= 1.0.")
        if self.fm_morse_tone_range_low_hz >= self.fm_morse_tone_range_high_hz:
            errors.append(
                f"fm_morse_tone_range_low_hz ({self.fm_morse_tone_range_low_hz}) "
                f"must be less than fm_morse_tone_range_high_hz ({self.fm_morse_tone_range_high_hz})."
            )

        # URL validation (when set, must look like a URL)
        url_fields = {
            "synapse_endpoint": self.synapse_endpoint,
            "diagnostics_endpoint": self.diagnostics_endpoint,
            "rem_endpoint": self.rem_endpoint,
            "gateway_url": self.gateway_url,
            "whisper_api_url": self.whisper_api_url,
        }
        for name, val in url_fields.items():
            if val and not val.startswith(("http://", "https://")):
                errors.append(f"{name}='{val}' must start with http:// or https://.")

        if errors:
            msg = "Configuration validation failed. Check your .env file:\n"
            for e in errors:
                msg += f"  - NODUS_EDGE_{e.split('=')[0].upper()}: {e}\n"
            raise ValueError(msg)

        return self

    @model_validator(mode='after')
    def _derive_from_server(self) -> 'Settings':
        """Derive service endpoints from NODUSNET_SERVER/NODUSNET_TOKEN when not explicitly set."""
        server = os.environ.get("NODUSNET_SERVER", "").rstrip("/")
        token = os.environ.get("NODUSNET_TOKEN", "")

        if server:
            if not self.synapse_endpoint:
                self.synapse_endpoint = server
            if not self.diagnostics_endpoint:
                self.diagnostics_endpoint = server
            if not self.rem_endpoint:
                self.rem_endpoint = server
            if not self.gateway_url:
                self.gateway_url = server

        if token:
            if not self.synapse_auth_token:
                self.synapse_auth_token = token
            if not self.whisper_auth_token:
                self.whisper_auth_token = token

        return self

    # File patterns (P25 mode)
    recording_pattern: str = "*_TO_*_FROM_*.wav"  # Individual call recordings
    event_log_pattern: str = "*_call_events.log"

    # FM mode settings (ignored in P25 mode)
    fm_frequencies: List[int] = []  # Frequencies to scan in Hz (legacy - use core + candidates)
    fm_dwell_seconds: float = 1.0  # Time per quiet frequency before moving on
    fm_idle_timeout_seconds: float = 5.0  # Silence after activity before moving on
    fm_squelch_threshold: int = 15  # rtl_fm squelch level (0-100), low to prefer noise over missed segments

    # Adaptive scanning - core frequencies always scanned
    fm_core_frequencies: List[int] = []  # Top 10 always in rotation

    # Adaptive scanning - candidate frequencies (promoted when active)
    fm_candidate_frequencies: List[int] = []  # Earn their way into rotation

    # Adaptive scanning - promotion/demotion rules
    fm_promotion_threshold_db: float = 10.0  # Signal must be X dB above noise floor
    fm_demotion_timeout_minutes: int = 60  # Drop from rotation after 1 hour idle
    fm_max_active_frequencies: int = 20  # Cap active list (core + promoted)

    # Wideband monitor settings
    fm_wideband_device_index: int = 1  # Separate RTL-SDR for wideband (0 = narrowband scanner)
    fm_wideband_center_hz: int = 146000000  # Center frequency for 2m monitoring
    fm_wideband_sample_rate: int = 2400000  # 2.4 MHz bandwidth
    fm_wideband_fft_size: int = 4096  # FFT bins
    fm_wideband_scan_interval: float = 0.5  # Seconds between spectrum scans
    fm_silence_timeout_seconds: float = 2.0  # Silence duration before moving to next freq
    fm_sample_rate_hz: int = 48000  # rtl_fm sample rate
    fm_output_sample_rate_hz: int = 16000  # Output sample rate for Whisper
    fm_capture_dir: str = "/home/nodus-edge/fm_capture"  # Directory for FM recordings
    fm_segment_max_seconds: int = 60  # Maximum segment duration
    fm_segment_min_seconds: float = 1.0  # Minimum segment duration (sub-1s cannot contain speech)
    fm_extract_callsigns: bool = True  # Extract ham callsigns from transcripts
    fm_enhanced_callsign_extraction: bool = True  # Informal phonetics + split-callsign patterns
    fm_hallucination_filter_enabled: bool = True  # Filter known Whisper hallucinations
    fm_beacon_filter_enabled: bool = False  # Filter automated repeater beacons (voice IDs)
    fm_kerchunk_filter_enabled: bool = True  # Filter courtesy tone kerchunks decoded as single-letter Morse
    p25_hallucination_filter_enabled: bool = True  # Filter known Whisper hallucinations (P25)
    fm_rtl_device_index: int = 0  # RTL-SDR device index
    fm_gain: str = "40"  # RTL-SDR gain ("auto" or numeric value)

    @field_validator("fm_gain", "aprs_gain", mode="after")
    @classmethod
    def _validate_gain(cls, v: str) -> str:
        if v.lower() == "auto":
            return "auto"
        try:
            val = float(v)
        except ValueError:
            raise ValueError(
                f"Invalid gain value '{v}'. Must be 'auto' or a number (e.g., '40'). "
                f"Check your .env file for typos."
            )
        if val < 0 or val > 100:
            raise ValueError(
                f"Gain value {val} out of range. Expected 0-100 or 'auto'."
            )
        return v

    @field_validator(
        "fm_core_frequencies", "fm_candidate_frequencies", "fm_frequencies",
        mode="after",
    )
    @classmethod
    def _validate_frequencies(cls, v: List[int]) -> List[int]:
        for freq in v:
            if freq < 1_000_000 or freq > 30_000_000_000:
                raise ValueError(
                    f"Frequency {freq} Hz out of range. "
                    f"Expected Hz values between 1 MHz and 30 GHz (e.g., 146940000 for 146.94 MHz)."
                )
        return v

    @field_validator("callsign", mode="after")
    @classmethod
    def _validate_callsign(cls, v: str) -> str:
        if not v:
            return v
        # Amateur radio: 1-2 letters, digit, 1-3 letters (e.g., W1AW, KK7QPZ)
        # GMRS: WR + 2 letters + 3 digits (e.g., WREF123)
        if not (re.match(r"^[A-Za-z]{1,2}\d[A-Za-z]{1,3}$", v)
                or re.match(r"^WR[A-Za-z]{2}\d{3}$", v)):
            raise ValueError(
                f"Invalid callsign format '{v}'. "
                f"Expected amateur (e.g., 'W1AW', 'KK7QPZ') "
                f"or GMRS (e.g., 'WRNB456') callsign."
            )
        return v.upper()

    @field_validator("log_level", mode="after")
    @classmethod
    def _validate_log_level(cls, v: str) -> str:
        valid = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v.upper() not in valid:
            raise ValueError(
                f"Invalid log_level '{v}'. Must be one of: {', '.join(sorted(valid))}."
            )
        return v

    @field_validator("dashboard_port", mode="after")
    @classmethod
    def _validate_port(cls, v: int) -> int:
        if v < 1 or v > 65535:
            raise ValueError(f"Port {v} out of range. Expected 1-65535.")
        return v

    # RTLSDR-Airband multichannel scanner
    fm_scanner_backend: Literal["rtl_fm", "airband"] = "rtl_fm"  # "airband" for simultaneous multi-channel
    fm_airband_binary: str = "rtl_airband"  # Path to RTLSDR-Airband binary
    fm_airband_fft_size: int = 4096  # FFT size for channelization
    fm_airband_center_freq_hz: int = 0  # Center freq override (0 = auto-calculate from freq list)
    fm_airband_squelch_snr_db: float = 6.0  # Per-channel squelch SNR threshold in dB (0 = auto)
    fm_airband_min_mp3_bytes: int = 4000  # Discard MP3s smaller than this (noise filter)
    fm_airband_keep_mp3: bool = True  # Keep source MP3 for pipeline audio embedding
    fm_audio_retention_hours: int = 48  # Delete WAV/MP3 older than this (edge buffer)

    # Audio denoising (spectral gating, runs after normalization, before Whisper)
    audio_denoise_enabled: bool = False  # Enable noisereduce spectral gating
    audio_denoise_prop_decrease: float = 0.75  # Noise reduction strength (0.0-1.0)

    # Morse code detection
    fm_morse_detection_enabled: bool = True  # Enable CW Morse code detection/decoding
    fm_morse_min_snr_db: float = 10.0  # Minimum tone SNR (dB) to consider Morse present
    fm_morse_tone_range_low_hz: int = 400  # Low end of CW tone search range
    fm_morse_tone_range_high_hz: int = 1200  # High end of CW tone search range
    fm_morse_min_confidence: float = 0.5  # Reject morse decodes below this confidence

    # FFT spillover detection (in airband scanner, pre-Whisper)
    fm_spillover_detection_enabled: bool = False  # Detect cross-frequency FFT spillover duplicates (disabled pending better winner selection)
    fm_spillover_buffer_seconds: float = 1.0  # Buffer delay to collect same-timestamp files
    fm_spillover_duration_tolerance_seconds: float = 0.5  # Duration match tolerance for spillover

    # RF bleedover detection (pre-Whisper signal strength gate)
    fm_bleedover_detection_enabled: bool = True  # Detect front-end overload from nearby transmitters
    fm_bleedover_threshold_db: float = -15.0  # Signal strength above this is suspected bleedover (normal: -50 to -70 dB)
    fm_bleedover_action: Literal["drop", "flag"] = "flag"  # "flag" adds metadata but still emits, "drop" discards

    # FM pipeline hardening
    fm_watchdog_timeout_seconds: float = 300.0  # Restart rtl_fm if zero bytes for 5 minutes
    fm_segment_watchdog_timeout_seconds: float = 1800.0  # USB reset if no segments for 30 minutes
    fm_max_freq_dwell_seconds: float = 180.0  # Force frequency rotation after 3 minutes
    fm_whisper_retry_interval_seconds: float = 120.0  # Retry Whisper connectivity when down

    # Quality gate (replaces hallucination phrase list)
    quality_score_threshold: float = 0.35
    quality_gate_enabled: bool = True  # Shadow mode: logs quality score but doesn't filter
    quality_gate_primary: bool = True  # Quality gate makes filtering decisions (legacy phrase list is fallback)

    # FM confidence floor — hard cutoff on Whisper's min segment confidence
    # Calibrated against 48kbps CBR audio + small model (2026-03-23).
    # Old threshold (0.45) was tuned for 16kbps VBR base model and rejected real speech.
    fm_min_confidence: float = 0.30

    # Tail loop truncation — detect and truncate mid-segment Whisper loops
    # Whisper sometimes transcribes valid speech then enters a loop repeating
    # a phrase dozens of times. This preserves the valid prefix.
    fm_tail_loop_truncation_enabled: bool = True

    # Whisper transcription
    transcription_enabled: bool = True
    whisper_api_url: str = "http://whisper:8000"
    whisper_timeout_seconds: int = 300
    whisper_queue_maxsize: int = 50  # Bounded work queue to prevent memory growth
    whisper_language: str = "en"
    whisper_vad_filter: bool = True
    whisper_auth_token: Optional[str] = None  # Bearer token for gateway whisper proxy (arm64 nodes)

    # Whisper anti-hallucination parameters
    whisper_condition_on_previous_text: bool = False  # False prevents hallucination propagation across segments
    whisper_repetition_penalty: float = 1.1  # Mild penalty on repeated tokens (1.0 = off)
    whisper_no_repeat_ngram_size: int = 3  # Prevent exact 3-gram repetition (0 = off)
    whisper_hallucination_silence_threshold: Optional[float] = 1.0  # Skip segments hallucinated over >1s silence

    # Shadow transcription (side-by-side model comparison)
    shadow_whisper_enabled: bool = False
    shadow_whisper_api_url: str = ""
    shadow_whisper_timeout_seconds: int = 120

    # Whisper initial prompt — primes decoder with ham radio vocabulary
    fm_whisper_prompt_enabled: bool = True
    fm_whisper_prompt_base: str = (
        "Amateur radio 2-meter repeater conversation. "
        "Net control, mobile, portable, 73."
    )
    fm_operator_cache_file: str = "known_operators.json"
    fm_operator_cache_reload_seconds: int = 3600

    # Processing
    poll_interval_seconds: float = 0.5  # Faster polling (was 2.0)
    batch_size: int = 50  # Larger batches (was 10)
    max_retries: int = 3

    # Deduplication (in-memory, stateless)
    dedup_cache_size: int = 10000
    dedup_ttl_seconds: int = 3600

    # HF mode settings (ignored in P25/FM modes)
    hf_audio_device: str = "default"  # ALSA device name for USB sound card
    hf_audio_sample_rate: int = 48000  # Audio sample rate in Hz
    hf_vox_threshold_db: float = -40.0  # dBFS threshold for VOX recording
    hf_vox_hang_time_seconds: float = 2.0  # Silence before stopping recording
    hf_segment_max_seconds: int = 120  # Max segment duration (HF QSOs are long)
    hf_segment_min_seconds: float = 1.0  # Min segment duration
    hf_capture_dir: str = "/data/hf_capture"  # Directory for HF recordings
    hf_cat_protocol: str = "none"  # icom_civ, yaesu_cat, elecraft_cat, mock, none
    hf_cat_port: str = "/dev/ttyUSB0"  # Serial port for CAT
    hf_cat_baud: int = 19200  # Serial baud rate
    hf_cat_address: int = 0x94  # CI-V address (Icom only, 0x94=IC-7300)
    hf_station_callsign: str = ""  # Station callsign (required for HF)

    # APRS mode settings (ignored in P25/FM/HF modes)
    aprs_frequency_hz: int = 144390000  # 144.390 MHz (North America APRS)
    aprs_device_index: int = 0  # RTL-SDR device index for APRS
    aprs_gain: str = "40"  # RTL-SDR gain
    aprs_sample_rate: int = 22050  # Audio sample rate for Direwolf
    aprs_dedup_ttl_seconds: float = 30.0  # Duplicate packet suppression window

    # Edge-thread grouping (FM mode)
    thread_gap_seconds: float = 45.0  # Silence gap to close a thread
    thread_db_path: str = "/data/threads.db"  # SQLite path for thread persistence
    thread_prune_hours: int = 24  # Delete closed threads older than this

    # Keyword watchlist (FM mode)
    watchlist_path: str = "/app/nodus_edge/data/watchlist.yaml"  # Keyword config file

    # Connectivity probe (internet reachability)
    connectivity_probe_url: str = ""  # URL to probe (empty = derive from synapse_endpoint)
    connectivity_probe_interval_sec: int = 30  # Probe frequency in seconds
    connectivity_fail_threshold: int = 3  # Consecutive failures before declaring offline

    # Edge dashboard
    dashboard_enabled: bool = True
    dashboard_port: int = 8073
    dashboard_max_segments: int = 500
    dashboard_token: str = ""  # Auth token for mutative dashboard endpoints (empty = no auth)
    timezone: str = ""  # IANA timezone (e.g., "America/Chicago"); empty = browser default

    # Logging
    log_level: str = "INFO"
    log_format: str = "console"  # "console" or "json"

    @property
    def recordings_path(self) -> Path:
        return Path(self.recordings_dir)

    @property
    def event_logs_path(self) -> Path:
        return Path(self.event_logs_dir)

    @property
    def output_path(self) -> Path:
        path = Path(self.output_dir)
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def tr_capture_path(self) -> Path:
        """Path for Trunk Recorder capture directory."""
        path = Path(self.tr_capture_dir)
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def hf_capture_path(self) -> Path:
        """Path for HF capture directory (creates if needed)."""
        path = Path(self.hf_capture_dir)
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def fm_capture_path(self) -> Path:
        """Path for FM capture directory (creates if needed)."""
        path = Path(self.fm_capture_dir)
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def fm_airband_output_path(self) -> Path:
        """Path for RTLSDR-Airband output files (creates if needed)."""
        path = self.fm_capture_path / "airband"
        path.mkdir(parents=True, exist_ok=True)
        return path


# Global settings instance
settings = Settings()
