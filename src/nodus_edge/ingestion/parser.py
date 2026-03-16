"""
SDRTrunk recording and event log parser.

Extracts rich P25 metadata from SDRTrunk outputs including:
- Individual call recordings (*_TO_*_FROM_*.wav)
- Call event logs (*_call_events.log)
- Decoded message logs (*_decoded_messages.log)
"""

import csv
import re
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

import structlog

from ..schema import (
    RFChannel,
    AudioMetadata,
    P25Metadata,
    LRRPData,
    ARSData,
    NetworkData,
    CallEvent,
)

logger = structlog.get_logger(__name__)


class RecordingParser:
    """
    Parser for SDRTrunk recording filenames and event logs.

    Extracts all available metadata from recordings and correlates
    with event log data for rich P25 information.
    """

    # Pattern for individual call recordings
    # Format: YYYYMMDD_HHMMSSSystemName_T-Control__TO_TGID_FROM_RADIOID.wav
    CALL_RECORDING_PATTERN = re.compile(
        r'^(\d{8})_(\d{6})(.+?)_T-Control__TO_(\d+)_FROM_(\d+)(?:_TONES_.+)?\.wav$'
    )

    # Pattern for baseband recordings
    # Format: YYYYMMDD_HHMMSS_FREQUENCY_SystemName_T-Control_CHANNEL_baseband.wav
    BASEBAND_PATTERN = re.compile(
        r'^(\d{8})_(\d{6})_(\d+)_(.+?)_T-Control_(\d+)_baseband\.wav$'
    )

    # Pattern for event log filenames
    # Format: YYYYMMDD_HHMMSS.mmm_FREQUENCY_Hz_TYPE_logtype.log
    EVENT_LOG_PATTERN = re.compile(
        r'^(\d{8})_(\d{6})\.(\d+)_(\d+)_Hz_([\w-]+)_(call_events|decoded_messages)\.log$'
    )

    def parse_call_recording(self, filepath: Path) -> Optional[Dict[str, Any]]:
        """
        Parse an individual call recording filename.

        Returns metadata dict or None if not a valid recording filename.
        """
        match = self.CALL_RECORDING_PATTERN.match(filepath.name)
        if not match:
            return None

        date_str = match.group(1)
        time_str = match.group(2)
        system_name = match.group(3)
        talkgroup_id = match.group(4)
        radio_id = match.group(5)

        try:
            dt = datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M%S")
        except ValueError:
            logger.warning("Invalid timestamp in filename", path=str(filepath))
            return None

        # Get file stats
        try:
            stat = filepath.stat()
            file_size = stat.st_size
        except OSError:
            file_size = None

        return {
            "filename": filepath.name,
            "filepath": str(filepath.absolute()),
            "timestamp": dt,
            "system_name": system_name,
            "talkgroup_id": talkgroup_id,
            "source_radio_id": radio_id,
            "file_size_bytes": file_size,
            "recording_type": "call",
        }

    def parse_baseband_recording(self, filepath: Path) -> Optional[Dict[str, Any]]:
        """Parse a baseband recording filename."""
        match = self.BASEBAND_PATTERN.match(filepath.name)
        if not match:
            return None

        date_str = match.group(1)
        time_str = match.group(2)
        frequency = int(match.group(3))
        system_name = match.group(4)
        channel = match.group(5)

        try:
            dt = datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M%S")
        except ValueError:
            return None

        try:
            stat = filepath.stat()
            file_size = stat.st_size
        except OSError:
            file_size = None

        return {
            "filename": filepath.name,
            "filepath": str(filepath.absolute()),
            "timestamp": dt,
            "system_name": system_name,
            "frequency_hz": frequency,
            "channel_number": channel,
            "file_size_bytes": file_size,
            "recording_type": "baseband",
        }

    def parse_event_log(self, filepath: Path) -> Optional[Dict[str, Any]]:
        """Parse event log filename metadata."""
        match = self.EVENT_LOG_PATTERN.match(filepath.name)
        if not match:
            return None

        date_str = match.group(1)
        time_str = match.group(2)
        ms_str = match.group(3)
        frequency = int(match.group(4))
        channel_type = match.group(5)
        log_type = match.group(6)

        try:
            dt = datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M%S")
        except ValueError:
            return None

        return {
            "filename": filepath.name,
            "filepath": str(filepath.absolute()),
            "timestamp": dt,
            "timestamp_ms": int(ms_str),
            "frequency_hz": frequency,
            "channel_type": channel_type,
            "log_type": log_type,
            "timestamp_prefix": f"{date_str}_{time_str}",
        }

    def parse_call_events_file(self, filepath: Path) -> List[CallEvent]:
        """
        Parse a call_events.log file into CallEvent objects.

        Extracts all P25 metadata including LRRP, ARS, network data.
        """
        events = []

        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    event = self._parse_event_row(row)
                    if event:
                        events.append(event)
        except Exception as e:
            logger.error("Error parsing event log", path=str(filepath), error=str(e))

        return events

    def extract_encrypted_calls(
        self,
        filepath: Path,
        since_timestamp: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """
        Extract encrypted group calls from event log.

        These calls have P25 metadata but no audio recordings since
        SDRTrunk cannot decode encrypted traffic.

        Returns list of dicts with call metadata suitable for creating segments.
        """
        encrypted_calls = []
        seen_calls = set()  # Dedupe by (timestamp, talkgroup, radio_id)

        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Use (value or '') pattern to handle None values from malformed CSV rows
                    event_type = (row.get('EVENT') or '').strip('"')

                    # Only process encrypted group calls
                    if 'Encrypted' not in event_type:
                        continue
                    if 'Group Call' not in event_type:
                        continue

                    # Parse timestamp
                    timestamp_str = (row.get('TIMESTAMP') or '').strip('"')
                    if not timestamp_str:
                        continue
                    try:
                        dt = datetime.strptime(timestamp_str, "%Y:%m:%d:%H:%M:%S")
                    except ValueError:
                        continue

                    # Skip if before since_timestamp
                    if since_timestamp and dt < since_timestamp:
                        continue

                    # Parse destination (TO field) - contains talkgroup
                    to_field = (row.get('TO') or '').strip('"')
                    tg_id, tg_name, _ = self._parse_destination(to_field)

                    if not tg_id:
                        continue

                    # Parse source (FROM field) - contains radio ID
                    from_field = (row.get('FROM') or '').strip('"')
                    source_radio_id, _ = self._parse_source(from_field)

                    # Parse frequency
                    freq_str = (row.get('FREQUENCY') or '').strip('"')
                    try:
                        frequency_hz = int(float(freq_str)) if freq_str else None
                    except ValueError:
                        frequency_hz = None

                    # Parse details for additional metadata
                    details_str = (row.get('DETAILS') or '').strip('"')
                    details = self._parse_details(details_str, event_type)

                    # Parse channel number (site-timeslot format like "2-88 TS1")
                    channel_str = (row.get('CHANNEL_NUMBER') or '').strip('"')
                    timeslot = self._parse_timeslot(row.get('TIMESLOT') or '')

                    # Create dedup key - unique per call start
                    # Round timestamp to nearest second for dedup
                    dedup_key = (
                        dt.strftime("%Y%m%d%H%M%S"),
                        tg_id,
                        source_radio_id or "unknown",
                    )

                    if dedup_key in seen_calls:
                        continue
                    seen_calls.add(dedup_key)

                    call_data = {
                        "timestamp": dt,
                        "talkgroup_id": tg_id,
                        "talkgroup_name": tg_name,
                        "source_radio_id": source_radio_id,
                        "frequency_hz": frequency_hz,
                        "channel_number": channel_str,
                        "timeslot": timeslot,
                        "event_type": event_type,
                        "protocol": (row.get('PROTOCOL') or '').strip('"') or "APCO-25",
                        "phase": details.get("phase"),
                        "priority": details.get("priority"),
                        "grant_type": details.get("grant_type"),
                        "encrypted": True,
                        "event_id": (row.get('EVENT_ID') or '').strip('"') or None,
                        "raw_details": details_str,
                        "source_file": str(filepath),
                    }

                    encrypted_calls.append(call_data)

        except Exception as e:
            logger.error("Error extracting encrypted calls", path=str(filepath), error=str(e))

        return encrypted_calls

    def _parse_event_row(self, row: Dict[str, str]) -> Optional[CallEvent]:
        """Parse a single CSV row into a CallEvent."""
        try:
            # Parse timestamp - use (value or '') to handle None from malformed CSV
            timestamp_str = (row.get('TIMESTAMP') or '').strip('"')
            if not timestamp_str:
                return None
            dt = datetime.strptime(timestamp_str, "%Y:%m:%d:%H:%M:%S")

            # Parse duration
            duration_str = (row.get('DURATION_MS') or '').strip('"')
            duration_ms = int(duration_str) if duration_str else None

            # Parse destination (TO field)
            to_field = (row.get('TO') or '').strip('"')
            tg_id, tg_name, dest_radio_id = self._parse_destination(to_field)

            # Parse source (FROM field)
            from_field = (row.get('FROM') or '').strip('"')
            source_radio_id, source_ip = self._parse_source(from_field)

            # Parse frequency
            freq_str = (row.get('FREQUENCY') or '').strip('"')
            frequency_hz = int(float(freq_str)) if freq_str else None

            # Parse timeslot
            timeslot_str = (row.get('TIMESLOT') or '').strip('"')
            timeslot = self._parse_timeslot(timeslot_str)

            # Parse details for rich metadata
            details_str = (row.get('DETAILS') or '').strip('"')
            event_type = (row.get('EVENT') or '').strip('"')
            details = self._parse_details(details_str, event_type)

            # Build LRRP data if present
            lrrp = None
            if details.get("lrrp"):
                lrrp = LRRPData(**details["lrrp"])

            # Build ARS data if present
            ars = None
            if details.get("ars"):
                ars = ARSData(**details["ars"])

            # Build network data if present
            network = None
            if details.get("network"):
                network = NetworkData(**details["network"])

            return CallEvent(
                timestamp=dt,
                event_type=event_type,
                duration_ms=duration_ms,
                protocol=(row.get('PROTOCOL') or '').strip('"') or None,
                source_radio_id=source_radio_id,
                source_ip=source_ip,
                destination_talkgroup_id=tg_id,
                destination_talkgroup_name=tg_name,
                destination_radio_id=dest_radio_id,
                channel_number=(row.get('CHANNEL_NUMBER') or '').strip('"') or None,
                frequency_hz=frequency_hz,
                timeslot=timeslot,
                phase=details.get("phase"),
                encrypted=details.get("encrypted", False),
                is_emergency=details.get("is_emergency", False),
                priority=details.get("priority"),
                grant_type=details.get("grant_type"),
                event_id=(row.get('EVENT_ID') or '').strip('"') or None,
                raw_details=details_str if details_str else None,
                lrrp=lrrp,
                ars=ars,
                network=network,
            )

        except Exception as e:
            logger.debug("Error parsing event row", error=str(e))
            return None

    def _parse_destination(
        self, to_field: str
    ) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Parse TO field into (talkgroup_id, talkgroup_name, radio_id)."""
        talkgroup_id = None
        talkgroup_name = None
        radio_id = None

        # Pattern: "[Name] (ID)" or just " (ID)"
        match = re.match(r'\[([^\]]+)\]\s*\((\d+)\)', to_field)
        if match:
            talkgroup_name = match.group(1)
            talkgroup_id = match.group(2)
        else:
            match = re.match(r'\s*\((\d+)\)', to_field)
            if match:
                radio_id = match.group(1)

        return talkgroup_id, talkgroup_name, radio_id

    def _parse_source(self, from_field: str) -> Tuple[Optional[str], Optional[str]]:
        """Parse FROM field into (radio_id, ip_address)."""
        if not from_field:
            return None, None
        from_field = from_field.strip()

        # Check if it's an IP address
        if re.match(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$', from_field):
            return None, from_field

        return from_field if from_field else None, None

    def _parse_timeslot(self, timeslot_str: str) -> Optional[int]:
        """Parse timeslot string like 'TS:1' or 'TS:2'."""
        match = re.search(r'TS:(\d+)', timeslot_str)
        return int(match.group(1)) if match else None

    def _parse_details(self, details_str: str, event_type: str) -> Dict[str, Any]:
        """Parse details string into structured data."""
        result: Dict[str, Any] = {
            "phase": None,
            "encrypted": False,
            "is_emergency": False,
            "priority": None,
            "grant_type": None,
        }

        if not details_str:
            # Still check event_type for emergency/encrypted even without details
            result["encrypted"] = 'Encrypted' in event_type
            result["is_emergency"] = 'Emergency' in event_type
            return result

        # Parse phase
        if 'PHASE 1' in details_str:
            result["phase"] = 1
        elif 'PHASE 2' in details_str:
            result["phase"] = 2

        # Parse encryption
        result["encrypted"] = (
            'ENCRYPTED' in details_str or
            'Encrypted' in event_type
        )

        # Parse emergency - P25 emergency button activation
        # SDRTrunk marks these as "Emergency Group Call" or similar
        result["is_emergency"] = (
            'EMERGENCY' in details_str.upper() or
            'Emergency' in event_type
        )

        # Parse priority
        pri_match = re.search(r'PRI(\d+)', details_str)
        if pri_match:
            result["priority"] = int(pri_match.group(1))

        # Parse grant type
        grant_types = [
            "DATA CHANNEL GRANT",
            "DATA CHANNEL ACTIVE",
            "CHANNEL GRANT",
            "CONTINUE",
        ]
        for gt in grant_types:
            if gt in details_str:
                result["grant_type"] = gt
                break

        # Parse LRRP data
        if 'LRRP' in details_str:
            result["lrrp"] = self._parse_lrrp(details_str)

        # Parse ARS data
        if 'ARS' in details_str:
            result["ars"] = self._parse_ars(details_str)

        # Parse network data
        if 'UDP PORT' in details_str or 'IP FROM' in details_str:
            result["network"] = self._parse_network(details_str)

        return result

    def _parse_lrrp(self, details_str: str) -> Dict[str, Any]:
        """Parse LRRP-specific data from details."""
        lrrp: Dict[str, Any] = {
            "request_type": None,
            "request_id": None,
            "trigger_distance": None,
            "requested_tokens": [],
        }

        if 'TRIGGERED LOCATION START' in details_str:
            lrrp["request_type"] = "TRIGGERED LOCATION START"
        elif 'TRIGGERED LOCATION STOP' in details_str:
            lrrp["request_type"] = "TRIGGERED LOCATION STOP"
        elif 'IMMEDIATE LOCATION REQUEST' in details_str:
            lrrp["request_type"] = "IMMEDIATE LOCATION REQUEST"
        elif 'LOCATION RESPONSE' in details_str:
            lrrp["request_type"] = "LOCATION RESPONSE"

        # Parse request ID
        id_match = re.search(r'ID:(\d+)', details_str)
        if id_match:
            lrrp["request_id"] = int(id_match.group(1))

        # Parse trigger distance
        dist_match = re.search(r'TRIGGER DISTANCE:(\d+)', details_str)
        if dist_match:
            lrrp["trigger_distance"] = int(dist_match.group(1))

        # Parse requested tokens
        tokens_match = re.search(r'REQUESTED TOKENS \[([^\]]+)\]', details_str)
        if tokens_match:
            lrrp["requested_tokens"] = [
                t.strip() for t in tokens_match.group(1).split(',')
            ]

        # Parse coordinates if present
        lat_match = re.search(r'LAT[ITUDE]*[:\s]*(-?\d+\.?\d*)', details_str, re.I)
        lon_match = re.search(r'LON[GITUDE]*[:\s]*(-?\d+\.?\d*)', details_str, re.I)
        if lat_match:
            lrrp["latitude"] = float(lat_match.group(1))
        if lon_match:
            lrrp["longitude"] = float(lon_match.group(1))

        return lrrp

    def _parse_ars(self, details_str: str) -> Dict[str, Any]:
        """Parse ARS-specific data from details."""
        ars: Dict[str, Any] = {
            "status": None,
            "refresh_minutes": None,
        }

        if 'REGISTRATION SUCCESS' in details_str:
            ars["status"] = "REGISTRATION SUCCESS"
        elif 'REGISTRATION FAILURE' in details_str:
            ars["status"] = "REGISTRATION FAILURE"
        elif 'DEREGISTRATION' in details_str:
            ars["status"] = "DEREGISTRATION"

        refresh_match = re.search(r'REFRESH IN:(\d+)mins', details_str)
        if refresh_match:
            ars["refresh_minutes"] = int(refresh_match.group(1))

        return ars

    def _parse_network(self, details_str: str) -> Dict[str, Any]:
        """Parse network-specific data from details."""
        network: Dict[str, Any] = {
            "protocol": "UDP",
        }

        port_match = re.search(r'UDP PORT FROM:(\d+) TO:(\d+)', details_str)
        if port_match:
            network["source_port"] = int(port_match.group(1))
            network["destination_port"] = int(port_match.group(2))

        ip_match = re.search(r'IP FROM:(\d+\.\d+\.\d+\.\d+)', details_str)
        if ip_match:
            network["source_ip"] = ip_match.group(1)

        ip_to_match = re.search(r'IP TO:(\d+\.\d+\.\d+\.\d+)', details_str)
        if ip_to_match:
            network["destination_ip"] = ip_to_match.group(1)

        return network

    def build_rf_channel(
        self,
        metadata: Dict[str, Any],
        events: Optional[List[CallEvent]] = None,
    ) -> RFChannel:
        """Build RFChannel from parsed metadata and events."""
        # Try to get frequency from events if not in metadata
        frequency_hz = metadata.get("frequency_hz", 0)
        if not frequency_hz and events:
            for event in events:
                if event.frequency_hz:
                    frequency_hz = event.frequency_hz
                    break

        # Get talkgroup name from events
        tg_name = None
        timeslot = None
        if events:
            for event in events:
                if event.destination_talkgroup_name:
                    tg_name = event.destination_talkgroup_name
                if event.timeslot:
                    timeslot = event.timeslot
                if tg_name and timeslot:
                    break

        return RFChannel(
            frequency_hz=frequency_hz,
            channel_number=metadata.get("channel_number"),
            talkgroup_id=metadata.get("talkgroup_id"),
            talkgroup_name=tg_name,
            system_name=metadata.get("system_name"),
            channel_type=metadata.get("channel_type"),
            timeslot=timeslot,
        )

    def build_audio_metadata(self, metadata: Dict[str, Any]) -> AudioMetadata:
        """Build AudioMetadata from parsed metadata."""
        return AudioMetadata(
            filename=metadata.get("filename", "unknown"),
            filepath=metadata.get("filepath"),
            duration_seconds=metadata.get("duration_seconds"),
            duration_ms=metadata.get("duration_ms"),
            file_size_bytes=metadata.get("file_size_bytes"),
            sample_rate_hz=metadata.get("sample_rate_hz", 8000),
            format=metadata.get("format", "wav"),
        )

    def build_p25_metadata(
        self,
        events: Optional[List[CallEvent]] = None,
    ) -> Optional[P25Metadata]:
        """Build P25Metadata from call events."""
        if not events:
            return None

        # Use first event with data
        first_event = events[0] if events else None
        if not first_event:
            return None

        return P25Metadata(
            protocol=first_event.protocol,
            event_type=first_event.event_type,
            event_id=first_event.event_id,
            phase=first_event.phase,
            encrypted=first_event.encrypted,
            is_emergency=first_event.is_emergency,
            priority=first_event.priority,
            grant_type=first_event.grant_type,
        )
