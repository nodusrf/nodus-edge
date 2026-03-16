"""
CAT/CI-V Radio Interface — Read-only radio state via USB serial.

Provides an abstract RadioInterface with concrete implementations for:
- Icom CI-V (IC-7300, IC-7610, IC-705, IC-7100)
- Yaesu CAT (FT-991/a)
- Elecraft CAT (KX2, KX3, K3/s)
- MockRadio (development/testing)
- NullRadio (no CAT connection — graceful fallback)

The CAT reader runs in a background thread, polling the radio every
~200ms and updating a thread-safe RadioState dataclass that the HF
pipeline reads when building segments.
"""

import logging
import struct
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

from nodus_edge.schema import HFBand, HFMode, frequency_to_band

logger = logging.getLogger(__name__)


@dataclass
class RadioState:
    """Thread-safe snapshot of current radio state."""
    frequency_hz: int = 0
    band: Optional[HFBand] = None
    mode: Optional[HFMode] = None
    sideband: Optional[str] = None  # "usb" or "lsb"
    bandwidth_hz: Optional[int] = None
    s_meter: Optional[int] = None
    s_meter_dbm: Optional[float] = None
    power_watts: Optional[float] = None
    connected: bool = False
    last_updated: float = field(default_factory=time.time)

    def copy(self) -> "RadioState":
        return RadioState(
            frequency_hz=self.frequency_hz,
            band=self.band,
            mode=self.mode,
            sideband=self.sideband,
            bandwidth_hz=self.bandwidth_hz,
            s_meter=self.s_meter,
            s_meter_dbm=self.s_meter_dbm,
            power_watts=self.power_watts,
            connected=self.connected,
            last_updated=self.last_updated,
        )


class RadioInterface(ABC):
    """Abstract interface for reading radio state via CAT/CI-V."""

    @abstractmethod
    def connect(self) -> bool:
        """Attempt to connect to the radio. Returns True on success."""
        ...

    @abstractmethod
    def disconnect(self) -> None:
        """Close the serial connection."""
        ...

    @abstractmethod
    def is_connected(self) -> bool:
        """Check if the connection is alive."""
        ...

    @abstractmethod
    def get_frequency(self) -> int:
        """Read current dial frequency in Hz."""
        ...

    @abstractmethod
    def get_mode(self) -> Optional[HFMode]:
        """Read current operating mode."""
        ...

    @abstractmethod
    def get_s_meter(self) -> Optional[int]:
        """Read S-meter (0-9 for S0-S9, 10+ for S9+10 dB steps)."""
        ...


class NullRadio(RadioInterface):
    """No CAT connection. Returns defaults for everything."""

    def connect(self) -> bool:
        return True

    def disconnect(self) -> None:
        pass

    def is_connected(self) -> bool:
        return True

    def get_frequency(self) -> int:
        return 0

    def get_mode(self) -> Optional[HFMode]:
        return None

    def get_s_meter(self) -> Optional[int]:
        return None


class MockRadio(RadioInterface):
    """
    Mock radio for development/testing.

    Returns configurable static values. Can be updated at runtime
    to simulate frequency/mode changes.
    """

    def __init__(
        self,
        frequency_hz: int = 14_074_000,
        mode: HFMode = HFMode.FT8,
        s_meter: int = 7,
    ):
        self.frequency_hz = frequency_hz
        self.mode = mode
        self.s_meter = s_meter
        self._connected = False

    def connect(self) -> bool:
        self._connected = True
        return True

    def disconnect(self) -> None:
        self._connected = False

    def is_connected(self) -> bool:
        return self._connected

    def get_frequency(self) -> int:
        return self.frequency_hz

    def get_mode(self) -> Optional[HFMode]:
        return self.mode

    def get_s_meter(self) -> Optional[int]:
        return self.s_meter

    def set_frequency(self, hz: int) -> None:
        """Update mock frequency (for testing)."""
        self.frequency_hz = hz

    def set_mode(self, mode: HFMode) -> None:
        """Update mock mode (for testing)."""
        self.mode = mode


class IcomCIV(RadioInterface):
    """
    Icom CI-V protocol for IC-7300, IC-7610, IC-705, IC-7100.

    CI-V uses binary frames:
      FE FE <to_addr> <from_addr> <cmd> [<sub_cmd>] [<data>...] FD

    Default addresses: 94h (IC-7300), 98h (IC-7610), A4h (IC-705), 88h (IC-7100)
    Controller address: E0h (default)
    """

    # CI-V commands (read-only for Phase 1)
    CMD_READ_FREQ = 0x03       # Read operating frequency
    CMD_READ_MODE = 0x04       # Read operating mode
    CMD_READ_S_METER = 0x15    # Read S-meter (sub-cmd 0x02)

    # Mode byte → HFMode mapping
    MODE_MAP = {
        0x00: HFMode.SSB,   # LSB
        0x01: HFMode.SSB,   # USB
        0x02: HFMode.AM,
        0x03: HFMode.CW,
        0x04: HFMode.RTTY,
        0x05: HFMode.FM,
        0x07: HFMode.CW,    # CW-R
        0x08: HFMode.RTTY,  # RTTY-R
    }

    # Mode byte → sideband
    SIDEBAND_MAP = {
        0x00: "lsb",
        0x01: "usb",
    }

    def __init__(self, port: str, baud: int = 19200, address: int = 0x94):
        self.port = port
        self.baud = baud
        self.address = address
        self.controller_address = 0xE0
        self._serial = None

    def connect(self) -> bool:
        try:
            import serial
            self._serial = serial.Serial(
                self.port,
                self.baud,
                timeout=0.5,
                write_timeout=0.5,
            )
            logger.info("CI-V connected to %s at %d baud (addr 0x%02X)", self.port, self.baud, self.address)
            return True
        except Exception as e:
            logger.error("CI-V connection failed: %s", e)
            self._serial = None
            return False

    def disconnect(self) -> None:
        if self._serial and self._serial.is_open:
            self._serial.close()
        self._serial = None

    def is_connected(self) -> bool:
        return self._serial is not None and self._serial.is_open

    def _send_command(self, cmd: int, sub_cmd: Optional[int] = None) -> Optional[bytes]:
        """Send CI-V command and read response."""
        if not self.is_connected():
            return None

        frame = bytearray([0xFE, 0xFE, self.address, self.controller_address, cmd])
        if sub_cmd is not None:
            frame.append(sub_cmd)
        frame.append(0xFD)

        try:
            self._serial.reset_input_buffer()
            self._serial.write(frame)
            # Read response (echo + response)
            response = self._serial.read(64)
            if not response:
                return None
            # Find the response frame (from radio to controller)
            # Look for FE FE E0 <radio_addr> ...
            idx = response.find(bytes([0xFE, 0xFE, self.controller_address, self.address]))
            if idx < 0:
                return None
            # Extract from command byte to FD
            end = response.find(0xFD, idx + 4)
            if end < 0:
                return None
            return bytes(response[idx + 4:end])
        except Exception as e:
            logger.debug("CI-V command 0x%02X failed: %s", cmd, e)
            return None

    @staticmethod
    def _bcd_to_freq(data: bytes) -> int:
        """Convert CI-V BCD-encoded frequency to Hz. 5 bytes, LSB first."""
        if len(data) < 5:
            return 0
        freq = 0
        for i in range(4, -1, -1):
            freq = freq * 100 + (data[i] >> 4) * 10 + (data[i] & 0x0F)
        return freq

    def get_frequency(self) -> int:
        resp = self._send_command(self.CMD_READ_FREQ)
        if resp and len(resp) >= 6:
            # resp[0] is cmd echo (0x03), data starts at [1]
            return self._bcd_to_freq(resp[1:6])
        return 0

    def get_mode(self) -> Optional[HFMode]:
        resp = self._send_command(self.CMD_READ_MODE)
        if resp and len(resp) >= 2:
            # resp[0] is cmd echo (0x04), resp[1] is mode byte
            mode_byte = resp[1]
            return self.MODE_MAP.get(mode_byte)
        return None

    def get_s_meter(self) -> Optional[int]:
        resp = self._send_command(self.CMD_READ_S_METER, sub_cmd=0x02)
        if resp and len(resp) >= 3:
            # resp[0] is cmd echo, resp[1] is sub-cmd echo
            # resp[2:4] is BCD meter value (0000-0255)
            raw = (resp[2] >> 4) * 1000 + (resp[2] & 0x0F) * 100
            if len(resp) >= 4:
                raw += (resp[3] >> 4) * 10 + (resp[3] & 0x0F)
            # Convert raw (0-255) to S-units (0-9, then 10+ for S9+)
            if raw <= 120:
                return min(int(raw / 13.3), 9)
            else:
                return 9 + int((raw - 120) / 13.5)
        return None


class YaesuCAT(RadioInterface):
    """
    Yaesu CAT protocol for FT-991/a.

    Uses semicolon-terminated ASCII commands.
    Common commands: IF (info), FA (freq A), MD (mode).
    """

    MODE_MAP = {
        "1": HFMode.SSB,   # LSB
        "2": HFMode.SSB,   # USB
        "3": HFMode.CW,    # CW-U
        "4": HFMode.FM,
        "5": HFMode.AM,
        "6": HFMode.RTTY,  # RTTY-L
        "7": HFMode.CW,    # CW-L
        "8": HFMode.SSB,   # DATA-L (SSB data)
        "9": HFMode.RTTY,  # RTTY-U
        "B": HFMode.FM,    # FM-N
        "C": HFMode.SSB,   # DATA-U
        "D": HFMode.AM,    # AM-N
    }

    SIDEBAND_MAP = {
        "1": "lsb",
        "2": "usb",
        "8": "lsb",
        "C": "usb",
    }

    def __init__(self, port: str, baud: int = 38400):
        self.port = port
        self.baud = baud
        self._serial = None

    def connect(self) -> bool:
        try:
            import serial
            self._serial = serial.Serial(self.port, self.baud, timeout=0.5, write_timeout=0.5)
            logger.info("Yaesu CAT connected to %s at %d baud", self.port, self.baud)
            return True
        except Exception as e:
            logger.error("Yaesu CAT connection failed: %s", e)
            self._serial = None
            return False

    def disconnect(self) -> None:
        if self._serial and self._serial.is_open:
            self._serial.close()
        self._serial = None

    def is_connected(self) -> bool:
        return self._serial is not None and self._serial.is_open

    def _send_command(self, cmd: str) -> Optional[str]:
        if not self.is_connected():
            return None
        try:
            self._serial.reset_input_buffer()
            self._serial.write(f"{cmd};".encode())
            response = self._serial.read_until(b";", size=128)
            if response and response.endswith(b";"):
                return response.decode("ascii", errors="ignore").rstrip(";")
            return None
        except Exception as e:
            logger.debug("Yaesu CAT command '%s' failed: %s", cmd, e)
            return None

    def get_frequency(self) -> int:
        resp = self._send_command("FA")
        if resp and resp.startswith("FA") and len(resp) >= 11:
            try:
                return int(resp[2:11])
            except ValueError:
                pass
        return 0

    def get_mode(self) -> Optional[HFMode]:
        resp = self._send_command("MD0")
        if resp and resp.startswith("MD0") and len(resp) >= 4:
            return self.MODE_MAP.get(resp[3])
        return None

    def get_s_meter(self) -> Optional[int]:
        resp = self._send_command("SM0")
        if resp and resp.startswith("SM0") and len(resp) >= 7:
            try:
                raw = int(resp[3:7])
                # FT-991 returns 0-255, map to S-units
                if raw <= 120:
                    return min(int(raw / 13.3), 9)
                else:
                    return 9 + int((raw - 120) / 13.5)
            except ValueError:
                pass
        return None


class ElecraftCAT(RadioInterface):
    """
    Elecraft CAT protocol for KX2, KX3, K3/s.

    Kenwood-compatible ASCII protocol. Commands are two letters + data + semicolon.
    """

    MODE_MAP = {
        "1": HFMode.SSB,   # LSB
        "2": HFMode.SSB,   # USB
        "3": HFMode.CW,
        "4": HFMode.FM,
        "5": HFMode.AM,
        "6": HFMode.SSB,   # DATA
        "7": HFMode.CW,    # CW-REV
        "9": HFMode.SSB,   # DATA-REV
    }

    def __init__(self, port: str, baud: int = 38400):
        self.port = port
        self.baud = baud
        self._serial = None

    def connect(self) -> bool:
        try:
            import serial
            self._serial = serial.Serial(self.port, self.baud, timeout=0.5, write_timeout=0.5)
            logger.info("Elecraft CAT connected to %s at %d baud", self.port, self.baud)
            return True
        except Exception as e:
            logger.error("Elecraft CAT connection failed: %s", e)
            self._serial = None
            return False

    def disconnect(self) -> None:
        if self._serial and self._serial.is_open:
            self._serial.close()
        self._serial = None

    def is_connected(self) -> bool:
        return self._serial is not None and self._serial.is_open

    def _send_command(self, cmd: str) -> Optional[str]:
        if not self.is_connected():
            return None
        try:
            self._serial.reset_input_buffer()
            self._serial.write(f"{cmd};".encode())
            response = self._serial.read_until(b";", size=128)
            if response and response.endswith(b";"):
                return response.decode("ascii", errors="ignore").rstrip(";")
            return None
        except Exception as e:
            logger.debug("Elecraft CAT command '%s' failed: %s", cmd, e)
            return None

    def get_frequency(self) -> int:
        resp = self._send_command("FA")
        if resp and resp.startswith("FA"):
            try:
                return int(resp[2:])
            except ValueError:
                pass
        return 0

    def get_mode(self) -> Optional[HFMode]:
        resp = self._send_command("MD")
        if resp and resp.startswith("MD") and len(resp) >= 3:
            return self.MODE_MAP.get(resp[2])
        return None

    def get_s_meter(self) -> Optional[int]:
        resp = self._send_command("SM")
        if resp and resp.startswith("SM"):
            try:
                raw = int(resp[2:])
                if raw <= 6:
                    return raw
                elif raw <= 14:
                    return min(raw - 1, 9)
                else:
                    return 9 + (raw - 14)
            except ValueError:
                pass
        return None


# =============================================================================
# CAT Poller Thread
# =============================================================================

class CATPoller:
    """
    Background thread that polls the radio and updates RadioState.

    Usage:
        poller = CATPoller(radio=MockRadio(), poll_interval_ms=200)
        poller.start()
        state = poller.get_state()  # Thread-safe snapshot
        poller.stop()
    """

    def __init__(self, radio: RadioInterface, poll_interval_ms: int = 200):
        self.radio = radio
        self.poll_interval = poll_interval_ms / 1000.0
        self._state = RadioState()
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def start(self) -> bool:
        """Start polling. Returns True if radio connected successfully."""
        if not self.radio.connect():
            logger.warning("CAT radio connection failed, poller not started")
            return False

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._poll_loop, daemon=True, name="cat-poller")
        self._thread.start()
        logger.info("CAT poller started (interval=%dms)", int(self.poll_interval * 1000))
        return True

    def stop(self) -> None:
        """Stop polling and disconnect."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2.0)
        self.radio.disconnect()
        logger.info("CAT poller stopped")

    def get_state(self) -> RadioState:
        """Get a thread-safe copy of the current radio state."""
        with self._lock:
            return self._state.copy()

    def _poll_loop(self) -> None:
        """Main polling loop (runs in background thread)."""
        while not self._stop_event.is_set():
            try:
                freq = self.radio.get_frequency()
                mode = self.radio.get_mode()
                s_meter = self.radio.get_s_meter()

                band = frequency_to_band(freq) if freq > 0 else None

                # Derive sideband from mode + frequency convention
                sideband = None
                if mode == HFMode.SSB:
                    if band and band in (
                        HFBand.BAND_160M, HFBand.BAND_80M, HFBand.BAND_40M
                    ):
                        sideband = "lsb"
                    else:
                        sideband = "usb"

                with self._lock:
                    self._state.frequency_hz = freq
                    self._state.band = band
                    self._state.mode = mode
                    self._state.sideband = sideband
                    self._state.s_meter = s_meter
                    self._state.connected = self.radio.is_connected()
                    self._state.last_updated = time.time()

            except Exception as e:
                logger.debug("CAT poll error: %s", e)
                with self._lock:
                    self._state.connected = False

            self._stop_event.wait(self.poll_interval)


def create_radio(protocol: str, port: str = "", baud: int = 19200, address: int = 0x94) -> RadioInterface:
    """Factory function to create a radio interface from config."""
    if protocol == "icom_civ":
        return IcomCIV(port=port, baud=baud, address=address)
    elif protocol == "yaesu_cat":
        return YaesuCAT(port=port, baud=baud)
    elif protocol == "elecraft_cat":
        return ElecraftCAT(port=port, baud=baud)
    elif protocol == "mock":
        return MockRadio()
    else:  # "none" or any other value
        return NullRadio()
