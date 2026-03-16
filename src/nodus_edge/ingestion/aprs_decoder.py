"""
APRS packet decoder using Direwolf as a software TNC.

Manages an rtl_fm | direwolf pipeline that receives on 144.390 MHz
and decodes AX.25/APRS packets. Decoded packets are passed to a
callback for pipeline processing.

Direwolf outputs decoded packets to stdout in a parseable format.
We capture these lines and extract the raw APRS packet string for
parsing by aprslib.
"""

import os
import re
import subprocess
import tempfile
import threading
import time
from typing import Callable, Optional

import structlog

from ..config import settings

logger = structlog.get_logger(__name__)

# Direwolf decoded packet line pattern:
# [0.3] W1AW>APRS,WIDE1-1:@092345z4903.50N/07201.75W_...
# The [channel] prefix is followed by the raw APRS packet.
_PACKET_RE = re.compile(r"^\[[\d.]+\]\s+(.+)$")


class APRSDecoder:
    """
    Manages rtl_fm -> direwolf pipeline for APRS packet decoding.

    rtl_fm tunes to 144.390 MHz and outputs raw FM audio.
    direwolf decodes the 1200 baud AFSK into AX.25 packets.
    """

    def __init__(
        self,
        on_packet: Callable[[str, float], None],
        frequency_hz: int = 144390000,
        device_index: int = 0,
        gain: str = "40",
        sample_rate: int = 22050,
    ):
        """
        Args:
            on_packet: Callback(raw_packet_str, timestamp) for each decoded packet.
            frequency_hz: APRS frequency in Hz (default 144.390 MHz North America).
            device_index: RTL-SDR device index.
            gain: RTL-SDR gain setting.
            sample_rate: Audio sample rate for rtl_fm output.
        """
        self._on_packet = on_packet
        self._frequency_hz = frequency_hz
        self._device_index = device_index
        self._gain = gain
        self._sample_rate = sample_rate

        self._rtl_fm_proc: Optional[subprocess.Popen] = None
        self._direwolf_proc: Optional[subprocess.Popen] = None
        self._reader_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

        # Stats
        self._packets_decoded = 0
        self._decode_errors = 0
        self._restarts = 0
        self._started_at: Optional[float] = None
        self._conf_path: Optional[str] = None

    def start(self) -> None:
        """Start the rtl_fm | direwolf pipeline."""
        self._stop_event.clear()
        self._started_at = time.time()
        self._start_pipeline()

        self._reader_thread = threading.Thread(
            target=self._reader_loop,
            daemon=True,
            name="aprs-reader",
        )
        self._reader_thread.start()
        logger.info(
            "APRS decoder started",
            frequency_mhz=self._frequency_hz / 1_000_000,
            device_index=self._device_index,
            sample_rate=self._sample_rate,
        )

    def stop(self) -> None:
        """Stop the pipeline."""
        self._stop_event.set()
        self._kill_pipeline()
        if self._reader_thread and self._reader_thread.is_alive():
            self._reader_thread.join(timeout=5)
        if self._conf_path and os.path.exists(self._conf_path):
            os.unlink(self._conf_path)
            self._conf_path = None
        logger.info(
            "APRS decoder stopped",
            packets_decoded=self._packets_decoded,
            decode_errors=self._decode_errors,
        )

    def _ensure_config(self) -> str:
        """Create a minimal direwolf config for receive-only APRS."""
        if self._conf_path and os.path.exists(self._conf_path):
            return self._conf_path
        fd, path = tempfile.mkstemp(prefix="direwolf_", suffix=".conf")
        with os.fdopen(fd, "w") as f:
            f.write("# Receive-only APRS config (auto-generated)\n")
            f.write("ADEVICE stdin null\n")
            f.write("ACHANNELS 1\n")
            f.write(f"ARATE {self._sample_rate}\n")
            f.write("CHANNEL 0\n")
            f.write("MODEM 1200\n")
        self._conf_path = path
        return path

    def _start_pipeline(self) -> None:
        """Launch rtl_fm piped to direwolf."""
        conf_path = self._ensure_config()

        # rtl_fm: tune to APRS frequency, output raw audio
        rtl_fm_cmd = [
            "rtl_fm",
            "-d", str(self._device_index),
            "-f", str(self._frequency_hz),
            "-s", str(self._sample_rate),
            "-g", self._gain,
            "-",  # output to stdout
        ]

        # direwolf: decode APRS from stdin audio
        # -c <conf> = config file
        # -n 1 = 1 audio channel (mono)
        # -r <rate> = sample rate
        # -b 16 = 16-bit samples
        # -t 0 = no terminal colors
        # -q d = quiet (suppress decoded packet hex dump)
        # - = read audio from stdin
        direwolf_cmd = [
            "direwolf",
            "-c", conf_path,
            "-n", "1",
            "-r", str(self._sample_rate),
            "-b", "16",
            "-t", "0",
            "-q", "d",
            "-",
        ]

        try:
            self._rtl_fm_proc = subprocess.Popen(
                rtl_fm_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            )

            self._direwolf_proc = subprocess.Popen(
                direwolf_cmd,
                stdin=self._rtl_fm_proc.stdout,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,  # line-buffered
            )

            # Allow rtl_fm to receive SIGPIPE if direwolf exits
            if self._rtl_fm_proc.stdout:
                self._rtl_fm_proc.stdout.close()

            logger.info("APRS pipeline started", rtl_fm=rtl_fm_cmd, direwolf=direwolf_cmd)

        except FileNotFoundError as e:
            logger.error("APRS pipeline binary not found", error=str(e))
            self._kill_pipeline()
            raise

    def _kill_pipeline(self) -> None:
        """Terminate rtl_fm and direwolf processes."""
        for name, proc in [("direwolf", self._direwolf_proc), ("rtl_fm", self._rtl_fm_proc)]:
            if proc and proc.poll() is None:
                try:
                    proc.terminate()
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=2)
                except Exception as e:
                    logger.warning(f"Error stopping {name}", error=str(e))

        self._rtl_fm_proc = None
        self._direwolf_proc = None

    def _reader_loop(self) -> None:
        """Read direwolf stdout and dispatch decoded packets."""
        while not self._stop_event.is_set():
            # Check if direwolf is still running
            if self._direwolf_proc is None or self._direwolf_proc.poll() is not None:
                if not self._stop_event.is_set():
                    logger.warning("Direwolf process exited, restarting...")
                    self._kill_pipeline()
                    time.sleep(2)
                    try:
                        self._start_pipeline()
                        self._restarts += 1
                    except Exception as e:
                        logger.error("Failed to restart APRS pipeline", error=str(e))
                        time.sleep(10)
                    continue

            try:
                line = self._direwolf_proc.stdout.readline()
                if not line:
                    continue

                line = line.strip()
                if not line:
                    continue

                # Parse decoded packet from direwolf output
                match = _PACKET_RE.match(line)
                if match:
                    raw_packet = match.group(1)
                    timestamp = time.time()
                    self._packets_decoded += 1
                    try:
                        self._on_packet(raw_packet, timestamp)
                    except Exception as e:
                        self._decode_errors += 1
                        logger.debug("Packet callback error", error=str(e), packet=raw_packet[:80])

            except Exception as e:
                if not self._stop_event.is_set():
                    logger.warning("APRS reader error", error=str(e))
                    time.sleep(1)

    def get_stats(self) -> dict:
        """Get decoder statistics."""
        running = (
            self._direwolf_proc is not None
            and self._direwolf_proc.poll() is None
        )
        uptime = time.time() - self._started_at if self._started_at else 0

        return {
            "running": running,
            "frequency_hz": self._frequency_hz,
            "device_index": self._device_index,
            "packets_decoded": self._packets_decoded,
            "decode_errors": self._decode_errors,
            "restarts": self._restarts,
            "uptime_seconds": round(uptime),
        }
