#!/usr/bin/env python3
"""
Test server for APRS edge dashboard rendering.

Serves the dashboard static files and provides a /events SSE endpoint
that pushes mock APRS packets (and one voice segment for regression).

Usage:
    python3 test_aprs_feed.py
    # Open http://localhost:8073 in browser
    # Click "Inject test packets" link or wait for auto-inject
"""

import json
import time
import threading
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from datetime import datetime, timezone
from uuid import uuid4

PORT = 8073
STATIC_DIR = Path(__file__).parent

# SSE clients
_clients: list = []
_lock = threading.Lock()


MOCK_PACKETS = [
    # 1. Position packet (mobile station)
    {
        "modality": "aprs",
        "schema_version": "1.0",
        "segment_id": str(uuid4()),
        "source_node_id": "nodus-edge-aprs-test",
        "timestamp": None,  # filled at send time
        "from_callsign": "W0BXR-9",
        "to_callsign": "APRS",
        "path": ["WIDE1-1", "WIDE2-1"],
        "packet_type": "position",
        "position": {
            "latitude": 41.2565,
            "longitude": -95.9345,
            "speed_kmh": 72.0,
            "course": 225.0,
            "altitude_m": 330.0,
            "symbol": ">",
            "symbol_table": "/",
        },
        "comment": "Nodus APRS test - mobile",
        "raw_packet": "W0BXR-9>APRS,WIDE1-1,WIDE2-1:@092345z4115.39N/09556.07W>072/225/A=001083 Nodus APRS test - mobile",
    },
    # 2. Weather packet
    {
        "modality": "aprs",
        "schema_version": "1.0",
        "segment_id": str(uuid4()),
        "source_node_id": "nodus-edge-aprs-test",
        "timestamp": None,
        "from_callsign": "KD0RBQ",
        "to_callsign": "APWX",
        "path": ["WIDE2-2"],
        "packet_type": "weather",
        "position": {
            "latitude": 41.2924,
            "longitude": -96.1528,
            "symbol": "_",
            "symbol_table": "/",
        },
        "weather": {
            "temperature_f": 68.5,
            "humidity_pct": 55.0,
            "pressure_mbar": 1013.2,
            "wind_speed_mph": 12.3,
            "wind_direction": 180.0,
            "wind_gust_mph": 18.7,
            "rain_1h_inches": 0.0,
            "rain_24h_inches": 0.15,
        },
        "comment": "WX Station Omaha",
        "raw_packet": "KD0RBQ>APWX,WIDE2-2:_09234715c180s012g019t069r000p015h55b10132",
    },
    # 3. Message packet
    {
        "modality": "aprs",
        "schema_version": "1.0",
        "segment_id": str(uuid4()),
        "source_node_id": "nodus-edge-aprs-test",
        "timestamp": None,
        "from_callsign": "N0TES",
        "to_callsign": "APRS",
        "path": ["WIDE1-1"],
        "packet_type": "message",
        "message_to": "W0BXR-9",
        "message_text": "Hey, are you on the repeater tonight? 73",
        "message_id": "042",
        "comment": "",
        "raw_packet": "N0TES>APRS,WIDE1-1::W0BXR-9 :Hey, are you on the repeater tonight? 73{042",
    },
    # 4. Status packet (fallback rendering - no position, no weather, no message)
    {
        "modality": "aprs",
        "schema_version": "1.0",
        "segment_id": str(uuid4()),
        "source_node_id": "nodus-edge-aprs-test",
        "timestamp": None,
        "from_callsign": "W0EAA",
        "to_callsign": "APRS",
        "path": [],
        "packet_type": "status",
        "comment": "Omaha ARC - Net Control 7pm Wednesdays 146.940-",
        "raw_packet": "W0EAA>APRS:>Omaha ARC - Net Control 7pm Wednesdays 146.940-",
    },
    # 5. Voice segment (regression check)
    {
        "schema_version": "1.0",
        "modality": "fm",
        "segment_id": str(uuid4()),
        "source_node_id": "nodus-edge-fm-test",
        "timestamp": None,
        "rf_channel": {
            "frequency_hz": 146940000,
            "repeater_callsign": "W0EAA",
        },
        "transcription": {
            "text": "This is a voice segment regression check. KD0TEST.",
        },
        "detected_callsigns": ["KD0TEST"],
        "audio": {},
        "signal_type": "voice",
    },
]


class SSEHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(STATIC_DIR), **kwargs)

    def do_GET(self):
        if self.path == "/events":
            self._handle_sse()
        elif self.path == "/api/stats":
            self._json_response({"pipeline": {}, "segments": []})
        elif self.path == "/inject":
            self._inject_packets()
        else:
            super().do_GET()

    def _handle_sse(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()

        with _lock:
            _clients.append(self.wfile)

        try:
            while True:
                time.sleep(1)
                # Keep-alive
                self.wfile.write(b": keepalive\n\n")
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            with _lock:
                if self.wfile in _clients:
                    _clients.remove(self.wfile)

    def _inject_packets(self):
        """Trigger mock packet injection."""
        threading.Thread(target=_send_mock_packets, daemon=True).start()
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(b"<html><body><h2>Injecting 5 test packets...</h2>"
                         b"<p>Return to <a href='/'>dashboard</a> to see them.</p>"
                         b"</body></html>")

    def _json_response(self, data):
        body = json.dumps(data).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def _broadcast(data: dict):
    payload = f"data: {json.dumps(data)}\n\n".encode()
    with _lock:
        dead = []
        for client in _clients:
            try:
                client.write(payload)
                client.flush()
            except (BrokenPipeError, ConnectionResetError):
                dead.append(client)
        for d in dead:
            _clients.remove(d)


def _send_mock_packets():
    labels = ["Position", "Weather", "Message", "Status (fallback)", "Voice (regression)"]
    for i, pkt in enumerate(MOCK_PACKETS):
        pkt = dict(pkt)
        pkt["timestamp"] = datetime.now(timezone.utc).isoformat()
        pkt["segment_id"] = str(uuid4())
        print(f"  [{i+1}/5] Sending {labels[i]}: {pkt.get('from_callsign', 'FM')}")
        _broadcast(pkt)
        time.sleep(1.5)
    print("  Done. All 5 test packets sent.")


if __name__ == "__main__":
    print(f"APRS Dashboard Test Server")
    print(f"  Dashboard: http://localhost:{PORT}")
    print(f"  Inject:    http://localhost:{PORT}/inject")
    print()
    print("Steps:")
    print("  1. Open the dashboard URL in your browser")
    print("  2. Click the Live Feed tab")
    print("  3. Open /inject in another tab (or curl it)")
    print("  4. Watch 5 packets appear: 4 APRS + 1 voice regression")
    print()

    server = HTTPServer(("0.0.0.0", PORT), SSEHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
        server.shutdown()
