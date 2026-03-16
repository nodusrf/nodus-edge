#!/usr/bin/env python3
"""
Fetch callsign data for offline lookup.

Uses HamDB API to populate a local callsign cache.
Can pre-populate from repeater callsigns or a list.
"""

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError


# HamDB API endpoint
HAMDB_API = "http://api.hamdb.org"


def fetch_callsign(callsign: str) -> Optional[Dict]:
    """Fetch callsign info from HamDB API."""
    url = f"{HAMDB_API}/{callsign}/json/nodus"

    request = Request(url)
    request.add_header("User-Agent", "Nodus-Edge/1.0")

    try:
        with urlopen(request, timeout=10) as response:
            data = json.loads(response.read().decode("utf-8"))
            hamdb = data.get("hamdb", {})

            if hamdb.get("messages", {}).get("status") == "OK":
                call_data = hamdb.get("callsign", {})
                return {
                    "callsign": call_data.get("call", "").upper(),
                    "name": f"{call_data.get('fname', '')} {call_data.get('name', '')}".strip(),
                    "city": call_data.get("addr2", ""),
                    "state": call_data.get("state", ""),
                    "country": call_data.get("country", ""),
                    "grid": call_data.get("grid", ""),
                    "class": call_data.get("class", ""),
                    "lat": call_data.get("lat", ""),
                    "lon": call_data.get("lon", ""),
                    "expires": call_data.get("expires", ""),
                }
            return None
    except (HTTPError, URLError, json.JSONDecodeError) as e:
        print(f"  Error fetching {callsign}: {e}")
        return None


def load_repeater_callsigns(repeater_file: Path) -> Set[str]:
    """Extract unique callsigns from repeater database."""
    if not repeater_file.exists():
        return set()

    with open(repeater_file) as f:
        data = json.load(f)

    callsigns = set()
    for rptr in data.get("repeaters", []):
        cs = rptr.get("Callsign", "").upper().strip()
        if cs:
            callsigns.add(cs)

    return callsigns


def fetch_all_callsigns(
    callsigns: List[str],
    existing_db: Optional[Dict] = None,
    delay: float = 0.5,
) -> Dict[str, Dict]:
    """Fetch info for all callsigns."""
    if existing_db is None:
        existing_db = {}

    results = dict(existing_db)
    to_fetch = [cs for cs in callsigns if cs.upper() not in results]

    print(f"Fetching {len(to_fetch)} callsigns ({len(callsigns) - len(to_fetch)} already cached)")

    for i, callsign in enumerate(to_fetch):
        print(f"  [{i+1}/{len(to_fetch)}] {callsign}...", end=" ", flush=True)

        info = fetch_callsign(callsign)
        if info:
            results[callsign.upper()] = info
            print("OK")
        else:
            print("not found")

        time.sleep(delay)

    return results


def main():
    parser = argparse.ArgumentParser(description="Fetch callsign data for offline lookup")
    parser.add_argument("--repeaters", type=str, default="repeaters.json",
                        help="Repeater database to extract callsigns from")
    parser.add_argument("--output", type=str, default="callsigns.json",
                        help="Output file")
    parser.add_argument("--callsigns", type=str, nargs="+",
                        help="Additional callsigns to fetch")
    parser.add_argument("--delay", type=float, default=0.5,
                        help="Delay between API calls (seconds)")

    args = parser.parse_args()

    data_dir = Path(__file__).parent
    repeater_file = data_dir / args.repeaters
    output_file = data_dir / args.output

    # Load existing database if present
    existing_db = {}
    if output_file.exists():
        with open(output_file) as f:
            existing_db = json.load(f)
        print(f"Loaded {len(existing_db)} existing callsigns from {output_file}")

    # Get callsigns to fetch
    callsigns = set()

    # From repeater database
    if repeater_file.exists():
        rptr_callsigns = load_repeater_callsigns(repeater_file)
        print(f"Found {len(rptr_callsigns)} callsigns in repeater database")
        callsigns.update(rptr_callsigns)

    # Additional specified callsigns
    if args.callsigns:
        callsigns.update(cs.upper() for cs in args.callsigns)

    if not callsigns:
        print("No callsigns to fetch")
        return

    # Fetch all
    results = fetch_all_callsigns(
        list(callsigns),
        existing_db=existing_db,
        delay=args.delay,
    )

    # Save
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\nSaved {len(results)} callsigns to {output_file}")

    # Stats
    states = {}
    for cs, info in results.items():
        state = info.get("state", "Unknown")
        states[state] = states.get(state, 0) + 1

    print("\nBy state:")
    for state, count in sorted(states.items(), key=lambda x: -x[1])[:10]:
        print(f"  {state}: {count}")


if __name__ == "__main__":
    main()
