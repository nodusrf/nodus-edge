#!/usr/bin/env python3
"""
Fetch RepeaterBook data for repeaters within a radius of a location.

Fetches from RepeaterBook API by state and filters by distance.
"""

import argparse
import json
import math
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

from nodus_edge.data.us_states import STATE_FIPS, NEIGHBORING_STATES


# RepeaterBook API endpoint
REPEATERBOOK_API = "https://www.repeaterbook.com/api/export.php"

# User-Agent (required by RepeaterBook)
USER_AGENT = "Nodus-Edge/1.0 (https://github.com/nodus) nodus@example.com"

def get_states_for_location(state_name: str) -> List[str]:
    """Get target state plus neighbors for comprehensive repeater coverage."""
    neighbors = NEIGHBORING_STATES.get(state_name, [])
    return [state_name] + neighbors


# Default center: Elkhorn, NE (68022)
DEFAULT_LAT = 41.2828
DEFAULT_LON = -96.2353
DEFAULT_RADIUS_MILES = 250


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance between two points in miles using Haversine formula."""
    R = 3959  # Earth's radius in miles

    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)

    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c


def fetch_state_repeaters(state_fips: str) -> List[Dict]:
    """Fetch all repeaters for a state from RepeaterBook API."""
    url = f"{REPEATERBOOK_API}?state_id={state_fips}"

    request = Request(url)
    request.add_header("User-Agent", USER_AGENT)

    try:
        with urlopen(request, timeout=30) as response:
            data = json.loads(response.read().decode("utf-8"))
            return data.get("results", [])
    except HTTPError as e:
        print(f"  HTTP Error {e.code}: {e.reason}")
        return []
    except URLError as e:
        print(f"  URL Error: {e.reason}")
        return []
    except json.JSONDecodeError as e:
        print(f"  JSON Error: {e}")
        return []


def filter_by_distance(
    repeaters: List[Dict],
    center_lat: float,
    center_lon: float,
    radius_miles: float,
) -> List[Dict]:
    """Filter repeaters by distance from center point."""
    filtered = []

    for rptr in repeaters:
        try:
            lat = float(rptr.get("Lat", 0))
            lon = float(rptr.get("Long", 0))

            if lat == 0 or lon == 0:
                continue

            distance = haversine_distance(center_lat, center_lon, lat, lon)

            if distance <= radius_miles:
                rptr["_distance_miles"] = round(distance, 1)
                filtered.append(rptr)
        except (ValueError, TypeError):
            continue

    return filtered


def fetch_all_repeaters(
    center_lat: float = DEFAULT_LAT,
    center_lon: float = DEFAULT_LON,
    radius_miles: float = DEFAULT_RADIUS_MILES,
    states: Optional[List[str]] = None,
) -> Dict:
    """Fetch all repeaters within radius from multiple states."""

    if states is None:
        states = list(STATE_FIPS.keys())

    all_repeaters = []
    metadata = {
        "center_lat": center_lat,
        "center_lon": center_lon,
        "radius_miles": radius_miles,
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "source": "RepeaterBook",
        "states_queried": [],
    }

    print(f"Fetching repeaters within {radius_miles} miles of ({center_lat}, {center_lon})")
    print(f"Querying {len(states)} states...")

    for state_name in states:
        fips = STATE_FIPS.get(state_name)
        if not fips:
            print(f"  Unknown state: {state_name}")
            continue

        print(f"  Fetching {state_name} (FIPS {fips})...", end=" ", flush=True)

        repeaters = fetch_state_repeaters(fips)
        print(f"got {len(repeaters)} repeaters...", end=" ", flush=True)

        filtered = filter_by_distance(repeaters, center_lat, center_lon, radius_miles)
        print(f"{len(filtered)} within radius")

        all_repeaters.extend(filtered)
        metadata["states_queried"].append({
            "state": state_name,
            "fips": fips,
            "total": len(repeaters),
            "in_radius": len(filtered),
        })

        # Rate limiting - be nice to the API
        time.sleep(1)

    # Sort by distance
    all_repeaters.sort(key=lambda x: x.get("_distance_miles", 9999))

    # Remove duplicates (same frequency + callsign)
    seen = set()
    unique_repeaters = []
    for rptr in all_repeaters:
        key = (rptr.get("Frequency"), rptr.get("Callsign"))
        if key not in seen:
            seen.add(key)
            unique_repeaters.append(rptr)

    metadata["total_repeaters"] = len(unique_repeaters)
    metadata["duplicates_removed"] = len(all_repeaters) - len(unique_repeaters)

    return {
        "metadata": metadata,
        "repeaters": unique_repeaters,
    }


def main():
    parser = argparse.ArgumentParser(description="Fetch RepeaterBook data")
    parser.add_argument("--lat", type=float, default=DEFAULT_LAT, help="Center latitude")
    parser.add_argument("--lon", type=float, default=DEFAULT_LON, help="Center longitude")
    parser.add_argument("--radius", type=float, default=DEFAULT_RADIUS_MILES, help="Radius in miles")
    parser.add_argument("--output", type=str, default="repeaters.json", help="Output file")
    parser.add_argument("--states", type=str, nargs="+", help="Specific states to query")

    args = parser.parse_args()

    data = fetch_all_repeaters(
        center_lat=args.lat,
        center_lon=args.lon,
        radius_miles=args.radius,
        states=args.states,
    )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"\nSaved {data['metadata']['total_repeaters']} repeaters to {output_path}")

    # Print summary by band
    bands = {}
    for rptr in data["repeaters"]:
        freq = float(rptr.get("Frequency", 0))
        if freq < 30:
            band = "HF"
        elif freq < 50:
            band = "6m"
        elif freq < 148:
            band = "2m"
        elif freq < 225:
            band = "1.25m"
        elif freq < 450:
            band = "70cm"
        elif freq < 1300:
            band = "33cm/23cm"
        else:
            band = "Other"
        bands[band] = bands.get(band, 0) + 1

    print("\nBy band:")
    for band, count in sorted(bands.items(), key=lambda x: -x[1]):
        print(f"  {band}: {count}")


if __name__ == "__main__":
    main()
