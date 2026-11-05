"""
live/fetch_opensky.py
Prototype: fetch live aircraft states over the continental US from OpenSky.

NOTE: OpenSky returns POSITIONS, not delay minutes. Anonymous access is heavily
rate-limited (a daily credit budget), so poll no faster than ~120 s and expect 429s.
If you have an OAuth2 token, export OPENSKY_TOKEN to raise your limits.

Usage:
    python live/fetch_opensky.py
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from _common import dump_json, http_get, now_iso  # noqa: E402

# Bounding box roughly covering the continental US.
BBOX = {"lamin": 24.0, "lomin": -125.0, "lamax": 50.0, "lomax": -66.0}
URL = "https://opensky-network.org/api/states/all?" + "&".join(f"{k}={v}" for k, v in BBOX.items())

FIELDS = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "lon", "lat", "baro_altitude", "on_ground", "velocity_ms", "heading",
    "vertical_rate", "sensors", "geo_altitude", "squawk", "spi", "position_source",
]


def fetch_opensky() -> list[dict]:
    headers = {}
    token = os.environ.get("OPENSKY_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"

    raw = http_get(URL, headers=headers)
    payload = json.loads(raw)
    fetched_at = now_iso()

    rows = []
    for s in payload.get("states") or []:
        row = dict(zip(FIELDS, s))
        row["callsign"] = (row.get("callsign") or "").strip()
        row["fetched_at"] = fetched_at
        row["api_time"] = payload.get("time")
        rows.append(row)
    return rows


def main():
    try:
        rows = fetch_opensky()
    except Exception as e:
        print(f"OpenSky fetch failed: {e}")
        sys.exit(1)

    airborne = [r for r in rows if not r["on_ground"] and r["lat"] is not None]
    print(f"OpenSky: {len(rows)} aircraft in US box, {len(airborne)} airborne with position")
    for r in airborne[:5]:
        print(f"  {r['callsign'] or r['icao24']:<9} lat={r['lat']:.3f} lon={r['lon']:.3f} "
              f"alt={r['baro_altitude']} vel={r['velocity_ms']} m/s")

    path = dump_json("opensky", rows[:500])  # cap the sample size
    print(f"Saved sample -> {path}")


if __name__ == "__main__":
    main()
