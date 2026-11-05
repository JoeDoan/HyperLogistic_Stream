"""
live/fetch_nws_weather.py
Prototype: fetch the latest weather observation for each hub airport from the
National Weather Service API (api.weather.gov). No key needed, but NWS requires
an identifying User-Agent (set in _common.py).

Usage:
    python live/fetch_nws_weather.py               # all hubs
    python live/fetch_nws_weather.py ATL ORD DFW   # selected hubs
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from _common import HUBS, dump_json, http_get, now_iso  # noqa: E402

BASE = "https://api.weather.gov/stations/{icao}/observations/latest"


def _val(props: dict, key: str):
    """NWS wraps numbers as {'value': x, 'unitCode': ...}. Return x (may be None)."""
    v = props.get(key)
    return v.get("value") if isinstance(v, dict) else None


def fetch_hub_weather(iata: str) -> dict:
    icao = "K" + iata  # continental US airports: IATA 'ATL' -> ICAO 'KATL'
    raw = http_get(BASE.format(icao=icao), headers={"Accept": "application/geo+json"})
    props = json.loads(raw).get("properties", {})

    # Lowest cloud layer with a base is the ceiling (BKN/OVC only).
    ceiling_m = None
    for layer in props.get("cloudLayers") or []:
        if layer.get("amount") in ("BKN", "OVC", "VV"):
            base = (layer.get("base") or {}).get("value")
            if base is not None and (ceiling_m is None or base < ceiling_m):
                ceiling_m = base

    return {
        "airport": iata,
        "station": icao,
        "obs_time": props.get("timestamp"),
        "description": props.get("textDescription"),
        "temp_c": _val(props, "temperature"),
        "wind_kmh": _val(props, "windSpeed"),
        "wind_gust_kmh": _val(props, "windGust"),
        "visibility_m": _val(props, "visibility"),
        "ceiling_m": ceiling_m,
        "raw_metar": props.get("rawMessage"),
        "fetched_at": now_iso(),
    }


def main():
    hubs = [h.upper() for h in sys.argv[1:]] or HUBS
    rows, failures = [], []

    for iata in hubs:
        try:
            rows.append(fetch_hub_weather(iata))
        except Exception as e:
            failures.append((iata, str(e)))

    print(f"NWS: {len(rows)}/{len(hubs)} hubs OK")
    for r in rows[:10]:
        print(f"  {r['airport']:<4} {str(r['description']):<22} wind={r['wind_kmh']} km/h "
              f"vis={r['visibility_m']} m ceiling={r['ceiling_m']} m")
    for iata, err in failures:
        print(f"  ! {iata} failed: {err}")

    print(f"Saved sample -> {dump_json('nws_weather', rows)}")
    if not rows:
        sys.exit(1)


if __name__ == "__main__":
    main()
