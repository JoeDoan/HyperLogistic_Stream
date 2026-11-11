"""
live/fetch_aeroapi.py
Fetch real-time hub-to-hub flights (arrived, enroute, departed) from FlightAware AeroAPI v4
and normalize them into the exact BTS historical feature schema (Origin, Dest, DepDelay,
ArrDelay, Distance, CRSDepTime, WheelsOff, CRSArrTime, etc.) so live data and historical
2024 validation data are directly comparable.

Usage:
    python live/fetch_aeroapi.py                  # fetch across tracked US hubs
    python live/fetch_aeroapi.py ATL ORD DFW      # fetch selected hubs only
"""

import json
import math
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

if os.path.dirname(__file__) not in sys.path:
    sys.path.insert(0, os.path.dirname(__file__))
from _common import HUBS, PROJECT_ROOT, RateLimitError, dump_json, http_get, now_iso  # noqa: E402
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from config.hubs import HUB_COORDS_LATLON as HUB_COORDS  # noqa: E402

AEROAPI_BASE = "https://aeroapi.flightaware.com/aeroapi"


def _load_env_key(var_name: str = "AEROAPI_KEY") -> str | None:
    """Load credential from environment or .env without printing its value."""
    val = os.environ.get(var_name)
    if val:
        return val.strip()
    for env_path in (PROJECT_ROOT / ".env", Path.home() / ".env"):
        if env_path.exists():
            try:
                for line in env_path.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    if k.strip() == var_name:
                        cleaned = v.strip().strip('"').strip("'")
                        if cleaned:
                            return cleaned
            except Exception:
                continue
    return None


def _extract_iata(airport_obj: dict | None) -> str | None:
    if not isinstance(airport_obj, dict):
        return None
    iata = airport_obj.get("code_iata")
    if iata and isinstance(iata, str):
        return iata.strip().upper()
    icao = airport_obj.get("code_icao") or airport_obj.get("code")
    if icao and isinstance(icao, str):
        code = icao.strip().upper()
        if len(code) == 4 and code.startswith("K"):
            return code[1:]
        return code
    return None


def _parse_iso_utc(ts_str: str | None) -> datetime | None:
    if not ts_str or not isinstance(ts_str, str):
        return None
    try:
        return datetime.fromisoformat(ts_str.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def _diff_minutes(actual_iso: str | None, sched_iso: str | None) -> float | None:
    t_act = _parse_iso_utc(actual_iso)
    t_sch = _parse_iso_utc(sched_iso)
    if t_act is None or t_sch is None:
        return None
    return round((t_act - t_sch).total_seconds() / 60.0, 2)


def _haversine_miles(origin: str, dest: str) -> float | None:
    if origin not in HUB_COORDS or dest not in HUB_COORDS:
        return None
    lat1, lon1 = map(math.radians, HUB_COORDS[origin])
    lat2, lon2 = map(math.radians, HUB_COORDS[dest])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return round(2 * 3958.8 * math.asin(math.sqrt(a)), 1)


def _normalize_flight(raw_f: dict, section: str, fetched_at: str) -> dict | None:
    if not isinstance(raw_f, dict):
        return None
    if raw_f.get("cancelled") or raw_f.get("diverted"):
        return None

    origin = _extract_iata(raw_f.get("origin"))
    dest = _extract_iata(raw_f.get("destination"))
    if not origin or not dest or origin == dest:
        return None

    sched_out = raw_f.get("scheduled_out") or raw_f.get("scheduled_off")
    act_out = raw_f.get("actual_out") or raw_f.get("actual_off") or raw_f.get("estimated_out") or raw_f.get("estimated_off")
    wheels_off = raw_f.get("actual_off") or raw_f.get("estimated_off") or act_out
    sched_in = raw_f.get("scheduled_in") or raw_f.get("scheduled_on")
    act_in = raw_f.get("actual_in") or raw_f.get("actual_on")
    est_in = raw_f.get("estimated_in") or raw_f.get("estimated_on")

    # Departure delay in minutes (matches BTS gate DepDelay: actual_out - scheduled_out, or AeroAPI departure_delay)
    dep_delay = _diff_minutes(raw_f.get("actual_out"), raw_f.get("scheduled_out"))
    if dep_delay is None and raw_f.get("departure_delay") is not None:
        try:
            dep_delay = round(float(raw_f.get("departure_delay")) / 60.0, 2)
        except (TypeError, ValueError):
            dep_delay = None
    if dep_delay is None:
        dep_delay = _diff_minutes(act_out, sched_out)

    # Arrival delay in minutes (matches BTS gate ArrDelay: actual_in - scheduled_in, or AeroAPI arrival_delay)
    actual_arr_delay = _diff_minutes(raw_f.get("actual_in"), raw_f.get("scheduled_in"))
    if actual_arr_delay is None and section == "arrivals" and raw_f.get("arrival_delay") is not None:
        try:
            actual_arr_delay = round(float(raw_f.get("arrival_delay")) / 60.0, 2)
        except (TypeError, ValueError):
            actual_arr_delay = None
    if actual_arr_delay is None:
        actual_arr_delay = _diff_minutes(act_in, sched_in)

    est_arr_delay = _diff_minutes(est_in or act_in, sched_in)
    if est_arr_delay is None and raw_f.get("arrival_delay") is not None:
        try:
            est_arr_delay = round(float(raw_f.get("arrival_delay")) / 60.0, 2)
        except (TypeError, ValueError):
            est_arr_delay = None

    # Determine status category
    if act_in is not None or section == "arrivals":
        status_category = "arrived"
    elif wheels_off is not None or section in ("enroute", "departures"):
        status_category = "enroute"
    else:
        status_category = "scheduled"

    # Distance in statute miles (BTS uses statute miles)
    dist = raw_f.get("route_distance")
    try:
        dist_miles = float(dist) if dist is not None and float(dist) > 0 else _haversine_miles(origin, dest)
    except (TypeError, ValueError):
        dist_miles = _haversine_miles(origin, dest)

    actual_delta_delay = None
    if actual_arr_delay is not None and dep_delay is not None:
        actual_delta_delay = round(actual_arr_delay - dep_delay, 2)

    ident = (raw_f.get("ident_iata") or raw_f.get("ident") or "").strip()
    fa_id = (raw_f.get("fa_flight_id") or f"{ident}_{sched_out or fetched_at}").strip()
    carrier = (raw_f.get("operator_iata") or raw_f.get("operator") or ident[:2] or "UNK").strip()

    dep_dt = _parse_iso_utc(wheels_off or sched_out)
    flight_date = dep_dt.strftime("%Y-%m-%d") if dep_dt else fetched_at[:10]

    return {
        "flight_id": fa_id,
        "ident": ident,
        "flight_date": flight_date,
        "tail_number": raw_f.get("registration") or "",
        "carrier": carrier,
        "origin": origin,
        "dest": dest,
        "is_hub_to_hub": (origin in HUBS) and (dest in HUBS),
        "status_category": status_category,
        "status_text": raw_f.get("status") or status_category,
        "crs_dep_time_utc": sched_out,
        "wheels_off_utc": wheels_off or sched_out,
        "crs_arr_time_utc": sched_in,
        "wheels_on_utc": act_in or est_in,
        "dep_delay": dep_delay,
        "actual_arr_delay": actual_arr_delay,
        "est_arr_delay": est_arr_delay,
        "actual_delta_delay": actual_delta_delay,
        "distance": dist_miles,
        "progress_percent": raw_f.get("progress_percent"),
        "aircraft_type": raw_f.get("aircraft_type"),
        "fetched_at": fetched_at,
    }


def fetch_hub_flights(iata: str, api_key: str | None = None, max_pages: int = 1) -> list[dict]:
    """Fetch recent arrivals, departures, and enroute flights for a single hub airport."""
    key = api_key or _load_env_key("AEROAPI_KEY")
    if not key:
        raise RuntimeError("AEROAPI_KEY not found in environment or .env")

    icao = f"K{iata.upper()}"
    url = f"{AEROAPI_BASE}/airports/{icao}/flights?max_pages={int(max_pages)}"
    headers = {
        "x-apikey": key,
        "Accept": "application/json; charset=UTF-8",
    }
    raw = http_get(url, headers=headers, timeout=25)
    payload = json.loads(raw)
    fetched_at = now_iso()

    by_id: dict[str, dict] = {}
    for section in ("arrivals", "departures", "enroute", "scheduled_departures", "scheduled_arrivals"):
        for item in payload.get(section) or []:
            norm = _normalize_flight(item, section=section, fetched_at=fetched_at)
            if norm is None:
                continue
            fid = norm["flight_id"]
            # Prefer 'arrived' or 'enroute' record with more complete delay info
            existing = by_id.get(fid)
            if existing is None or (
                existing["actual_delta_delay"] is None and norm["actual_delta_delay"] is not None
            ) or (
                existing["dep_delay"] is None and norm["dep_delay"] is not None
            ):
                by_id[fid] = norm

    return list(by_id.values())


# Core 10 mega-hubs (7 top passenger hubs + 3 major air-cargo hubs: FedEx MEM, UPS SDF, Amazon CVG).
# Fits within a single 10-req/min FlightAware Personal tier burst window while capturing flights across all 24 hubs.
AEROAPI_CORE_HUBS = ["ATL", "ORD", "DFW", "DEN", "LAX", "JFK", "EWR", "MEM", "SDF", "CVG"]


def fetch_aeroapi_flights(
    hubs: list[str] | None = None,
    sleep_between_s: float = 0.5,
    hub_to_hub_only: bool = False,
) -> list[dict]:
    """
    Poll FlightAware AeroAPI across tracked hubs, deduplicating flights that appear at both
    their origin and destination hubs.
    Paces first 9 requests at 0.5s (within 10 req/min burst), then 6.2s if polling >9 hubs.
    """
    key = _load_env_key("AEROAPI_KEY")
    if not key:
        raise RuntimeError("AEROAPI_KEY not found in environment or .env")

    target_hubs = [h.upper() for h in (hubs or AEROAPI_CORE_HUBS)]
    merged: dict[str, dict] = {}

    for idx, iata in enumerate(target_hubs):
        if idx > 0:
            pause = 6.2 if idx >= 9 else sleep_between_s
            if pause > 0:
                time.sleep(pause)
        try:
            rows = fetch_hub_flights(iata, api_key=key, max_pages=1)
            for r in rows:
                if hub_to_hub_only and not r["is_hub_to_hub"]:
                    continue
                fid = r["flight_id"]
                existing = merged.get(fid)
                if existing is None or (
                    existing.get("actual_delta_delay") is None and r.get("actual_delta_delay") is not None
                ) or (
                    existing.get("dep_delay") is None and r.get("dep_delay") is not None
                ):
                    merged[fid] = r
            print(f"  [AeroAPI] {iata}: +{len(rows)} flights (unique total: {len(merged)})", flush=True)
        except RateLimitError as e:
            wait_s = min(max(e.retry_after_s, 10.0), 65.0)
            print(f"  ! [AeroAPI] {iata} rate-limited, waiting {wait_s:.0f}s then retrying once...", flush=True)
            time.sleep(wait_s)
            try:
                rows = fetch_hub_flights(iata, api_key=key, max_pages=1)
                for r in rows:
                    if not hub_to_hub_only or r["is_hub_to_hub"]:
                        merged[r["flight_id"]] = r
                print(f"  [AeroAPI] {iata} (retry OK): +{len(rows)} flights (unique total: {len(merged)})", flush=True)
            except Exception as e2:
                print(f"  ! [AeroAPI] {iata} retry failed: {e2}", flush=True)
        except Exception as e:
            print(f"  ! [AeroAPI] {iata} failed: {e}", flush=True)

    return list(merged.values())


def main():
    hubs = [h.upper() for h in sys.argv[1:] if not h.startswith("-")] or HUBS
    rows = fetch_aeroapi_flights(hubs=hubs)
    arrived = [r for r in rows if r["status_category"] == "arrived" and r["actual_delta_delay"] is not None]
    enroute = [r for r in rows if r["status_category"] == "enroute" and r["dep_delay"] is not None]
    h2h = [r for r in rows if r["is_hub_to_hub"]]

    print(
        f"\nAeroAPI summary: {len(rows)} unique flights across {len(hubs)} hubs "
        f"({len(h2h)} hub-to-hub, {len(arrived)} arrived with ground-truth delta_delay, "
        f"{len(enroute)} active/enroute with dep_delay)"
    )
    for r in (arrived[:5] + enroute[:5]):
        print(
            f"  {r['ident']:<8} {r['origin']}->{r['dest']} [{r['status_category']:<7}] "
            f"dep_delay={r['dep_delay']}m actual_arr_delay={r['actual_arr_delay']}m "
            f"delta={r['actual_delta_delay']}m dist={r['distance']}mi"
        )
    path = dump_json("aeroapi_flights", rows)
    print(f"Saved sample -> {path}")


if __name__ == "__main__":
    main()
