"""
live/_common.py
Shared helpers for the live-data prototype scripts (stdlib only, no new dependencies).
"""

import json
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SAMPLES_DIR = PROJECT_ROOT / "data" / "live_samples"

# NWS requires a identifying User-Agent. Put a real contact here.
USER_AGENT = "HyperLogisticsStream/0.1 (CS5540 student project; mtuan82@gmail.com)"

# Hub IATA codes (same set as the dashboard's HUB_COORDS keys).
HUBS = [
    "ATL", "ORD", "DFW", "DEN", "LAX", "JFK", "EWR", "SFO", "MEM", "SDF", "CVG",
    "CLT", "LAS", "PHX", "MIA", "IAH", "BOS", "SEA", "MSP", "DTW", "PHL", "BWI",
    "SLC", "MCO",
]


def http_get(url: str, headers: dict | None = None, timeout: int = 20, retries: int = 2) -> bytes:
    """GET with a small retry/backoff. Raises the last error if all attempts fail."""
    hdrs = {"User-Agent": USER_AGENT, "Accept": "*/*"}
    if headers:
        hdrs.update(headers)

    last_err = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=hdrs)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            last_err = e
            if e.code == 429:
                print(f"  ! rate limited (429). Retry-After={e.headers.get('Retry-After')}")
            if e.code in (429, 500, 502, 503, 504) and attempt < retries:
                time.sleep(2 ** (attempt + 1))
                continue
            raise
        except (urllib.error.URLError, TimeoutError) as e:
            last_err = e
            if attempt < retries:
                time.sleep(2 ** (attempt + 1))
                continue
            raise
    raise last_err  # pragma: no cover


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def save_sample(name: str, content: bytes | str, ext: str) -> Path:
    """Save a raw API response so we can build/parse offline and as a demo fallback."""
    SAMPLES_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = SAMPLES_DIR / f"{name}_{stamp}.{ext}"
    mode = "wb" if isinstance(content, bytes) else "w"
    with open(path, mode, **({} if isinstance(content, bytes) else {"encoding": "utf-8"})) as f:
        f.write(content)
    return path


def dump_json(name: str, obj) -> Path:
    return save_sample(name, json.dumps(obj, indent=2), "json")
