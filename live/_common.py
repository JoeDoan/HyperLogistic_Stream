"""
live/_common.py
Shared helpers for the live-data prototype scripts (stdlib only, no new dependencies).
"""

import email.utils
import json
import math
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


class RateLimitError(RuntimeError):
    """HTTP 429 rate-limit error carrying the parsed Retry-After backoff window."""

    def __init__(self, message: str, retry_after_s: float = 60.0, url: str = ""):
        super().__init__(message)
        self.retry_after_s = float(retry_after_s)
        self.url = url


def parse_retry_after(header_val: str | None, default_s: float = 60.0) -> float:
    fallback = max(1.0, float(default_s))
    if header_val is None:
        return fallback
    raw = str(header_val).strip()
    if not raw:
        return fallback
    try:
        numeric = float(raw)
        if math.isfinite(numeric):
            return max(1.0, numeric)
    except ValueError:
        pass
    try:
        dt = email.utils.parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(1.0, (dt - datetime.now(timezone.utc)).total_seconds())
    except (TypeError, ValueError, OverflowError):
        return fallback


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
                retry_hdr = e.headers.get("Retry-After") if e.headers else None
                wait_s = parse_retry_after(retry_hdr, default_s=60.0)
                print(f"  ! rate limited (429). Retry-After={retry_hdr} (wait {wait_s:.0f}s)")
                if wait_s <= 15.0 and attempt < retries:
                    time.sleep(wait_s)
                    continue
                raise RateLimitError(
                    f"HTTP 429 for {url} (retry after {wait_s:.0f}s)",
                    retry_after_s=wait_s,
                    url=url,
                ) from e
            if e.code in (500, 502, 503, 504) and attempt < retries:
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
