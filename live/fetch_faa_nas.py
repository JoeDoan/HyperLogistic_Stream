"""
live/fetch_faa_nas.py
Prototype: fetch real-time US airport delays / ground stops / closures
from the FAA National Airspace System (NAS) Status service (XML, no key needed).

This is the strongest "story" signal: it says which airports are delayed and why.

Usage:
    python live/fetch_faa_nas.py
"""

import os
import sys
import xml.etree.ElementTree as ET

sys.path.insert(0, os.path.dirname(__file__))
from _common import dump_json, http_get, now_iso, save_sample  # noqa: E402

URL = "https://nasstatus.faa.gov/api/airport-status-information"


def _flatten(elem: ET.Element) -> dict:
    """Flatten an XML element (and its children) into a simple dict of tag -> text."""
    out = {}
    for child in elem.iter():
        if child is elem:
            continue
        text = (child.text or "").strip()
        if text:
            out[child.tag.lower()] = text
        for k, v in child.attrib.items():
            out[f"{child.tag.lower()}_{k.lower()}"] = v
    return out


def parse_nas(xml_bytes: bytes) -> list[dict]:
    """
    Parse every delay record into one flat row:
      {airport, type, reason, ...}
    The schema varies by delay type, so we parse defensively.
    """
    root = ET.fromstring(xml_bytes)
    fetched_at = now_iso()
    rows = []

    for delay_type in root.iter("Delay_type"):
        type_name = (delay_type.findtext("Name") or "").strip()
        for group in delay_type:
            if group.tag == "Name":
                continue
            # e.g. Ground_Delay_List, Ground_Stop_List, Airport_Closure_List, ...
            for item in group:
                row = _flatten(item)
                row["delay_type"] = type_name
                row["list"] = group.tag
                row["airport"] = (row.get("arpt") or "").upper()
                row["fetched_at"] = fetched_at
                rows.append(row)
    return rows


def fetch_faa() -> tuple[list[dict], bytes]:
    raw = http_get(URL)
    return parse_nas(raw), raw


def main():
    try:
        rows, raw = fetch_faa()
    except Exception as e:
        print(f"FAA NAS fetch failed: {e}")
        sys.exit(1)

    print(f"FAA NAS: {len(rows)} active delay records")
    for r in rows[:10]:
        print(f"  {r.get('airport') or '???':<4} {r['delay_type']:<28} "
              f"reason={r.get('reason', '-')}")
    if not rows:
        print("  (no active delays right now. That's normal on a calm day.)")

    print(f"Saved raw XML  -> {save_sample('faa_nas', raw, 'xml')}")
    print(f"Saved parsed   -> {dump_json('faa_nas_parsed', rows)}")


if __name__ == "__main__":
    main()
