#!/usr/bin/env python3
"""
02_kafka_producer.py
Phase 2 Ingestion: Kafka Time-Replay Engine.

Replays BTS 2024 flight departure events in chronological WheelsOff order,
emitting configurable rates (1k / 5k / 20k events/sec) to the 'flights_stream' topic.

Usage:
    python pipeline/02_kafka_producer.py --rate 1000
    python pipeline/02_kafka_producer.py --rate 5000 --limit 100000
"""

import argparse
import csv
import glob
import json
import os
import re
import signal
import sys
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
import zoneinfo

import yaml

try:
    from confluent_kafka import Producer, KafkaException
    CONFLUENT = True
    HAS_KAFKA_CLIENT = True
except ImportError:
    CONFLUENT = False
    try:
        from kafka import KafkaProducer
        HAS_KAFKA_CLIENT = True
    except ImportError:
        HAS_KAFKA_CLIENT = False

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SHUTDOWN = threading.Event()
CONFIG_PATH = PROJECT_ROOT / "config" / "pipeline_config.yaml"

AIRPORT_TZ = {
    'ATL': 'America/New_York', 'ORD': 'America/Chicago', 'DFW': 'America/Chicago',
    'DEN': 'America/Denver', 'LAX': 'America/Los_Angeles', 'JFK': 'America/New_York',
    'EWR': 'America/New_York', 'SFO': 'America/Los_Angeles', 'MEM': 'America/Chicago',
    'SDF': 'America/New_York', 'CVG': 'America/New_York', 'CLT': 'America/New_York',
    'LAS': 'America/Los_Angeles', 'PHX': 'America/Phoenix', 'MIA': 'America/New_York',
    'IAH': 'America/Chicago', 'BOS': 'America/New_York', 'SEA': 'America/Los_Angeles',
    'MSP': 'America/Chicago', 'DTW': 'America/New_York', 'PHL': 'America/New_York',
    'BWI': 'America/New_York', 'SLC': 'America/Denver', 'MCO': 'America/New_York',
}


def load_config(config_path: str = None):
    resolved = config_path or os.environ.get("HYPERLOGISTICS_CONFIG", str(CONFIG_PATH))
    if not os.path.isabs(resolved):
        resolved = str(PROJECT_ROOT / resolved)
    with open(resolved) as f:
        return yaml.safe_load(f)


def hhmm_to_utc_dt(flight_date_str: str, hhmm_float, airport_code: str):
    """Convert BTS HHMM local time to a timezone-aware UTC datetime object."""
    if hhmm_float is None or str(hhmm_float).strip() == "":
        return None
    tz_name = AIRPORT_TZ.get(airport_code)
    if not tz_name:
        return None
    try:
        hhmm = int(float(hhmm_float))
        if hhmm == 2400:
            hour, minute, extra = 0, 0, 1
        else:
            hour, minute, extra = hhmm // 100, hhmm % 100, 0
        if hour > 23 or minute > 59:
            return None
        fdate = datetime.strptime(flight_date_str.strip(), '%Y-%m-%d')
        local_dt = fdate.replace(hour=hour, minute=minute) + timedelta(days=extra)
        tz = zoneinfo.ZoneInfo(tz_name)
        local_aware = local_dt.replace(tzinfo=tz)
        return local_aware.astimezone(zoneinfo.ZoneInfo('UTC'))
    except Exception:
        return None


def hhmm_to_utc(flight_date_str: str, hhmm_float, airport_code: str):
    """Convert BTS HHMM local time to UTC ISO-8601 string."""
    dt = hhmm_to_utc_dt(flight_date_str, hhmm_float, airport_code)
    return dt.strftime('%Y-%m-%dT%H:%M:%SZ') if dt else None


def parse_bts_row(row: dict):
    """Parse a BTS CSV row into a Kafka message payload with midnight rollover protection."""
    try:
        origin = row.get('Origin', '').strip()
        dest = row.get('Dest', '').strip()
        if not origin or not dest:
            return None
        if origin not in AIRPORT_TZ or dest not in AIRPORT_TZ:
            return None

        cancelled = float(row.get('Cancelled') or 0)
        diverted = float(row.get('Diverted') or 0)
        if cancelled or diverted:
            return None

        dep_delay_raw = row.get('DepDelay') or ''
        arr_delay_raw = row.get('ArrDelay') or ''
        distance_raw = row.get('Distance') or ''
        if not dep_delay_raw.strip() or not arr_delay_raw.strip():
            return None

        dep_delay = float(dep_delay_raw)
        distance = float(distance_raw) if distance_raw.strip() else 0.0
        flight_date = row.get('FlightDate', '').strip()
        carrier = row.get('Reporting_Airline', '').strip()
        tail = row.get('Tail_Number', '').strip() or 'UNKNOWN'

        crs_dep_raw = row.get('CRSDepTime') or ''
        wheels_off_raw = row.get('WheelsOff') or ''
        crs_arr_raw = row.get('CRSArrTime') or ''

        crs_dep_dt = hhmm_to_utc_dt(flight_date, crs_dep_raw, origin)
        wheels_off_dt = hhmm_to_utc_dt(flight_date, wheels_off_raw, origin)
        crs_arr_dt = hhmm_to_utc_dt(flight_date, crs_arr_raw, dest)

        if not wheels_off_dt or not crs_arr_dt:
            return None

        if crs_dep_dt and wheels_off_dt < crs_dep_dt - timedelta(hours=1):
            wheels_off_dt += timedelta(days=1)
        if crs_dep_dt and crs_arr_dt < crs_dep_dt:
            crs_arr_dt += timedelta(days=1)

        wheels_off_utc = wheels_off_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        crs_dep_utc = crs_dep_dt.strftime('%Y-%m-%dT%H:%M:%SZ') if crs_dep_dt else wheels_off_utc
        crs_arr_utc = crs_arr_dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        flight_id = f"{carrier}{row.get('Flight_Number_Reporting_Airline','0').strip()}_{flight_date.replace('-','')}_{origin}_{dest}"
        now_ms = int(time.time() * 1000)

        return {
            "flight_id": flight_id.replace(' ', ''),
            "flight_date": flight_date,
            "tail_number": tail,
            "carrier": carrier,
            "origin": origin,
            "dest": dest,
            "crs_dep_time_utc": crs_dep_utc,
            "wheels_off_utc": wheels_off_utc,
            "crs_arr_time_utc": crs_arr_utc,
            "dep_delay": dep_delay,
            "distance": distance,
            "emit_epoch_ms": now_ms,
            "emit_ts": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
        }
    except (ValueError, TypeError):
        return None


def _month_sort_key(filepath: str):
    m = re.search(r'_(\d{4})_(\d+)\.csv$', filepath)
    return (int(m.group(1)), int(m.group(2))) if m else (9999, 99)


def load_bts_2024_sorted(bts_dir: str, limit: int = None):
    """Load 2024 BTS CSVs sorted by integer month, parse into events, sort by WheelsOff UTC."""
    pattern = os.path.join(bts_dir, "*2024*.csv")
    csv_files = sorted(glob.glob(pattern), key=_month_sort_key)
    if not csv_files:
        raise FileNotFoundError(
            f"No 2024 BTS CSVs in {bts_dir}. "
            "Run: python scripts/download_bts.py --years 2024 --extract"
        )

    print(f"[PRODUCER] Loading BTS 2024 from {len(csv_files)} file(s)...", flush=True)
    events = []
    for filepath in csv_files:
        print(f"  Parsing {Path(filepath).name}...", flush=True)
        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f)
            for row in reader:
                payload = parse_bts_row(row)
                if payload:
                    events.append((payload['wheels_off_utc'], payload))
                if limit and len(events) >= limit * 2:
                    break
        if limit and len(events) >= limit * 2:
            break

    print(f"[PRODUCER] Parsed {len(events):,} valid events. Sorting by WheelsOff...", flush=True)
    events.sort(key=lambda x: x[0])
    result = [e[1] for e in events]
    if limit:
        result = result[:limit]
    print(f"[PRODUCER] Ready to emit {len(result):,} events.", flush=True)
    return result


class ThroughputCounter:
    """Thread-safe rolling throughput counter."""
    def __init__(self, window_sec=5):
        self.window_sec = window_sec
        self.timestamps = deque()
        self._lock = threading.Lock()

    def record(self):
        now = time.monotonic()
        with self._lock:
            self.timestamps.append(now)
            cutoff = now - self.window_sec
            while self.timestamps and self.timestamps[0] < cutoff:
                self.timestamps.popleft()

    def rate(self):
        now = time.monotonic()
        with self._lock:
            cutoff = now - self.window_sec
            while self.timestamps and self.timestamps[0] < cutoff:
                self.timestamps.popleft()
            return len(self.timestamps) / self.window_sec


def make_confluent_producer(bootstrap_servers: str) -> "Producer":
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'linger.ms': 5,
        'batch.num.messages': 10000,
        'compression.type': 'lz4',
        'queue.buffering.max.messages': 500000,
        'queue.buffering.max.kbytes': 1048576,
    }
    return Producer(conf)


def make_kafka_python_producer(bootstrap_servers: str):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        compression_type='lz4',
        linger_ms=5,
        batch_size=65536,
        buffer_memory=67108864,
    )


def emit_events(events, topic: str, producer, rate_eps: int, verbose_every: int = 5000):
    """Emit events at target rate with token-bucket throttling and emission-time stamping."""
    counter = ThroughputCounter(window_sec=5)
    total_sent = 0
    total_errors = 0
    interval_ns = int(1e9 / rate_eps)
    start_wall = time.monotonic()
    next_emit = start_wall

    _stop = threading.Event()

    def handle_sig(*_):
        print("\n[PRODUCER] Interrupt received. Flushing and shutting down...")
        SHUTDOWN.set()
        _stop.set()

    signal.signal(signal.SIGINT, handle_sig)
    signal.signal(signal.SIGTERM, handle_sig)

    def status_printer():
        while not _stop.is_set():
            elapsed = time.monotonic() - start_wall
            print(
                f"\r[PRODUCER] Sent: {total_sent:>8,}  Errors: {total_errors}  "
                f"Rate: {counter.rate():>7.0f} eps  Elapsed: {elapsed:.1f}s  Target: {rate_eps} eps",
                end="", flush=True
            )
            time.sleep(1)

    status_thread = threading.Thread(target=status_printer, daemon=True)
    status_thread.start()

    for i, payload in enumerate(events):
        if _stop.is_set():
            break

        now = time.monotonic()
        sleep_ns = (next_emit - now) * 1e9
        if sleep_ns > 0:
            time.sleep(sleep_ns / 1e9)
        next_emit = max(time.monotonic(), next_emit) + interval_ns / 1e9

        payload["emit_epoch_ms"] = int(time.time() * 1000)
        payload["emit_ts"] = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

        msg_bytes = json.dumps(payload).encode('utf-8')
        key = payload['dest'].encode('utf-8')

        try:
            if CONFLUENT:
                producer.produce(topic, value=msg_bytes, key=key, on_delivery=None)
                if i % 1000 == 0:
                    producer.poll(0)
            else:
                producer.send(topic, value=payload, key=key)
            counter.record()
            total_sent += 1
        except Exception:
            total_errors += 1

    _stop.set()
    print()

    if CONFLUENT:
        print("[PRODUCER] Flushing Confluent producer...")
        producer.flush(timeout=30)
    else:
        print("[PRODUCER] Flushing kafka-python producer...")
        producer.flush()

    elapsed = time.monotonic() - start_wall
    avg_rate = total_sent / elapsed if elapsed > 0 else 0
    print(f"[PRODUCER] Done. Emitted {total_sent:,} events in {elapsed:.1f}s "
          f"(avg {avg_rate:.0f} eps). Errors: {total_errors}")
    return total_sent, elapsed


def main():
    parser = argparse.ArgumentParser(description="HyperLogistics Kafka Producer (Time-Replay Engine)")
    parser.add_argument("--config", type=str, default=None, help="Path to YAML config file")
    parser.add_argument("--rate", type=int, default=1000,
                        help="Target emission rate in events/sec (default: 1000)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max events to emit (default: all)")
    parser.add_argument("--bootstrap", type=str, default=None,
                        help="Kafka bootstrap servers (overrides config)")
    parser.add_argument("--topic", type=str, default=None,
                        help="Kafka topic name (overrides config)")
    parser.add_argument("--loop", action="store_true",
                        help="Loop forever (restart after all events emitted)")
    args = parser.parse_args()

    if not HAS_KAFKA_CLIENT:
        print("ERROR: Install confluent-kafka or kafka-python:\n  pip install confluent-kafka")
        sys.exit(1)

    cfg = load_config(args.config)
    bootstrap_servers = args.bootstrap or cfg["kafka"]["bootstrap_servers"]
    topic = args.topic or cfg["kafka"]["topic"]
    raw_bts = cfg["paths"]["raw_bts"]
    bts_dir = raw_bts if os.path.isabs(raw_bts) else str(PROJECT_ROOT / raw_bts)

    print("=" * 60)
    print(f"HyperLogistics-Stream: Kafka Producer")
    print(f"Bootstrap: {bootstrap_servers}  Topic: {topic}")
    print(f"Target rate: {args.rate} eps  Limit: {args.limit or 'all'}")
    print("=" * 60)

    events = load_bts_2024_sorted(bts_dir, limit=args.limit)

    if CONFLUENT:
        print("[PRODUCER] Using confluent-kafka")
        producer = make_confluent_producer(bootstrap_servers)
    else:
        print("[PRODUCER] Using kafka-python")
        producer = make_kafka_python_producer(bootstrap_servers)

    run_count = 0
    while True:
        run_count += 1
        if args.loop and run_count > 1:
            print(f"\n[PRODUCER] Loop iteration {run_count} - restarting replay...")
        emit_events(events, topic, producer, rate_eps=args.rate)
        if not args.loop or SHUTDOWN.is_set():
            break


if __name__ == "__main__":
    main()
