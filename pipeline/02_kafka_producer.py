#!/usr/bin/env python3
"""
02_kafka_producer.py
Phase 2 Ingestion: Kafka Time-Replay Engine.

Replays BTS 2024 flight departure events in chronological WheelsOff order,
emitting configurable rates (1k / 5k / 20k events/sec) to the 'flights_stream' topic.

Message payload (JSON):
{
  "flight_id": "AA1234_20240108_ORD_DFW",
  "tail_number": "N485PX",
  "carrier": "AA",
  "origin": "ORD",
  "dest": "DFW",
  "crs_dep_time_utc": "2024-01-08T14:30:00Z",
  "wheels_off_utc": "2024-01-08T14:45:00Z",
  "crs_arr_time_utc": "2024-01-08T17:15:00Z",
  "dep_delay": 15.0,
  "distance": 802.0,
  "emit_ts": "2024-01-08T14:45:00Z"
}

Usage:
    python pipeline/02_kafka_producer.py --rate 1000
    python pipeline/02_kafka_producer.py --rate 5000 --limit 100000
"""

import os
import sys
import csv
import glob
import json
import time
import signal
import argparse
import threading
from pathlib import Path
from datetime import datetime, timedelta
from collections import deque
import zoneinfo

import yaml

try:
    from confluent_kafka import Producer, KafkaException
    CONFLUENT = True
except ImportError:
    try:
        from kafka import KafkaProducer
        CONFLUENT = False
    except ImportError:
        print("ERROR: Install confluent-kafka or kafka-python:\n  pip install confluent-kafka")
        sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = Path(os.environ.get("HYPERLOGISTICS_CONFIG", PROJECT_ROOT / "config" / "pipeline_config.yaml"))

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


def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def hhmm_to_utc(flight_date_str: str, hhmm_float, airport_code: str):
    """Convert BTS HHMM local time to UTC datetime string."""
    if hhmm_float is None:
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
        utc_dt = local_aware.astimezone(zoneinfo.ZoneInfo('UTC'))
        return utc_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    except Exception:
        return None


def parse_bts_row(row: dict):
    """Parse a BTS CSV row into a Kafka message payload. Returns None if invalid."""
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

        wheels_off_utc = hhmm_to_utc(flight_date, wheels_off_raw, origin)
        crs_dep_utc = hhmm_to_utc(flight_date, crs_dep_raw, origin)
        crs_arr_utc = hhmm_to_utc(flight_date, crs_arr_raw, dest)

        if not wheels_off_utc:
            return None

        flight_id = f"{carrier}{row.get('Flight_Number_Reporting_Airline','0').strip()}_{flight_date.replace('-','')}_{ origin}_{dest}"

        return {
            "flight_id": flight_id.replace(' ', ''),
            "tail_number": tail,
            "carrier": carrier,
            "origin": origin,
            "dest": dest,
            "crs_dep_time_utc": crs_dep_utc,
            "wheels_off_utc": wheels_off_utc,
            "crs_arr_time_utc": crs_arr_utc,
            "dep_delay": dep_delay,
            "distance": distance,
            "emit_ts": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
        }
    except (ValueError, TypeError):
        return None


def load_bts_2024_sorted(bts_dir: str, limit: int = None):
    """Load all 2024 BTS CSVs, parse into events, sort by WheelsOff UTC."""
    pattern = os.path.join(bts_dir, "*2024*.csv")
    csv_files = sorted(glob.glob(pattern))
    if not csv_files:
        raise FileNotFoundError(
            f"No 2024 BTS CSVs in {bts_dir}. "
            "Run: python scripts/download_bts.py --years 2024 --extract"
        )

    print(f"[PRODUCER] Loading BTS 2024 from {len(csv_files)} file(s)...")
    events = []
    for filepath in csv_files:
        print(f"  Parsing {Path(filepath).name}...")
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

    print(f"[PRODUCER] Parsed {len(events):,} valid events. Sorting by WheelsOff...")
    events.sort(key=lambda x: x[0])
    result = [e[1] for e in events]
    if limit:
        result = result[:limit]
    print(f"[PRODUCER] Ready to emit {len(result):,} events.")
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
    """Emit events at target rate with token-bucket throttling."""
    counter = ThroughputCounter(window_sec=5)
    total_sent = 0
    total_errors = 0
    interval_ns = int(1e9 / rate_eps)  # nanoseconds between events
    start_wall = time.monotonic()
    next_emit = start_wall

    _stop = threading.Event()

    def handle_sig(*_):
        print("\n[PRODUCER] Interrupt received. Flushing and shutting down...")
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

        # Token-bucket rate control
        now = time.monotonic()
        sleep_ns = (next_emit - now) * 1e9
        if sleep_ns > 0:
            time.sleep(sleep_ns / 1e9)
        next_emit = max(time.monotonic(), next_emit) + interval_ns / 1e9

        msg_bytes = json.dumps(payload).encode('utf-8')
        key = payload['dest'].encode('utf-8')

        try:
            if CONFLUENT:
                producer.produce(topic, value=msg_bytes, key=key, on_delivery=None)
                if i % 1000 == 0:
                    producer.poll(0)
            else:
                producer.send(topic, value=payload, key=key)
        except Exception as e:
            total_errors += 1

        counter.record()
        total_sent += 1

    _stop.set()
    print()  # newline after status line

    # Flush remaining
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

    cfg = load_config()
    bootstrap_servers = args.bootstrap or cfg["kafka"]["bootstrap_servers"]
    topic = args.topic or cfg["kafka"]["topic"]
    bts_dir = str(PROJECT_ROOT / cfg["paths"]["raw_bts"])

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
            print(f"\n[PRODUCER] Loop iteration {run_count} — restarting replay...")
        emit_events(events, topic, producer, rate_eps=args.rate)
        if not args.loop:
            break


if __name__ == "__main__":
    main()
