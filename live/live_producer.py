#!/usr/bin/env python3
"""
live/live_producer.py
Live ingestion: poll OpenSky, FAA NAS and NWS on their own schedules and publish
to Kafka topics for Spark Structured Streaming.

Topics (defaults, override with flags):
    live_flights    <- OpenSky aircraft states        (every 120 s)
    airport_status  <- FAA NAS delays / ground stops  (every 60 s)
    weather         <- NWS observations per hub       (every 5 min)

Resilience:
  * If a fetch fails, FAA and weather re-publish the last good rows flagged
    stale=true (with stale_age_s), so the dashboard shows a STALE badge instead of going blank.
  * OpenSky is large, so on failure it is skipped (not re-published).
  * --record saves every published message to JSONL so you can replay a real live
    session later if the APIs fail on demo day.

Usage:
    python live/live_producer.py                       # publish to Kafka
    python live/live_producer.py --dry-run             # fetch + print, no Kafka
    python live/live_producer.py --record              # also save data/live_recordings/*.jsonl
    python live/live_producer.py --no-opensky          # FAA + weather only
"""

import argparse
import json
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml

sys.path.insert(0, os.path.dirname(__file__))
from _common import HUBS, PROJECT_ROOT, RateLimitError  # noqa: E402
from fetch_faa_nas import fetch_faa  # noqa: E402
from fetch_nws_weather import fetch_hub_weather  # noqa: E402
from fetch_opensky import fetch_opensky  # noqa: E402

try:
    from confluent_kafka import Producer
    CONFLUENT = True
except ImportError:
    try:
        from kafka import KafkaProducer
        CONFLUENT = False
    except ImportError:
        Producer = KafkaProducer = None
        CONFLUENT = None  # only OK with --dry-run

CONFIG_PATH = PROJECT_ROOT / "config" / "pipeline_config.yaml"
RECORD_DIR = PROJECT_ROOT / "data" / "live_recordings"
SHUTDOWN = threading.Event()
PRINT_LOCK = threading.Lock()


def log(msg: str):
    with PRINT_LOCK:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def load_config(path=None):
    resolved = path or os.environ.get("HYPERLOGISTICS_CONFIG", str(CONFIG_PATH))
    if not os.path.isabs(resolved):
        resolved = str(PROJECT_ROOT / resolved)
    with open(resolved) as f:
        return yaml.safe_load(f)


class Sink:
    """Publishes dict messages to Kafka (or prints / records them)."""

    def __init__(self, bootstrap: str, dry_run: bool, record: bool):
        self.dry_run = dry_run
        self._lock = threading.Lock()
        self._rec_file = None
        self.producer = None

        if record:
            RECORD_DIR.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            path = RECORD_DIR / f"live_session_{stamp}.jsonl"
            self._rec_file = open(path, "a", encoding="utf-8")
            log(f"Recording session -> {path}")

        if not dry_run:
            if CONFLUENT is None:
                sys.exit("ERROR: install confluent-kafka or kafka-python (or use --dry-run)")
            if CONFLUENT:
                self.producer = Producer({
                    "bootstrap.servers": bootstrap,
                    "linger.ms": 50,
                    "compression.type": "lz4",
                })
            else:
                self.producer = KafkaProducer(
                    bootstrap_servers=bootstrap,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                )

    def send(self, topic: str, rows: list[dict], key_field: str):
        if not rows:
            return
        if self._rec_file:
            with self._lock:
                for r in rows:
                    self._rec_file.write(json.dumps({"topic": topic, "value": r}) + "\n")
                self._rec_file.flush()

        if self.dry_run:
            return

        for r in rows:
            key = str(r.get(key_field) or "").encode("utf-8")
            if CONFLUENT:
                self.producer.produce(topic, value=json.dumps(r).encode("utf-8"), key=key)
                self.producer.poll(0)
            else:
                self.producer.send(topic, value=r, key=key)

    def close(self):
        if self.producer:
            self.producer.flush(30) if CONFLUENT else self.producer.flush()
        if self._rec_file:
            self._rec_file.close()


class Source:
    """One polled feed with last-known-good caching and stale re-publish."""

    def __init__(self, name, topic, interval, fetch, key_field, republish_stale=True):
        self.name, self.topic, self.interval = name, topic, interval
        self.fetch, self.key_field = fetch, key_field
        self.republish_stale = republish_stale
        self.last_rows: list[dict] = []
        self.last_ok_epoch: float | None = None
        self.sent = self.failures = 0
        self.sink_failures: int = 0
        self.cooldown_until: float = 0.0

    def _stamp(self, rows, stale: bool):
        age = int(time.time() - self.last_ok_epoch) if self.last_ok_epoch else None
        out = []
        for r in rows:
            r = dict(r)
            r["stale"] = stale
            r["stale_age_s"] = age if stale else 0
            r["emit_epoch_ms"] = int(time.time() * 1000)
            out.append(r)
        return out

    def poll_once(self, sink: Sink):
        payload: list[dict] = []
        is_stale = False
        fetch_err: Exception | None = None

        try:
            rows = self.fetch()
            self.last_rows, self.last_ok_epoch = rows, time.time()
            payload = self._stamp(rows, stale=False)
        except RateLimitError as e:
            self.failures += 1
            fetch_err = e
            self.cooldown_until = max(self.cooldown_until, time.monotonic() + e.retry_after_s)
            if self.republish_stale and self.last_rows:
                payload = self._stamp(self.last_rows, stale=True)
                is_stale = True
            else:
                log(f"{self.name:<8} 429   ({e}) -> backing off {e.retry_after_s:.0f}s")
                return
        except Exception as e:
            self.failures += 1
            fetch_err = e
            if self.republish_stale and self.last_rows:
                payload = self._stamp(self.last_rows, stale=True)
                is_stale = True
            else:
                log(f"{self.name:<8} FAIL  ({type(e).__name__}: {e}) -> skipped")
                return

        try:
            sink.send(self.topic, payload, self.key_field)
        except Exception as sink_err:
            self.sink_failures += 1
            log(f"{self.name:<8} SINK FAIL ({type(sink_err).__name__}: {sink_err}) -> {self.topic}")
            return

        if is_stale:
            log(f"{self.name:<8} FAIL  ({type(fetch_err).__name__}: {fetch_err}) -> re-sent {len(payload)} STALE rows")
        else:
            self.sent += len(payload)
            log(f"{self.name:<8} OK    {len(payload):>5} rows -> {self.topic}")

    def run(self, sink: Sink):
        while not SHUTDOWN.is_set():
            started = time.monotonic()
            try:
                self.poll_once(sink)
            except Exception as exc:
                log(f"{self.name:<8} UNEXPECTED ({type(exc).__name__}: {exc})")
            wait = max(
                1.0,
                self.interval - (time.monotonic() - started),
                self.cooldown_until - time.monotonic(),
            )
            SHUTDOWN.wait(wait)


def fetch_all_weather(hubs):
    """All hubs; tolerate individual hub failures but fail if everything failed."""
    rows, errors = [], 0
    for iata in hubs:
        if SHUTDOWN.is_set():
            break
        try:
            rows.append(fetch_hub_weather(iata))
        except Exception:
            errors += 1
    if not rows:
        raise RuntimeError(f"all {errors} hub weather requests failed")
    return rows


def fetch_opensky_airborne():
    """Keep only airborne aircraft with a position (drops ~10% noise)."""
    return [r for r in fetch_opensky() if not r["on_ground"] and r["lat"] is not None]


def main():
    ap = argparse.ArgumentParser(description="HyperLogistics live feed producer")
    ap.add_argument("--config", default=None)
    ap.add_argument("--bootstrap", default=None, help="Kafka bootstrap servers (overrides config)")
    ap.add_argument("--dry-run", action="store_true", help="Fetch and log, but do not publish")
    ap.add_argument("--record", action="store_true", help="Save published messages to JSONL")
    ap.add_argument("--no-opensky", action="store_true")
    ap.add_argument("--once", action="store_true", help="Poll each source once, then exit")
    ap.add_argument("--opensky-interval", type=int, default=120)
    ap.add_argument("--faa-interval", type=int, default=60)
    ap.add_argument("--weather-interval", type=int, default=300)
    ap.add_argument("--hubs", nargs="*", default=HUBS, help="IATA codes for weather")
    args = ap.parse_args()

    cfg = load_config(args.config)
    bootstrap = args.bootstrap or cfg["kafka"]["bootstrap_servers"]
    hubs = [h.upper() for h in args.hubs]

    sources = [
        Source("faa", "airport_status", args.faa_interval,
               lambda: fetch_faa()[0], "airport"),
        Source("weather", "weather", args.weather_interval,
               lambda: fetch_all_weather(hubs), "airport"),
    ]
    if not args.no_opensky:
        sources.append(Source("opensky", "live_flights", args.opensky_interval,
                              fetch_opensky_airborne, "icao24", republish_stale=False))

    log("=" * 56)
    log(f"HyperLogistics live producer | bootstrap={bootstrap} | dry_run={args.dry_run}")
    log("Topics: " + ", ".join(f"{s.topic}({s.interval}s)" for s in sources))
    log("=" * 56)

    sink = Sink(bootstrap, args.dry_run, args.record)

    def stop(*_):
        log("Shutdown requested, flushing...")
        SHUTDOWN.set()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    if args.once:
        for s in sources:
            s.poll_once(sink)
        sink.close()
        return

    threads = [threading.Thread(target=s.run, args=(sink,), daemon=True, name=s.name) for s in sources]
    for t in threads:
        t.start()
    try:
        while not SHUTDOWN.is_set():
            SHUTDOWN.wait(1)
    finally:
        SHUTDOWN.set()
        for t in threads:
            t.join(timeout=5)
        sink.close()
        for s in sources:
            log(f"{s.name:<8} total sent={s.sent:,} failures={s.failures} sink_failures={s.sink_failures}")


if __name__ == "__main__":
    main()
