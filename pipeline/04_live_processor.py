#!/usr/bin/env python3
"""
04_live_processor.py
Phase 6 (Plan V2): Spark Structured Streaming for the LIVE feeds.

Reads the three topics published by live/live_producer.py and writes Gold tables
for the dashboard's "Live Radar":

  weather         -> gold_live_weather   flight category (VFR/MVFR/IFR/LIFR), weather severity,
                                          optional join with a historical hub baseline
  airport_status  -> gold_live_airport_status   FAA delay severity (ground stop > ground delay > delay)
  live_flights    -> gold_live_hub_traffic      aircraft within RADIUS_KM of each hub (haversine)

Every Gold row keeps the `stale` flag + `stale_age_s` from the producer (dashboard STALE badge)
and `latency_ms` = Gold write time - producer emit time (for the p50/p95 story).

This script is separate from 03_stream_processor.py (replay + ML scoring) so the two
can be developed in parallel and run side by side.

Optional baseline (built by 01_batch_train.py, Plan A):
    models/baseline_hub_hour_dow.parquet
    columns: hub, hour_local, day_of_week (Spark dayofweek, Sun=1), typical_dep_delay,
             typical_arr_delay, median_*, dep_n, arr_n   (training years only)
The live row is matched on the hub's LOCAL hour and weekday. typical_delay_min is the mean of
the typical departure and arrival delay. If the file does not exist, baseline columns are null
and everything else still works.

Usage:
    python pipeline/04_live_processor.py                    # live, from latest offsets
    python pipeline/04_live_processor.py --starting earliest  # re-read everything in Kafka
    python pipeline/04_live_processor.py --test             # stop after a few batches
"""

import argparse
import os
import shutil
import sys
import time
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, LongType, StringType, StructField, StructType,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "pipeline_config.yaml"

# Hub coordinates (same hubs as dashboard/app.py HUB_COORDS).
HUBS = {
    "ATL": (33.6407, -84.4277), "ORD": (41.9742, -87.9073), "DFW": (32.8998, -97.0403),
    "DEN": (39.8561, -104.6737), "LAX": (33.9425, -118.4081), "JFK": (40.6413, -73.7781),
    "EWR": (40.6895, -74.1745), "SFO": (37.6213, -122.3790), "MEM": (35.0424, -89.9767),
    "SDF": (38.1744, -85.7360), "CVG": (39.0489, -84.6678), "CLT": (35.2140, -80.9431),
    "LAS": (36.0840, -115.1537), "PHX": (33.4352, -112.0101), "MIA": (25.7959, -80.2870),
    "IAH": (29.9844, -95.3414), "BOS": (42.3656, -71.0096), "SEA": (47.4502, -122.3088),
    "MSP": (44.8848, -93.2223), "DTW": (42.2162, -83.3554), "PHL": (39.8744, -75.2424),
    "BWI": (39.1774, -76.6684), "SLC": (40.7899, -111.9791), "MCO": (28.4312, -81.3081),
}

COMMON = [
    StructField("fetched_at", StringType(), True),
    StructField("stale", BooleanType(), True),
    StructField("stale_age_s", LongType(), True),
    StructField("emit_epoch_ms", LongType(), True),
]

WEATHER_SCHEMA = StructType([
    StructField("airport", StringType(), True),
    StructField("station", StringType(), True),
    StructField("obs_time", StringType(), True),
    StructField("description", StringType(), True),
    StructField("temp_c", DoubleType(), True),
    StructField("wind_kmh", DoubleType(), True),
    StructField("wind_gust_kmh", DoubleType(), True),
    StructField("visibility_m", DoubleType(), True),
    StructField("ceiling_m", DoubleType(), True),
    StructField("raw_metar", StringType(), True),
] + COMMON)

# FAA fields vary by delay type; keep everything as strings and parse defensively.
FAA_SCHEMA = StructType([
    StructField("airport", StringType(), True),
    StructField("delay_type", StringType(), True),
    StructField("list", StringType(), True),
    StructField("reason", StringType(), True),
    StructField("avg", StringType(), True),
    StructField("min", StringType(), True),
    StructField("max", StringType(), True),
    StructField("trend", StringType(), True),
] + COMMON)

FLIGHT_SCHEMA = StructType([
    StructField("icao24", StringType(), True),
    StructField("callsign", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("baro_altitude", DoubleType(), True),
    StructField("on_ground", BooleanType(), True),
    StructField("velocity_ms", DoubleType(), True),
    StructField("vertical_rate", DoubleType(), True),
] + COMMON)


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


def load_config(path=None):
    resolved = path or os.environ.get("HYPERLOGISTICS_CONFIG", str(CONFIG_PATH))
    if not os.path.isabs(resolved):
        resolved = str(PROJECT_ROOT / resolved)
    with open(resolved) as f:
        return yaml.safe_load(f)


def cfg_path(cfg: dict, key: str, default: str) -> str:
    p = cfg.get("paths", {}).get(key, default)
    return p if os.path.isabs(p) else str(PROJECT_ROOT / p)


def build_spark(cores: int, shuffle_partitions: int) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("HyperLogistics-Live")
        .master(f"local[{cores}]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_topic(spark, bootstrap, topic, schema, starting, max_offsets):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", starting)
        .option("maxOffsetsPerTrigger", str(max_offsets))
        .option("failOnDataLoss", "false")
        .load()
        .select(F.from_json(F.col("value").cast("string"), schema).alias("d"))
        .select("d.*")
        .filter(F.col("airport").isNotNull() if "airport" in schema.fieldNames()
                else F.col("icao24").isNotNull())
    )


def with_gold_meta(df):
    """Add write time and end-to-end latency (producer emit -> Gold write)."""
    now_ms = (F.unix_timestamp(F.current_timestamp()) * 1000).cast("long")
    return (
        df.withColumn("gold_write_epoch_ms", now_ms)
          .withColumn("latency_ms", F.col("gold_write_epoch_ms") - F.col("emit_epoch_ms"))
          .withColumn("gold_date", F.to_date(F.current_timestamp()))
    )


# ----------------------------- transforms ---------------------------------

def transform_weather(df, baseline):
    vis_mi = F.col("visibility_m") / F.lit(1609.34)
    ceil_ft = F.col("ceiling_m") * F.lit(3.281)
    gust = F.coalesce(F.col("wind_gust_kmh"), F.col("wind_kmh"), F.lit(0.0))

    category = (
        F.when((vis_mi < 1) | (ceil_ft < 500), "LIFR")
         .when((vis_mi < 3) | (ceil_ft < 1000), "IFR")
         .when((vis_mi <= 5) | (ceil_ft <= 3000), "MVFR")
         .otherwise("VFR")
    )
    base_sev = (
        F.when(category == "LIFR", 3).when(category == "IFR", 2)
         .when(category == "MVFR", 1).otherwise(0)
    )
    # Strong gusts bump the severity by one level (cap at 3). ~50 km/h = ~27 kt.
    sev = F.least(F.lit(3), base_sev + F.when(gust >= 50, 1).otherwise(0))

    tz_map = F.create_map(*[x for k, v in AIRPORT_TZ.items() for x in (F.lit(k), F.lit(v))])
    local_ts = F.from_utc_timestamp(F.current_timestamp(), tz_map[F.col("airport")])

    out = (
        df.withColumn("flight_category", category)
          .withColumn("weather_severity", sev.cast("int"))
          .withColumn("obs_ts", F.to_timestamp("obs_time"))
          # Baseline is keyed on the hub's LOCAL hour and Spark dayofweek (Sun=1), same as 01_batch_train.py
          .withColumn("hour_local", F.hour(local_ts).cast("int"))
          .withColumn("day_of_week", F.dayofweek(local_ts).cast("int"))
    )
    if baseline is not None:
        out = out.join(F.broadcast(baseline), on=["airport", "day_of_week", "hour_local"], how="left")
    else:
        out = (out.withColumn("typical_delay_min", F.lit(None).cast("double"))
                  .withColumn("typical_dep_delay_min", F.lit(None).cast("double"))
                  .withColumn("typical_arr_delay_min", F.lit(None).cast("double")))
    return out


def transform_faa(df):
    t = F.lower(F.coalesce(F.col("delay_type"), F.lit("")))
    sev = (
        F.when(t.contains("ground stop"), 3)
         .when(t.contains("ground delay"), 2)
         .when(t.contains("arrival") | t.contains("departure") | t.contains("delay"), 1)
         .otherwise(0)
    )
    # Closures are mostly non-scheduled GA notices (noise) so they get severity 0 (kept, not scored).
    sev = F.when(t.contains("closure"), 0).otherwise(sev)
    return (
        df.withColumn("delay_severity", sev.cast("int"))
          .withColumn("avg_delay_min", F.regexp_extract("avg", r"(\d+)", 1).cast("double"))
          .withColumn("is_actionable", F.col("delay_severity") > 0)
    )


def transform_traffic(df, hubs_df, radius_km):
    keyed = df.withColumn("_snap_key", F.coalesce(F.col("fetched_at"), F.lit("")))
    latest_key = keyed.agg(F.max("_snap_key").alias("_snap_key")).filter(F.col("_snap_key").isNotNull())
    snap = keyed.join(latest_key, on="_snap_key", how="inner").drop("_snap_key").dropDuplicates(["icao24"])

    meta = (
        snap.agg(
            F.count("*").alias("_snap_n"),
            F.max("emit_epoch_ms").alias("emit_epoch_ms"),
            F.max(F.col("stale").cast("int")).cast("boolean").alias("stale"),
            F.max("stale_age_s").alias("stale_age_s"),
            F.max("fetched_at").alias("fetched_at"),
        )
        .filter(F.col("_snap_n") > 0)
        .drop("_snap_n")
    )

    ac = snap.filter((F.col("on_ground") == False) & F.col("lat").isNotNull() & F.col("lon").isNotNull())  # noqa: E712
    j = ac.crossJoin(F.broadcast(hubs_df))
    dlat = F.radians(F.col("hub_lat") - F.col("lat"))
    dlon = F.radians(F.col("hub_lon") - F.col("lon"))
    a = (F.sin(dlat / 2) ** 2) + F.cos(F.radians(F.col("lat"))) * F.cos(F.radians(F.col("hub_lat"))) * (F.sin(dlon / 2) ** 2)
    dist_km = F.lit(2 * 6371.0) * F.asin(F.sqrt(a))
    counts = (
        j.withColumn("dist_km", dist_km)
        .filter(F.col("dist_km") <= radius_km)
        .groupBy("airport")
        .agg(
            F.count("*").alias("aircraft_nearby"),
            F.sum(F.when(F.col("baro_altitude") < 3000, 1).otherwise(0)).alias("low_altitude_count"),
            F.sum(F.when(F.col("vertical_rate") < -1.0, 1).otherwise(0)).alias("descending_count"),
            F.avg("baro_altitude").alias("avg_altitude_m"),
        )
    )

    base = F.broadcast(hubs_df.select("airport")).crossJoin(meta)
    return (
        base.join(counts, on="airport", how="left")
        .select(
            F.col("airport"),
            F.coalesce(F.col("aircraft_nearby"), F.lit(0)).cast("long").alias("aircraft_nearby"),
            F.coalesce(F.col("low_altitude_count"), F.lit(0)).cast("long").alias("low_altitude_count"),
            F.coalesce(F.col("descending_count"), F.lit(0)).cast("long").alias("descending_count"),
            F.col("avg_altitude_m").cast("double").alias("avg_altitude_m"),
            F.col("emit_epoch_ms"),
            F.col("stale"),
            F.col("stale_age_s"),
            F.col("fetched_at"),
        )
        .withColumn("radius_km", F.lit(float(radius_km)))
        .withColumn("snapshot_ts", F.current_timestamp())
    )


# ------------------------------- runner -----------------------------------

def start_query(name, stream_df, out_path, ckpt_root, trigger_sec, transform):
    def write_batch(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return
        t0 = time.time()
        out = with_gold_meta(transform(batch_df))
        (out.coalesce(1).write.mode("append").partitionBy("gold_date").parquet(out_path))
        print(f"  [{name}] batch {batch_id} written in {(time.time() - t0) * 1000:.0f} ms -> {out_path}")

    return (
        stream_df.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", os.path.join(ckpt_root, name))
        .trigger(processingTime=f"{trigger_sec} seconds")
        .queryName(name)
        .start()
    )


def main():
    ap = argparse.ArgumentParser(description="HyperLogistics live Spark processor")
    ap.add_argument("--config", default=None)
    ap.add_argument("--bootstrap", default=None)
    ap.add_argument("--cores", type=int, default=2)
    ap.add_argument("--shuffle-partitions", type=int, default=8)
    ap.add_argument("--trigger-interval", type=int, default=5)
    ap.add_argument("--starting", default="latest", choices=["latest", "earliest"])
    ap.add_argument("--radius-km", type=float, default=100.0, help="Hub traffic radius")
    ap.add_argument("--reset", action="store_true", help="Delete live checkpoints before starting")
    ap.add_argument("--test", action="store_true", help="Run ~60 s then stop")
    args = ap.parse_args()

    cfg = load_config(args.config)
    bootstrap = args.bootstrap or cfg["kafka"]["bootstrap_servers"]

    out_weather = cfg_path(cfg, "gold_live_weather", "data/gold/live/weather")
    out_faa = cfg_path(cfg, "gold_live_airport_status", "data/gold/live/airport_status")
    out_traffic = cfg_path(cfg, "gold_live_hub_traffic", "data/gold/live/hub_traffic")
    ckpt_root = os.path.join(cfg_path(cfg, "checkpoints", "checkpoints"), "live")
    baseline_path = os.path.join(cfg_path(cfg, "models", "models"), "baseline_hub_hour_dow.parquet")

    if args.reset:
        shutil.rmtree(ckpt_root, ignore_errors=True)
        print(f"[LIVE] Removed checkpoints at {ckpt_root}")

    print("=" * 60)
    print("HyperLogistics-Stream: LIVE Spark processor")
    print(f"Bootstrap={bootstrap}  starting={args.starting}  trigger={args.trigger_interval}s")
    print("=" * 60)

    spark = build_spark(args.cores, args.shuffle_partitions)

    baseline = None
    if Path(baseline_path).exists():
        raw = spark.read.parquet(baseline_path)
        dep, arr = F.col("typical_dep_delay"), F.col("typical_arr_delay")
        baseline = raw.select(
            F.col("hub").alias("airport"),
            F.col("day_of_week").cast("int").alias("day_of_week"),
            F.col("hour_local").cast("int").alias("hour_local"),
            dep.alias("typical_dep_delay_min"),
            arr.alias("typical_arr_delay_min"),
            F.when(dep.isNotNull() & arr.isNotNull(), (dep + arr) / 2)
             .otherwise(F.coalesce(dep, arr)).alias("typical_delay_min"),
        ).cache()
        print(f"[LIVE] Loaded hub baseline: {baseline.count():,} hub-hour-weekday cells")
    else:
        print(f"[LIVE] No baseline at {baseline_path}. typical_delay_min will be null (run 01_batch_train.py to build it).")

    hubs_df = spark.createDataFrame(
        [(k, v[0], v[1]) for k, v in HUBS.items()], ["airport", "hub_lat", "hub_lon"]
    )

    queries = [
        start_query("weather",
                    read_topic(spark, bootstrap, "weather", WEATHER_SCHEMA, args.starting, 1000),
                    out_weather, ckpt_root, args.trigger_interval,
                    lambda d: transform_weather(d, baseline)),
        start_query("airport_status",
                    read_topic(spark, bootstrap, "airport_status", FAA_SCHEMA, args.starting, 1000),
                    out_faa, ckpt_root, args.trigger_interval, transform_faa),
        start_query("hub_traffic",
                    read_topic(spark, bootstrap, "live_flights", FLIGHT_SCHEMA, args.starting, 20000),
                    out_traffic, ckpt_root, args.trigger_interval,
                    lambda d: transform_traffic(d, hubs_df, args.radius_km)),
    ]
    print("[LIVE] 3 queries started: weather, airport_status, hub_traffic. Ctrl+C to stop.\n")

    try:
        if args.test:
            time.sleep(60)
            print("[LIVE] Test window over. Stopping.")
        else:
            spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n[LIVE] Shutdown signal received.")
    finally:
        for q in queries:
            try:
                q.stop()
            except Exception:
                pass
        spark.stop()
        print("[LIVE] Stopped cleanly.")


if __name__ == "__main__":
    sys.exit(main())
