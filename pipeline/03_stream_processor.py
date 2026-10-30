#!/usr/bin/env python3
"""
03_stream_processor.py
Phase 3+4: Spark Structured Streaming - Feature Fusion, ML Scoring & Medallion Write.

Reads from Kafka 'flights_stream', performs:
  - Schema parse (from_json, static schema)
  - Broadcast join with static NOAA weather & lane mean tables
  - GBT model scoring (PipelineModel.transform) with aligned feature columns
  - Sliding window hub aggregations with 6-hour arrival watermark
  - Bronze / Silver / Gold Parquet writes with per-config checkpointing
  - Benchmark metrics logging (throughput, batch time, p50/p95 latency)

Usage:
    python pipeline/03_stream_processor.py --cores 4 --shuffle-partitions 8 --max-offsets 5000
"""

import argparse
import glob
import json
import os
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml
from py4j.protocol import Py4JError
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, LongType, StringType, StructField, StructType, TimestampType
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "pipeline_config.yaml"

FLIGHT_SCHEMA = StructType([
    StructField("flight_id", StringType(), True),
    StructField("tail_number", StringType(), True),
    StructField("carrier", StringType(), True),
    StructField("origin", StringType(), True),
    StructField("dest", StringType(), True),
    StructField("crs_dep_time_utc", StringType(), True),
    StructField("wheels_off_utc", StringType(), True),
    StructField("crs_arr_time_utc", StringType(), True),
    StructField("dep_delay", DoubleType(), True),
    StructField("distance", DoubleType(), True),
    StructField("emit_epoch_ms", LongType(), True),
    StructField("emit_ts", StringType(), True),
])

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


def resolve_path(cfg_paths: dict, key: str) -> str:
    p = cfg_paths[key]
    return p if os.path.isabs(p) else str(PROJECT_ROOT / p)


def build_spark(cores: int, shuffle_partitions: int, app_name: str = "HyperLogistics-Stream") -> SparkSession:
    core_str = f"local[{cores}]"
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(core_str)
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .config("spark.scheduler.mode", "FAIR")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_static_weather(spark: SparkSession, noaa_dir: str, year: int = 2024):
    """Load and materialize 2024 NOAA weather as a clean Parquet table for fast broadcast joins."""
    cache_pq = f"/tmp/weather_static_v2_{year}.parquet"  # v2: adds ceiling
    if not Path(cache_pq).exists():
        paths = glob.glob(os.path.join(noaa_dir, str(year), "*.csv"))
        if not paths:
            raise FileNotFoundError(f"No NOAA 2024 CSVs in {noaa_dir}/{year}/")

        raw_weather = (
            spark.read
            .option("header", "true")
            .csv(paths)
            .select(
                F.input_file_name().alias("_fp"),
                F.col("DATE").alias("raw_date"),
                F.col("WND"), F.col("VIS"), F.col("TMP"), F.col("CIG"),
            )
            .withColumn("icao", F.regexp_extract("_fp", r"([A-Z0-9]{4})_\d+\.csv$", 1))
            .filter(F.col("icao") != "")
            .withColumn("observed_utc", F.to_timestamp("raw_date"))
            .filter(F.col("observed_utc").isNotNull())
            .withColumn("utc_hour", F.date_trunc("hour", "observed_utc"))
            .withColumn("_wnd_raw", F.split(F.col("WND"), ",").getItem(3).cast(DoubleType()))
            .withColumn("_vis_raw", F.split(F.col("VIS"), ",").getItem(0).cast(DoubleType()))
            .withColumn("_tmp_raw", F.regexp_extract(F.col("TMP"), r"^([+\-]?\d+)", 1).cast(DoubleType()))
            .withColumn(
                "DestWindSpeed",
                F.when(F.col("_wnd_raw") >= 9999, F.lit(None)).otherwise(F.col("_wnd_raw") / 10.0)
            )
            .withColumn(
                "DestVisibility",
                F.when(F.col("_vis_raw") >= 999999, F.lit(None)).otherwise(F.col("_vis_raw"))
            )
            .withColumn(
                "DestTemp",
                F.when(F.abs(F.col("_tmp_raw")) >= 9999, F.lit(None)).otherwise(F.col("_tmp_raw") / 10.0)
            )
            .withColumn("_cig_raw", F.split(F.col("CIG"), ",").getItem(0).cast(DoubleType()))
            .withColumn(
                "DestCeiling",
                F.when(F.col("_cig_raw").isNull() | (F.col("_cig_raw") >= 99999), F.lit(22000.0))
                 .otherwise(F.col("_cig_raw"))
            )
            .select("icao", "utc_hour", "DestWindSpeed", "DestVisibility", "DestTemp", "DestCeiling")
            .dropDuplicates(["icao", "utc_hour"])
            .fillna({"DestWindSpeed": 3.5, "DestVisibility": 9000.0, "DestTemp": 15.0,
                     "DestCeiling": 22000.0})
        )
        raw_weather.coalesce(1).write.mode("overwrite").parquet(cache_pq)

    weather_df = spark.read.parquet(cache_pq).cache()
    print(f"[STREAM] Loaded {weather_df.count():,} static weather rows for broadcast join.")
    return weather_df


def load_lane_mean(spark: SparkSession, models_dir: str):
    """Load and cache pre-computed lane mean table from batch training."""
    lane_path = os.path.join(models_dir, "lane_mean.parquet")
    if not Path(lane_path).exists():
        print("[STREAM] Warning: lane_mean.parquet not found. LaneMeanDelay will be 0.0")
        schema = StructType([
            StructField("Origin", StringType()), StructField("Dest", StringType()),
            StructField("LaneMeanDelay", DoubleType()),
        ])
        return spark.createDataFrame([], schema)
    df = spark.read.parquet(lane_path).cache()
    print(f"[STREAM] Loaded {df.count():,} lane mean entries.")
    return df


def apply_output_root(cfg: dict, output_root: str) -> dict:
    """Redirect Bronze/Silver/Gold/checkpoints under one root (used by benchmark runs)."""
    root = output_root if os.path.isabs(output_root) else str(PROJECT_ROOT / output_root)
    paths = dict(cfg["paths"])
    paths["bronze"] = os.path.join(root, "bronze")
    paths["silver"] = os.path.join(root, "silver")
    paths["gold_predictions"] = os.path.join(root, "gold", "predictions")
    paths["gold_congestion"] = os.path.join(root, "gold", "hub_congestion")
    paths["checkpoints"] = os.path.join(root, "checkpoints")
    return {**cfg, "paths": paths}


def reset_outputs(targets: list):
    """Delete Medallion outputs and checkpoints so a replay starts clean."""
    import shutil
    for t in targets:
        p = Path(t).resolve()
        # Safety: only wipe directories that live inside the project
        if PROJECT_ROOT not in p.parents:
            print(f"[RESET] Refusing to delete outside project: {p}")
            continue
        if p.exists():
            shutil.rmtree(p, ignore_errors=True)
            print(f"[RESET] Removed {p}")


class BatchMetricsLogger:
    """Collects per-batch latency and throughput for RQ2 benchmarks."""

    def __init__(self, output_path: str, config_label: str):
        self.output_path = Path(output_path)
        self.output_path.mkdir(parents=True, exist_ok=True)
        self.config_label = config_label
        self.records = []
        self._lock = threading.Lock()

    def log_batch(self, batch_id: int, num_rows: int, start_ts: float, end_ts: float,
                  emit_timestamps: list = None):
        duration_ms = (end_ts - start_ts) * 1000
        throughput = num_rows / (end_ts - start_ts) if (end_ts - start_ts) > 0 else 0

        p50_latency_ms = None
        p95_latency_ms = None
        if emit_timestamps:
            import numpy as np
            now_ts = end_ts * 1000
            latencies = sorted([max(0.0, now_ts - ts) for ts in emit_timestamps if ts and ts > 0])
            if latencies:
                p50_latency_ms = float(np.percentile(latencies, 50))
                p95_latency_ms = float(np.percentile(latencies, 95))

        record = {
            "config": self.config_label,
            "batch_id": batch_id,
            "num_rows": num_rows,
            "duration_ms": round(duration_ms, 1),
            "throughput_rps": round(throughput, 1),
            "p50_latency_ms": round(p50_latency_ms, 1) if p50_latency_ms is not None else None,
            "p95_latency_ms": round(p95_latency_ms, 1) if p95_latency_ms is not None else None,
            "wall_ts": datetime.now(timezone.utc).isoformat(),
        }
        with self._lock:
            self.records.append(record)

        print(
            f"  [Batch {batch_id:4d}] rows={num_rows:6,}  dur={duration_ms:7.1f}ms  "
            f"tput={throughput:8.0f} rps  "
            f"p50={p50_latency_ms:.0f}ms  p95={p95_latency_ms:.0f}ms"
            if p50_latency_ms is not None else
            f"  [Batch {batch_id:4d}] rows={num_rows:6,}  dur={duration_ms:7.1f}ms  "
            f"tput={throughput:8.0f} rps"
        )

    def flush(self):
        if not self.records:
            return
        outfile = self.output_path / f"metrics_{self.config_label}.jsonl"
        with open(outfile, "w") as f:
            for r in self.records:
                f.write(json.dumps(r) + "\n")
        print(f"[METRICS] Saved {len(self.records)} batch records -> {outfile}")


def run_streaming_pipeline(
    spark: SparkSession,
    cfg: dict,
    bootstrap_servers: str,
    max_offsets: int,
    trigger_interval_sec: int,
    metrics_logger: BatchMetricsLogger,
    test_mode: bool = False,
    reset: bool = False,
    dedupe: bool = True,
    starting_offsets: str = "earliest",
):
    noaa_dir = resolve_path(cfg["paths"], "raw_noaa")
    models_dir = resolve_path(cfg["paths"], "models")
    bronze_path = resolve_path(cfg["paths"], "bronze")
    silver_path = resolve_path(cfg["paths"], "silver")
    gold_path = resolve_path(cfg["paths"], "gold_predictions")
    gold_agg_path = resolve_path(cfg["paths"], "gold_congestion")
    checkpoint_root = os.path.join(resolve_path(cfg["paths"], "checkpoints"), metrics_logger.config_label)
    if reset:
        reset_outputs([bronze_path, silver_path, gold_path, gold_agg_path, checkpoint_root])
    elif test_mode and os.path.exists(checkpoint_root):
        import shutil
        shutil.rmtree(checkpoint_root, ignore_errors=True)
    topic = cfg["kafka"]["topic"]
    high_risk_thresh = cfg["streaming"]["high_risk_threshold_min"]

    weather_static = load_static_weather(spark, noaa_dir)
    lane_mean_static = load_lane_mean(spark, models_dir)

    gbt_model_path = os.path.join(models_dir, "gbt_pipeline_v1")
    if not Path(gbt_model_path).exists():
        print("[STREAM] WARNING: GBT model not found. Run 01_batch_train.py first.")
        print("[STREAM] Using zero-prediction fallback for demo mode.")
        gbt_model = None
    else:
        print(f"[STREAM] Loading GBT model from {gbt_model_path}...")
        gbt_model = PipelineModel.load(gbt_model_path)
        print("[STREAM] GBT model loaded.")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("maxOffsetsPerTrigger", str(max_offsets))
        .option("failOnDataLoss", "false")
        .load()
    )

    window_dur = cfg["streaming"]["window_duration"]
    slide_dur = cfg["streaming"]["slide_duration"]

    parsed_stream = (
        raw_stream
        .select(
            F.col("offset").alias("kafka_offset"),
            F.col("partition").alias("kafka_partition"),
            F.col("timestamp").alias("kafka_ts"),
            F.col("value").cast("string").alias("raw_json"),
            F.from_json(F.col("value").cast("string"), FLIGHT_SCHEMA).alias("data"),
        )
        .select("kafka_offset", "kafka_partition", "kafka_ts", "raw_json", "data.*")
        .filter(F.col("flight_id").isNotNull())
        .withColumn("wheels_off_ts", F.to_timestamp("wheels_off_utc"))
        .withColumn("crs_arr_ts", F.to_timestamp("crs_arr_time_utc"))
        .withColumn(
            "emit_epoch_ms",
            F.coalesce(
                F.col("emit_epoch_ms"),
                (F.unix_timestamp(F.to_timestamp("emit_ts")) * 1000).cast("long")
            )
        )
        .withColumn("sched_arr_hour_utc", F.date_trunc("hour", "crs_arr_ts"))
        .withColumn("dest_icao", F.concat(F.lit("K"), F.col("dest")))
        .withColumn("origin_icao", F.concat(F.lit("K"), F.col("origin")))
        .withColumn("wheels_off_hour_utc", F.date_trunc("hour", "wheels_off_ts"))
        .withColumn("DepDelay", F.col("dep_delay").cast(DoubleType()))
        .withColumn("Distance", F.col("distance").cast(DoubleType()))
        .withColumn("DepHourUTC", F.hour("wheels_off_ts").cast(DoubleType()))
        .withColumn("ArrHourUTC", F.hour("crs_arr_ts").cast(DoubleType()))
        .withColumn("DayOfWeek", F.dayofweek(F.to_date("wheels_off_ts")).cast("int"))
        .filter(F.col("DepDelay").isNotNull() & F.col("Distance").isNotNull())
    )

    weather_renamed = weather_static.select(
        F.col("icao").alias("w_icao"),
        F.col("utc_hour").alias("w_utc_hour"),
        F.col("DestWindSpeed"),
        F.col("DestVisibility"),
        F.col("DestTemp"),
        F.col("DestCeiling"),
    )
    origin_renamed = weather_static.select(
        F.col("icao").alias("o_icao"),
        F.col("utc_hour").alias("o_utc_hour"),
        F.col("DestWindSpeed").alias("OriginWindSpeed"),
        F.col("DestVisibility").alias("OriginVisibility"),
        F.col("DestTemp").alias("OriginTemp"),
        F.col("DestCeiling").alias("OriginCeiling"),
    )

    lane_renamed = lane_mean_static.select(
        F.col("Origin").alias("l_origin"),
        F.col("Dest").alias("l_dest"),
        F.col("LaneMeanDelay"),
    )

    enriched_stream = (
        parsed_stream
        .join(
            F.broadcast(weather_renamed),
            (parsed_stream.dest_icao == weather_renamed.w_icao) &
            (parsed_stream.sched_arr_hour_utc == weather_renamed.w_utc_hour),
            how="left"
        )
        .drop("w_icao", "w_utc_hour")
        .join(
            F.broadcast(origin_renamed),
            (parsed_stream.origin_icao == origin_renamed.o_icao) &
            (parsed_stream.wheels_off_hour_utc == origin_renamed.o_utc_hour),
            how="left"
        )
        .drop("o_icao", "o_utc_hour")
        .join(
            F.broadcast(lane_renamed),
            (parsed_stream.origin == lane_renamed.l_origin) &
            (parsed_stream.dest == lane_renamed.l_dest),
            how="left"
        )
        .drop("l_origin", "l_dest")
        .fillna({
            "DestWindSpeed": 3.5, "DestVisibility": 9000.0, "DestTemp": 15.0, "DestCeiling": 22000.0,
            "OriginWindSpeed": 3.5, "OriginVisibility": 9000.0, "OriginTemp": 15.0,
            "OriginCeiling": 22000.0, "LaneMeanDelay": 0.0,
        })
    )

    if gbt_model:
        scored_stream = gbt_model.transform(enriched_stream)
    else:
        scored_stream = enriched_stream.withColumn("prediction", F.lit(0.0))

    today_col = F.to_date(F.current_timestamp())
    unified_stream = (
        scored_stream
        .withColumn("predicted_delta_delay", F.col("prediction"))
        .withColumn(
            "predicted_eta_utc",
            F.from_unixtime(
                F.unix_timestamp("crs_arr_ts") +
                F.col("dep_delay") * 60 +
                F.col("prediction") * 60
            ).cast(TimestampType())
        )
        .withColumn("is_high_risk", F.col("prediction") > F.lit(high_risk_thresh))
        .withColumn("ingest_date", today_col)
        .withColumn("enrich_date", today_col)
        .withColumn("gold_date", today_col)
    )

    silver_cols = [
        "kafka_ts", "flight_id", "tail_number", "carrier", "origin", "dest",
        "crs_dep_time_utc", "wheels_off_utc", "crs_arr_time_utc",
        "emit_epoch_ms", "emit_ts",
        "wheels_off_ts", "crs_arr_ts", "sched_arr_hour_utc", "dest_icao",
        "DepDelay", "Distance", "DepHourUTC", "ArrHourUTC", "DayOfWeek",
        "DestWindSpeed", "DestVisibility", "DestTemp", "DestCeiling",
        "OriginWindSpeed", "OriginVisibility", "OriginTemp", "OriginCeiling", "LaneMeanDelay", "enrich_date",
    ]
    gold_cols = [
        "flight_id", "carrier", "origin", "dest", "tail_number",
        "wheels_off_ts", "crs_arr_ts", "predicted_eta_utc",
        F.col("DepDelay").alias("dep_delay"), "predicted_delta_delay", "is_high_risk",
        "DestWindSpeed", "DestVisibility", "DestTemp", "DestCeiling",
        "OriginWindSpeed", "OriginVisibility", "OriginTemp", "OriginCeiling",
        "LaneMeanDelay", "gold_date",
    ]

    def dedupe_batch(df):
        """Drop repeated flight_ids within the batch and against rows already in Gold."""
        df = df.dropDuplicates(["flight_id"])
        if Path(gold_path).exists():
            try:
                seen = spark.read.parquet(gold_path).select("flight_id").distinct()
                df = df.join(seen, on="flight_id", how="left_anti")
            except Exception as exc:  # Gold unreadable mid-write: keep batch-level dedupe only
                print(f"[STREAM] Gold dedupe skipped: {exc}")
        return df

    def unified_foreach_batch(df, batch_id):
        t_start = time.time()
        df.persist()
        write_df = None
        try:
            emit_rows = df.select("emit_epoch_ms").collect()
            count = len(emit_rows)
            if count == 0:
                print(f"  [Batch {batch_id:4d}] 0 rows after parse/filter")
            if count > 0:
                out = df
                if dedupe:
                    # Full count() materializes the cache before Gold is written, so the
                    # anti-join is never re-evaluated against this batch's own output.
                    write_df = dedupe_batch(df).persist()
                    kept = write_df.count()
                    if kept == 0:
                        print(f"  [Batch {batch_id:4d}] all {count:,} rows already in Gold, skipped")
                        metrics_logger.log_batch(batch_id, count, t_start, time.time())
                        return
                    if kept < count:
                        print(f"  [Batch {batch_id:4d}] dropped {count - kept:,} repeated rows")
                    out = write_df
                emit_ts_list = [
                    row["emit_epoch_ms"]
                    for row in emit_rows
                    if row["emit_epoch_ms"]
                ]
                (
                    out.select("kafka_offset", "kafka_partition", "kafka_ts", "raw_json", "ingest_date")
                    .coalesce(1)
                    .write.mode("append")
                    .partitionBy("ingest_date")
                    .parquet(bronze_path)
                )
                (
                    out.select(*silver_cols)
                    .coalesce(1)
                    .write.mode("append")
                    .partitionBy("enrich_date")
                    .parquet(silver_path)
                )
                (
                    out.select(*gold_cols)
                    .coalesce(1)
                    .write.mode("append")
                    .partitionBy("gold_date")
                    .parquet(gold_path)
                )
                (
                    out.groupBy(
                        F.window("crs_arr_ts", window_dur, slide_dur).alias("arrival_window"),
                        F.col("dest"),
                    )
                    .agg(
                        F.count("flight_id").alias("inbound_count"),
                        F.avg("predicted_delta_delay").alias("avg_predicted_delta"),
                        F.sum(F.when(F.col("is_high_risk"), 1).otherwise(0)).alias("high_risk_count"),
                    )
                    .select(
                        F.col("arrival_window.start").alias("window_start"),
                        F.col("arrival_window.end").alias("window_end"),
                        F.col("dest"),
                        "inbound_count", "avg_predicted_delta", "high_risk_count",
                    )
                    .coalesce(1)
                    .write.mode("append")
                    .parquet(gold_agg_path)
                )
                metrics_logger.log_batch(
                    batch_id, count, t_start, time.time(),
                    emit_timestamps=emit_ts_list
                )
        finally:
            if write_df is not None:
                write_df.unpersist()
            df.unpersist()

    stream_query = (
        unified_stream.writeStream
        .foreachBatch(unified_foreach_batch)
        .option("checkpointLocation", os.path.join(checkpoint_root, "unified"))
        .trigger(processingTime=f"{trigger_interval_sec} seconds")
        .start()
    )
    print("[STREAM] Unified Medallion query started -> Bronze/Silver/Gold/Congestion")

    print("\n[STREAM] Query running. Press Ctrl+C to stop.\n")
    try:
        if test_mode:
            deadline = time.time() + 60
            while len(metrics_logger.records) < 4 and stream_query.isActive and time.time() < deadline:
                time.sleep(0.5)
            print(f"[STREAM] Test mode completed {len(metrics_logger.records)} batches. Stopping.")
        else:
            spark.streams.awaitAnyTermination()
    except (KeyboardInterrupt, Py4JError):
        # SIGINT while blocked in awaitAnyTermination surfaces as Py4JError, not KeyboardInterrupt
        print("\n[STREAM] Shutdown signal received.")
    finally:
        try:
            stream_query.stop()
        except Exception:
            pass
        metrics_logger.flush()
        print("[STREAM] All queries stopped cleanly.")


def main():
    parser = argparse.ArgumentParser(description="HyperLogistics Spark Structured Streaming Pipeline")
    parser.add_argument("--config", type=str, default=None, help="Path to YAML config file")
    parser.add_argument("--cores", type=int, default=4, help="Spark local cores (default: 4)")
    parser.add_argument("--shuffle-partitions", type=int, default=8,
                        help="spark.sql.shuffle.partitions (default: 8)")
    parser.add_argument("--max-offsets", type=int, default=5000,
                        help="maxOffsetsPerTrigger (default: 5000)")
    parser.add_argument("--trigger-interval", type=int, default=2,
                        help="Micro-batch trigger interval in seconds (default: 2)")
    parser.add_argument("--bootstrap", type=str, default=None,
                        help="Kafka bootstrap servers (overrides config)")
    parser.add_argument("--test", action="store_true",
                        help="Stop after 4 batches or 60s (for quick validation)")
    parser.add_argument("--benchmark-label", type=str, default=None,
                        help="Label for benchmark metrics output file")
    parser.add_argument("--starting-offsets", type=str, default="latest",
                        choices=["latest", "earliest"],
                        help="Kafka startingOffsets; use earliest to pick up rows produced before start")
    parser.add_argument("--reset", action="store_true",
                        help="Delete Bronze/Silver/Gold/checkpoints before starting (clean replay)")
    parser.add_argument("--output-root", type=str, default=None,
                        help="Write Bronze/Silver/Gold/checkpoints under this directory instead of "
                             "the config paths (keeps benchmark runs out of the dashboard's Gold)")
    parser.add_argument("--no-dedupe", action="store_true",
                        help="Skip flight_id dedupe (benchmarks replay the same flights in a loop)")
    args = parser.parse_args()

    cfg = load_config(args.config)
    if args.output_root:
        cfg = apply_output_root(cfg, args.output_root)
    bootstrap_servers = args.bootstrap or cfg["kafka"]["bootstrap_servers"]

    config_label = args.benchmark_label or (
        f"c{args.cores}_sp{args.shuffle_partitions}_mo{args.max_offsets}"
    )

    print("=" * 60)
    print("HyperLogistics-Stream: Spark Structured Streaming")
    print(f"Config: {config_label}")
    print(f"Cores={args.cores}  ShufflePartitions={args.shuffle_partitions}  "
          f"MaxOffsets={args.max_offsets}  Trigger={args.trigger_interval}s")
    print("=" * 60)

    metrics_dir = resolve_path(cfg["paths"], "benchmark_results")
    metrics_logger = BatchMetricsLogger(output_path=metrics_dir, config_label=config_label)

    spark = build_spark(
        cores=args.cores,
        shuffle_partitions=args.shuffle_partitions,
        app_name=f"HyperLogistics-Stream-{config_label}",
    )

    run_streaming_pipeline(
        spark=spark,
        cfg=cfg,
        bootstrap_servers=bootstrap_servers,
        max_offsets=args.max_offsets,
        trigger_interval_sec=args.trigger_interval,
        metrics_logger=metrics_logger,
        test_mode=args.test,
        reset=args.reset,
        dedupe=not args.no_dedupe,
        starting_offsets=args.starting_offsets,
    )

    spark.stop()


if __name__ == "__main__":
    main()
