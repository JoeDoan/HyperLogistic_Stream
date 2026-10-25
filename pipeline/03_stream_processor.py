#!/usr/bin/env python3
"""
03_stream_processor.py
Phase 3+4: Spark Structured Streaming — Feature Fusion, ML Scoring & Medallion Write.

Reads from Kafka 'flights_stream', performs:
  - Schema parse (from_json, static schema)
  - Broadcast join with static NOAA weather table
  - GBT model scoring (PipelineModel.transform)
  - Sliding window hub aggregations
  - Bronze / Silver / Gold Parquet writes with checkpointing
  - Benchmark metrics logging (throughput, p50/p95 latency)

Usage:
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
      --master local[4] \
      pipeline/03_stream_processor.py \
      --cores 4 --shuffle-partitions 8 --max-offsets 5000
"""

import os
import sys
import time
import json
import argparse
import threading
from pathlib import Path
from datetime import datetime

import yaml

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from pyspark.ml import PipelineModel
from pyspark.sql.streaming import DataStreamWriter

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = Path(os.environ.get("HYPERLOGISTICS_CONFIG", PROJECT_ROOT / "config" / "pipeline_config.yaml"))

# Kafka message JSON schema (matches producer payload)
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
IATA_TO_ICAO = {k: 'K' + k for k in AIRPORT_TZ}


def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def build_spark(cores: int, shuffle_partitions: int, app_name: str = "HyperLogistics-Stream") -> SparkSession:
    core_str = f"local[{cores}]"
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(core_str)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "false")  # Disable AQE for deterministic benchmarks
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        # Kafka connector package must be on classpath via --packages spark-sql-kafka-0-10
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_static_weather(spark: SparkSession, noaa_dir: str, year: int = 2024):
    """Load and broadcast 2024 NOAA weather as a static DataFrame."""
    import glob as glb
    paths = glb.glob(os.path.join(noaa_dir, str(year), "*.csv"))
    if not paths:
        raise FileNotFoundError(f"No NOAA 2024 CSVs in {noaa_dir}/{year}/")

    @F.udf(returnType=DoubleType())
    def parse_wind(s):
        if not s or s == 'nan':
            return None
        parts = str(s).split(',')
        if len(parts) >= 4:
            try:
                v = int(parts[3])
                return None if v == 9999 else v / 10.0
            except ValueError:
                pass
        return None

    @F.udf(returnType=DoubleType())
    def parse_vis(s):
        if not s or s == 'nan':
            return None
        parts = str(s).split(',')
        if parts:
            try:
                v = int(parts[0])
                return None if v == 999999 else float(v)
            except ValueError:
                pass
        return None

    @F.udf(returnType=DoubleType())
    def parse_tmp(s):
        if not s or s == 'nan':
            return None
        parts = str(s).split(',')
        if parts:
            try:
                v = int(parts[0])
                return None if v == 9999 else v / 10.0
            except ValueError:
                pass
        return None

    weather_df = (
        spark.read
        .option("header", "true")
        .csv(paths)
        .select(
            F.input_file_name().alias("_fp"),
            F.col("DATE").alias("raw_date"),
            F.col("WND"), F.col("VIS"), F.col("TMP"),
        )
        .withColumn("icao", F.regexp_extract("_fp", r"([A-Z0-9]{4})_\d+\.csv$", 1))
        .filter(F.col("icao") != "")
        .withColumn("observed_utc", F.to_timestamp("raw_date"))
        .filter(F.col("observed_utc").isNotNull())
        .withColumn("utc_hour", F.date_trunc("hour", "observed_utc"))
        .withColumn("DestWindSpeed", parse_wind(F.col("WND")))
        .withColumn("DestVisibility", parse_vis(F.col("VIS")))
        .withColumn("DestTemp", parse_tmp(F.col("TMP")))
        .select("icao", "utc_hour", "DestWindSpeed", "DestVisibility", "DestTemp")
        .dropDuplicates(["icao", "utc_hour"])
        .fillna({"DestWindSpeed": 3.5, "DestVisibility": 9000.0, "DestTemp": 15.0})
    )
    print(f"[STREAM] Loaded {weather_df.count():,} static weather rows for broadcast join.")
    return weather_df


def load_lane_mean(spark: SparkSession, models_dir: str):
    """Load pre-computed lane mean table from batch training."""
    lane_path = os.path.join(models_dir, "lane_mean.parquet")
    if not Path(lane_path).exists():
        print("[STREAM] Warning: lane_mean.parquet not found. LaneMeanDelay will be 0.0")
        schema = StructType([
            StructField("Origin", StringType()), StructField("Dest", StringType()),
            StructField("LaneMeanDelay", DoubleType()),
        ])
        return spark.createDataFrame([], schema)
    df = spark.read.parquet(lane_path)
    print(f"[STREAM] Loaded {df.count():,} lane mean entries.")
    return df


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
            latencies = sorted([now_ts - ts for ts in emit_timestamps if ts > 0])
            if latencies:
                p50_latency_ms = float(np.percentile(latencies, 50))
                p95_latency_ms = float(np.percentile(latencies, 95))

        record = {
            "config": self.config_label,
            "batch_id": batch_id,
            "num_rows": num_rows,
            "duration_ms": round(duration_ms, 1),
            "throughput_rps": round(throughput, 1),
            "p50_latency_ms": round(p50_latency_ms, 1) if p50_latency_ms else None,
            "p95_latency_ms": round(p95_latency_ms, 1) if p95_latency_ms else None,
            "wall_ts": datetime.utcnow().isoformat(),
        }
        with self._lock:
            self.records.append(record)

        print(
            f"  [Batch {batch_id:4d}] rows={num_rows:6,}  dur={duration_ms:7.1f}ms  "
            f"tput={throughput:8.0f} rps  "
            f"p50={p50_latency_ms:.0f}ms  p95={p95_latency_ms:.0f}ms"
            if p50_latency_ms else
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
        print(f"[METRICS] Saved {len(self.records)} batch records → {outfile}")


def run_streaming_pipeline(
    spark: SparkSession,
    cfg: dict,
    bootstrap_servers: str,
    max_offsets: int,
    trigger_interval_sec: int,
    metrics_logger: BatchMetricsLogger,
    test_mode: bool = False,
):
    noaa_dir = str(PROJECT_ROOT / cfg["paths"]["raw_noaa"])
    models_dir = str(PROJECT_ROOT / cfg["paths"]["models"])
    bronze_path = str(PROJECT_ROOT / cfg["paths"]["bronze"])
    silver_path = str(PROJECT_ROOT / cfg["paths"]["silver"])
    gold_path = str(PROJECT_ROOT / cfg["paths"]["gold_predictions"])
    gold_agg_path = str(PROJECT_ROOT / cfg["paths"]["gold_congestion"])
    checkpoint_root = str(PROJECT_ROOT / cfg["paths"]["checkpoints"])
    topic = cfg["kafka"]["topic"]
    high_risk_thresh = cfg["streaming"]["high_risk_threshold_min"]

    # --- Static data (broadcast) ---
    weather_static = load_static_weather(spark, noaa_dir)
    lane_mean_static = load_lane_mean(spark, models_dir)

    # --- Load serialized GBT PipelineModel ---
    gbt_model_path = os.path.join(models_dir, "gbt_pipeline_v1")
    if not Path(gbt_model_path).exists():
        print("[STREAM] WARNING: GBT model not found. Run 01_batch_train.py first.")
        print("[STREAM] Using zero-prediction fallback for demo mode.")
        gbt_model = None
    else:
        print(f"[STREAM] Loading GBT model from {gbt_model_path}...")
        gbt_model = PipelineModel.load(gbt_model_path)
        print("[STREAM] GBT model loaded.")

    # --- Kafka Source Stream ---
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", str(max_offsets))
        .option("failOnDataLoss", "false")
        .load()
    )

    # --- Bronze Layer: Raw Kafka Dump ---
    bronze_stream = (
        raw_stream
        .select(
            F.col("offset").alias("kafka_offset"),
            F.col("partition").alias("kafka_partition"),
            F.col("timestamp").alias("kafka_ts"),
            F.col("value").cast("string").alias("raw_json"),
        )
        .withColumn("ingest_date", F.to_date(F.current_timestamp()))
    )

    bronze_query = (
        bronze_stream.writeStream
        .format("parquet")
        .option("path", bronze_path)
        .option("checkpointLocation", os.path.join(checkpoint_root, "bronze"))
        .partitionBy("ingest_date")
        .outputMode("append")
        .trigger(processingTime=f"{trigger_interval_sec} seconds")
        .start()
    )
    print(f"[STREAM] Bronze query started → {bronze_path}")

    # --- Parse JSON ---
    parsed_stream = (
        raw_stream
        .select(
            F.col("timestamp").alias("kafka_ts"),
            F.from_json(F.col("value").cast("string"), FLIGHT_SCHEMA).alias("data"),
        )
        .select("kafka_ts", "data.*")
        .filter(F.col("flight_id").isNotNull())
        .withColumn("wheels_off_ts", F.to_timestamp("wheels_off_utc"))
        .withColumn("crs_arr_ts", F.to_timestamp("crs_arr_time_utc"))
        .withColumn("emit_epoch_ms",
                    (F.unix_timestamp("emit_ts") * 1000).cast("long"))
        .withColumn("sched_arr_hour_utc", F.date_trunc("hour", "crs_arr_ts"))
        .withColumn("dest_icao", F.concat(F.lit("K"), F.col("dest")))
        .withColumn("DepHourUTC", F.hour("wheels_off_ts"))
        .withColumn("ArrHourUTC", F.hour("crs_arr_ts"))
        .withColumn("DayOfWeek", F.dayofweek(F.to_date("wheels_off_ts")).cast("int"))
        .filter(F.col("dep_delay").isNotNull() & F.col("distance").isNotNull())
    )

    # --- Silver Layer: Broadcast Join with Weather + Lane Mean ---
    weather_renamed = weather_static.select(
        F.col("icao").alias("w_icao"),
        F.col("utc_hour").alias("w_utc_hour"),
        F.col("DestWindSpeed"),
        F.col("DestVisibility"),
        F.col("DestTemp"),
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
            F.broadcast(lane_mean_static),
            on=["origin", "dest"],
            how="left"
        )
        .fillna({"DestWindSpeed": 3.5, "DestVisibility": 9000.0, "DestTemp": 15.0, "LaneMeanDelay": 0.0})
    )

    silver_stream = enriched_stream.withColumn("enrich_date", F.to_date(F.current_timestamp()))

    silver_query = (
        silver_stream.writeStream
        .format("parquet")
        .option("path", silver_path)
        .option("checkpointLocation", os.path.join(checkpoint_root, "silver"))
        .partitionBy("enrich_date", "origin")
        .outputMode("append")
        .trigger(processingTime=f"{trigger_interval_sec} seconds")
        .start()
    )
    print(f"[STREAM] Silver query started → {silver_path}")

    # --- Gold Layer: ML Scoring + ETA ---
    if gbt_model:
        scored_stream = gbt_model.transform(enriched_stream)
    else:
        scored_stream = enriched_stream.withColumn("prediction", F.lit(0.0))

    gold_stream = (
        scored_stream
        .withColumn("predicted_delta_delay", F.col("prediction"))
        .withColumn("predicted_eta_utc",
                    F.from_unixtime(
                        F.unix_timestamp("crs_arr_ts") +
                        F.col("dep_delay") * 60 +
                        F.col("prediction") * 60
                    ).cast(TimestampType()))
        .withColumn("is_high_risk", F.col("prediction") > F.lit(high_risk_thresh))
        .withColumn("gold_date", F.to_date(F.current_timestamp()))
        .select(
            "flight_id", "carrier", "origin", "dest", "tail_number",
            "wheels_off_ts", "crs_arr_ts", "predicted_eta_utc",
            "dep_delay", "predicted_delta_delay", "is_high_risk",
            "DestWindSpeed", "DestVisibility", "DestTemp",
            "LaneMeanDelay", "emit_epoch_ms", "gold_date",
        )
    )

    def gold_foreach_batch(df, batch_id):
        t_start = time.time()
        count = df.count()
        t_end = time.time()
        if count > 0:
            emit_ts_list = [
                row["emit_epoch_ms"]
                for row in df.select("emit_epoch_ms").collect()
                if row["emit_epoch_ms"]
            ]
            metrics_logger.log_batch(
                batch_id, count, t_start, t_end,
                emit_timestamps=emit_ts_list
            )
            # Write Gold predictions
            (
                df.drop("emit_epoch_ms")
                .write.mode("append")
                .partitionBy("gold_date")
                .parquet(gold_path)
            )

    gold_query = (
        gold_stream.writeStream
        .foreachBatch(gold_foreach_batch)
        .option("checkpointLocation", os.path.join(checkpoint_root, "gold"))
        .trigger(processingTime=f"{trigger_interval_sec} seconds")
        .start()
    )
    print(f"[STREAM] Gold query started → {gold_path}")

    # --- Gold Aggregations: Sliding Window Hub Congestion ---
    window_dur = cfg["streaming"]["window_duration"]
    slide_dur = cfg["streaming"]["slide_duration"]
    watermark = cfg["streaming"]["watermark_delay"]

    agg_stream = (
        scored_stream
        .withWatermark("crs_arr_ts", watermark)
        .groupBy(
            F.window("crs_arr_ts", window_dur, slide_dur).alias("arrival_window"),
            F.col("dest"),
        )
        .agg(
            F.count("flight_id").alias("inbound_count"),
            F.avg("prediction").alias("avg_predicted_delta"),
            F.sum(F.when(F.col("prediction") > high_risk_thresh, 1).otherwise(0)).alias("high_risk_count"),
        )
        .select(
            F.col("arrival_window.start").alias("window_start"),
            F.col("arrival_window.end").alias("window_end"),
            F.col("dest"),
            "inbound_count", "avg_predicted_delta", "high_risk_count",
        )
    )

    agg_query = (
        agg_stream.writeStream
        .format("parquet")
        .option("path", gold_agg_path)
        .option("checkpointLocation", os.path.join(checkpoint_root, "gold_agg"))
        .outputMode("append")
        .trigger(processingTime=f"{trigger_interval_sec} seconds")
        .start()
    )
    print(f"[STREAM] Gold aggregation query started → {gold_agg_path}")

    print("\n[STREAM] All queries running. Press Ctrl+C to stop.\n")
    try:
        if test_mode:
            # In test mode, run for a fixed number of seconds
            time.sleep(30)
            print("[STREAM] Test mode timeout reached. Stopping.")
        else:
            spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n[STREAM] Shutdown signal received.")
    finally:
        for q in [bronze_query, silver_query, gold_query, agg_query]:
            try:
                q.stop()
            except Exception:
                pass
        metrics_logger.flush()
        print("[STREAM] All queries stopped cleanly.")


def main():
    parser = argparse.ArgumentParser(description="HyperLogistics Spark Structured Streaming Pipeline")
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
                        help="Run for 30s then exit (for quick validation)")
    parser.add_argument("--benchmark-label", type=str, default=None,
                        help="Label for benchmark metrics output file")
    args = parser.parse_args()

    cfg = load_config()
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

    metrics_dir = str(PROJECT_ROOT / cfg["paths"]["benchmark_results"])
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
    )

    spark.stop()


if __name__ == "__main__":
    main()
