#!/usr/bin/env python3
"""
01_batch_train.py
Phase 1 Offline Batch Pipeline: Feature Engineering + Spark MLlib Model Training.

Loads BTS On-Time Performance (2018-2023, excluding 2020) and NOAA hourly weather,
engineers features, trains GBTRegressor (primary) and RandomForestRegressor (baseline),
evaluates on 2023 holdout, and serializes both PipelineModels to disk.

Usage:
    spark-submit --master local[4] pipeline/01_batch_train.py
    python pipeline/01_batch_train.py  (runs with local[*] automatically)
"""

import os
import sys
import glob
import json
import time
import argparse
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
)
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import GBTRegressor, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

CONFIG_PATH = PROJECT_ROOT / "config" / "pipeline_config.yaml"

# IATA -> IANA timezone mapping (serialized as broadcast)
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

# IATA -> ICAO for weather join
IATA_TO_ICAO = {k: 'K' + k for k in AIRPORT_TZ}


def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def build_spark(cores: int = None, shuffle_partitions: int = 200, app_name: str = "HyperLogistics-Batch") -> SparkSession:
    core_str = f"local[{cores}]" if cores else "local[*]"
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(core_str)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_bts_csv(spark: SparkSession, bts_dir: str, years: list) -> "DataFrame":
    """Load BTS CSVs for specified years, filter cancelled/diverted."""
    schema = StructType([
        StructField("FlightDate", StringType(), True),
        StructField("Reporting_Airline", StringType(), True),
        StructField("Tail_Number", StringType(), True),
        StructField("Origin", StringType(), True),
        StructField("Dest", StringType(), True),
        StructField("CRSDepTime", DoubleType(), True),
        StructField("DepTime", DoubleType(), True),
        StructField("DepDelay", DoubleType(), True),
        StructField("WheelsOff", DoubleType(), True),
        StructField("WheelsOn", DoubleType(), True),
        StructField("CRSArrTime", DoubleType(), True),
        StructField("ArrTime", DoubleType(), True),
        StructField("ArrDelay", DoubleType(), True),
        StructField("Cancelled", DoubleType(), True),
        StructField("Diverted", DoubleType(), True),
        StructField("Distance", DoubleType(), True),
    ])

    known_airports = list(AIRPORT_TZ.keys())
    all_paths = []
    for year in years:
        pattern = os.path.join(bts_dir, f"*{year}*.csv")
        all_paths.extend(glob.glob(pattern))

    if not all_paths:
        raise FileNotFoundError(
            f"No BTS CSVs found for years {years} in {bts_dir}. "
            "Run: python scripts/download_bts.py --years 2018 2019 2021 2022 2023 --extract"
        )

    print(f"[BTS] Found {len(all_paths)} CSV files for years {years}")

    df = (
        spark.read
        .option("header", "true")
        .schema(schema)
        .csv(all_paths)
        .filter((F.col("Cancelled") == 0) & (F.col("Diverted") == 0))
        .filter(F.col("DepDelay").isNotNull() & F.col("ArrDelay").isNotNull())
        .filter(F.col("Origin").isin(known_airports) & F.col("Dest").isin(known_airports))
        .withColumn("Year", F.year(F.to_date("FlightDate", "yyyy-MM-dd")))
        .withColumn("DayOfWeek", F.dayofweek(F.to_date("FlightDate", "yyyy-MM-dd")).cast(IntegerType()))
        .withColumn("delta_delay", F.col("ArrDelay") - F.col("DepDelay"))
    )
    return df


def add_utc_timestamps(spark: SparkSession, df: "DataFrame") -> "DataFrame":
    """Convert HHMM local times to UTC using per-airport timezone."""
    tz_map_json = json.dumps(AIRPORT_TZ)

    @F.udf(returnType=TimestampType())
    def hhmm_to_utc(flight_date_str, hhmm_float, airport_code, tz_json):
        if hhmm_float is None or airport_code is None:
            return None
        import json
        from datetime import datetime, timedelta
        import zoneinfo
        tz_map = json.loads(tz_json)
        tz_name = tz_map.get(airport_code)
        if not tz_name:
            return None
        try:
            hhmm = int(hhmm_float)
            if hhmm == 2400:
                hour, minute, extra_day = 0, 0, 1
            else:
                hour, minute, extra_day = hhmm // 100, hhmm % 100, 0
            if hour > 23 or minute > 59:
                return None
            fdate = datetime.strptime(flight_date_str, '%Y-%m-%d')
            local_dt = fdate.replace(hour=hour, minute=minute) + timedelta(days=extra_day)
            tz = zoneinfo.ZoneInfo(tz_name)
            local_aware = local_dt.replace(tzinfo=tz)
            utc_dt = local_aware.astimezone(zoneinfo.ZoneInfo('UTC'))
            return utc_dt.replace(tzinfo=None)
        except Exception:
            return None

    df = (
        df
        .withColumn(
            "wheels_off_utc",
            hhmm_to_utc(F.col("FlightDate"), F.col("WheelsOff"), F.col("Origin"), F.lit(tz_map_json))
        )
        .withColumn(
            "sched_arr_utc",
            hhmm_to_utc(F.col("FlightDate"), F.col("CRSArrTime"), F.col("Dest"), F.lit(tz_map_json))
        )
        .filter(F.col("wheels_off_utc").isNotNull() & F.col("sched_arr_utc").isNotNull())
        .withColumn("DepHourUTC", F.hour("wheels_off_utc"))
        .withColumn("ArrHourUTC", F.hour("sched_arr_utc"))
        .withColumn(
            "sched_arr_hour_utc",
            F.date_trunc("hour", F.col("sched_arr_utc"))
        )
    )
    return df


def load_noaa_weather(spark: SparkSession, noaa_dir: str, years: list) -> "DataFrame":
    """Load and clean NOAA ISD hourly weather CSVs for given years."""
    all_paths = []
    for year in years:
        pattern = os.path.join(noaa_dir, str(year), "*.csv")
        all_paths.extend(glob.glob(pattern))

    if not all_paths:
        raise FileNotFoundError(
            f"No NOAA CSVs found in {noaa_dir} for years {years}. "
            "Run: python scripts/download_noaa.py --years 2018 2019 2021 2022 2023"
        )

    print(f"[NOAA] Found {len(all_paths)} station-year files")

    @F.udf(returnType=DoubleType())
    def parse_wind_speed(wnd_str):
        if not wnd_str or wnd_str == 'nan':
            return None
        parts = str(wnd_str).split(',')
        if len(parts) >= 4:
            try:
                v = int(parts[3])
                return None if v == 9999 else v / 10.0
            except ValueError:
                pass
        return None

    @F.udf(returnType=DoubleType())
    def parse_visibility(vis_str):
        if not vis_str or vis_str == 'nan':
            return None
        parts = str(vis_str).split(',')
        if parts:
            try:
                v = int(parts[0])
                return None if v == 999999 else float(v)
            except ValueError:
                pass
        return None

    @F.udf(returnType=DoubleType())
    def parse_temp(tmp_str):
        if not tmp_str or tmp_str == 'nan':
            return None
        parts = str(tmp_str).split(',')
        if parts:
            try:
                v = int(parts[0])
                return None if v == 9999 else v / 10.0
            except ValueError:
                pass
        return None

    # Extract ICAO from filename via path manipulation
    weather_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(all_paths)
        .select(
            F.input_file_name().alias("_filepath"),
            F.col("DATE").alias("raw_date"),
            F.col("WND"),
            F.col("VIS"),
            F.col("TMP"),
        )
        .withColumn("icao", F.regexp_extract(F.col("_filepath"), r"([A-Z0-9]{4})_\d+\.csv$", 1))
        .filter(F.col("icao") != "")
        .withColumn("observed_utc", F.to_timestamp("raw_date"))
        .filter(F.col("observed_utc").isNotNull())
        .withColumn("utc_hour", F.date_trunc("hour", "observed_utc"))
        .withColumn("wind_speed_mps", parse_wind_speed(F.col("WND")))
        .withColumn("visibility_m", parse_visibility(F.col("VIS")))
        .withColumn("temp_c", parse_temp(F.col("TMP")))
        .drop("_filepath", "WND", "VIS", "TMP", "raw_date")
        # Deduplicate: one reading per (icao, hour)
        .dropDuplicates(["icao", "utc_hour"])
    )
    return weather_df


def add_lane_history(df: "DataFrame") -> "DataFrame":
    """Compute 30-day rolling lane mean delay using a lookback window.
    Approximation: use full-dataset historical mean per Origin-Dest lane as proxy
    (Spark windowed rolling average would require sorted partition; use aggregate mean here).
    """
    lane_mean = (
        df
        .groupBy("Origin", "Dest")
        .agg(F.avg("delta_delay").alias("LaneMeanDelay"))
    )
    df = df.join(F.broadcast(lane_mean), on=["Origin", "Dest"], how="left")
    df = df.fillna(0.0, subset=["LaneMeanDelay"])
    return df


def join_weather(flights_df: "DataFrame", weather_df: "DataFrame") -> "DataFrame":
    """Broadcast join flights with static NOAA weather on (dest_icao, sched_arr_hour_utc)."""
    iata_to_icao_map = IATA_TO_ICAO

    @F.udf(returnType=StringType())
    def to_icao(iata):
        return iata_to_icao_map.get(iata, 'K' + iata if iata else None)

    flights_df = flights_df.withColumn("dest_icao", to_icao(F.col("Dest")))

    weather_renamed = weather_df.select(
        F.col("icao").alias("w_icao"),
        F.col("utc_hour").alias("w_utc_hour"),
        F.col("wind_speed_mps").alias("DestWindSpeed"),
        F.col("visibility_m").alias("DestVisibility"),
        F.col("temp_c").alias("DestTemp"),
    )

    joined = flights_df.join(
        F.broadcast(weather_renamed),
        (flights_df.dest_icao == weather_renamed.w_icao) &
        (flights_df.sched_arr_hour_utc == weather_renamed.w_utc_hour),
        how="inner"
    ).drop("w_icao", "w_utc_hour")

    # Fill missing weather features with medians (constants for robustness)
    joined = joined.fillna({
        "DestWindSpeed": 3.5,
        "DestVisibility": 9000.0,
        "DestTemp": 15.0,
    })
    return joined


def build_feature_vector(df: "DataFrame") -> "DataFrame":
    feature_cols = [
        "DepDelay", "Distance", "DepHourUTC", "ArrHourUTC", "DayOfWeek",
        "DestWindSpeed", "DestVisibility", "DestTemp", "LaneMeanDelay"
    ]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")
    return assembler.transform(df)


def evaluate_model(model, test_df, label_col="delta_delay", model_name="Model"):
    predictions = model.transform(test_df)
    evaluator_mae = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="mae")
    evaluator_rmse = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="rmse")
    mae = evaluator_mae.evaluate(predictions)
    rmse = evaluator_rmse.evaluate(predictions)
    print(f"  [{model_name}] MAE={mae:.3f} min  RMSE={rmse:.3f} min")
    return mae, rmse


def compute_baselines(test_df):
    """Compute Baseline 1 (delta=0) and Baseline 2 (lane mean) MAE/RMSE."""
    # Baseline 1: No-change (predict delta_delay = 0 for every flight)
    evaluator_mae = RegressionEvaluator(labelCol="delta_delay", predictionCol="b1_pred", metricName="mae")
    evaluator_rmse = RegressionEvaluator(labelCol="delta_delay", predictionCol="b1_pred", metricName="rmse")
    b1_df = test_df.withColumn("b1_pred", F.lit(0.0))
    b1_mae = evaluator_mae.evaluate(b1_df)
    b1_rmse = evaluator_rmse.evaluate(b1_df)
    print(f"  [Baseline-1 No-Change]  MAE={b1_mae:.3f} min  RMSE={b1_rmse:.3f} min")

    # Baseline 2: Lane historical mean
    b2_df = test_df.withColumn("b2_pred", F.col("LaneMeanDelay"))
    evaluator_mae2 = RegressionEvaluator(labelCol="delta_delay", predictionCol="b2_pred", metricName="mae")
    evaluator_rmse2 = RegressionEvaluator(labelCol="delta_delay", predictionCol="b2_pred", metricName="rmse")
    b2_mae = evaluator_mae2.evaluate(b2_df)
    b2_rmse = evaluator_rmse2.evaluate(b2_df)
    print(f"  [Baseline-2 Lane Mean]  MAE={b2_mae:.3f} min  RMSE={b2_rmse:.3f} min")
    return {"b1_mae": b1_mae, "b1_rmse": b1_rmse, "b2_mae": b2_mae, "b2_rmse": b2_rmse}


def main():
    parser = argparse.ArgumentParser(description="HyperLogistics Batch Training Pipeline")
    parser.add_argument("--cores", type=int, default=None, help="Spark local cores (default: all)")
    parser.add_argument("--shuffle-partitions", type=int, default=64, help="spark.sql.shuffle.partitions")
    parser.add_argument("--skip-rf", action="store_true", help="Skip Random Forest training to save time")
    args = parser.parse_args()

    cfg = load_config()
    bts_dir = str(PROJECT_ROOT / cfg["paths"]["raw_bts"])
    noaa_dir = str(PROJECT_ROOT / cfg["paths"]["raw_noaa"])
    models_dir = PROJECT_ROOT / cfg["paths"]["models"]
    models_dir.mkdir(parents=True, exist_ok=True)

    train_years = cfg["training"]["train_years"]
    val_year = cfg["training"]["val_year"]
    all_years = train_years + [val_year]

    print("=" * 60)
    print("HyperLogistics-Stream: Phase 1 Batch Training")
    print(f"Train years: {train_years}  |  Val year: {val_year}")
    print("=" * 60)

    spark = build_spark(cores=args.cores, shuffle_partitions=args.shuffle_partitions)

    # --- Load & Clean BTS ---
    t0 = time.time()
    bts_raw = load_bts_csv(spark, bts_dir, all_years)
    bts_utc = add_utc_timestamps(spark, bts_raw)

    # --- Load & Clean NOAA ---
    noaa_df = load_noaa_weather(spark, noaa_dir, all_years)

    # --- Join BTS with NOAA ---
    print("[JOIN] Joining flights with destination weather...")
    joined = join_weather(bts_utc, noaa_df)

    # --- Lane Mean Delay Feature ---
    train_data = joined.filter(F.col("Year").isin(train_years))
    val_data = joined.filter(F.col("Year") == val_year)

    # Compute lane mean on train only, broadcast to val
    lane_mean = (
        train_data
        .groupBy("Origin", "Dest")
        .agg(F.avg("delta_delay").alias("LaneMeanDelay"))
    )
    train_data = train_data.join(F.broadcast(lane_mean), on=["Origin", "Dest"], how="left").fillna(0.0, subset=["LaneMeanDelay"])
    val_data = val_data.join(F.broadcast(lane_mean), on=["Origin", "Dest"], how="left").fillna(0.0, subset=["LaneMeanDelay"])

    # Persist lane mean for the streaming pipeline
    lane_mean_path = str(models_dir / "lane_mean.parquet")
    lane_mean.write.mode("overwrite").parquet(lane_mean_path)
    print(f"[SAVE] Lane mean table → {lane_mean_path}")

    # --- Build Feature Vector ---
    feature_cols = [
        "DepDelay", "Distance", "DepHourUTC", "ArrHourUTC", "DayOfWeek",
        "DestWindSpeed", "DestVisibility", "DestTemp", "LaneMeanDelay"
    ]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")

    train_vec = assembler.transform(train_data).select("features", "delta_delay").cache()
    val_vec = assembler.transform(val_data).select("features", "delta_delay", "LaneMeanDelay").cache()

    train_count = train_vec.count()
    val_count = val_vec.count()
    print(f"[DATA] Train: {train_count:,}  Val: {val_count:,}  (elapsed {time.time()-t0:.1f}s)")

    # --- Train GBT (Primary Model) ---
    print("\n[TRAIN] Training GBTRegressor...")
    gbt = GBTRegressor(
        featuresCol="features",
        labelCol="delta_delay",
        maxIter=cfg["training"]["gbt_max_iter"],
        maxDepth=cfg["training"]["gbt_max_depth"],
        seed=cfg["training"]["seed"],
    )
    t_gbt = time.time()
    gbt_model = gbt.fit(train_vec)
    print(f"  GBT training time: {time.time()-t_gbt:.1f}s")

    # --- Evaluate ---
    print("\n[EVAL] Holdout evaluation on 2023:")
    compute_baselines(val_vec)
    gbt_mae, gbt_rmse = evaluate_model(gbt_model, val_vec, model_name="GBT")

    # Save assembled pipeline (assembler + model) for streaming use
    from pyspark.ml import Pipeline as SparkPipeline
    gbt_pipeline = SparkPipeline(stages=[assembler, gbt])
    gbt_pipeline_model = gbt_pipeline.fit(train_data.select(feature_cols + ["delta_delay"]))
    gbt_model_path = str(models_dir / "gbt_pipeline_v1")
    gbt_pipeline_model.write().overwrite().save(gbt_model_path)
    print(f"[SAVE] GBT PipelineModel → {gbt_model_path}")

    # --- Train RF (Baseline Model) ---
    if not args.skip_rf:
        print("\n[TRAIN] Training RandomForestRegressor...")
        rf = RandomForestRegressor(
            featuresCol="features",
            labelCol="delta_delay",
            numTrees=cfg["training"]["rf_num_trees"],
            seed=cfg["training"]["seed"],
        )
        t_rf = time.time()
        rf_model = rf.fit(train_vec)
        print(f"  RF training time: {time.time()-t_rf:.1f}s")
        rf_mae, rf_rmse = evaluate_model(rf_model, val_vec, model_name="RandomForest")

        rf_pipeline = SparkPipeline(stages=[assembler, rf])
        rf_pipeline_model = rf_pipeline.fit(train_data.select(feature_cols + ["delta_delay"]))
        rf_model_path = str(models_dir / "rf_pipeline_v1")
        rf_pipeline_model.write().overwrite().save(rf_model_path)
        print(f"[SAVE] RF PipelineModel → {rf_model_path}")

    # --- Save Training Metrics ---
    metrics = {
        "train_count": train_count,
        "val_count": val_count,
        "gbt_mae": gbt_mae,
        "gbt_rmse": gbt_rmse,
        "train_years": train_years,
        "val_year": val_year,
        "total_elapsed_sec": time.time() - t0,
    }
    metrics_path = models_dir / "training_metrics.json"
    import json
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"\n[SAVE] Metrics → {metrics_path}")

    print("\n" + "=" * 60)
    print("Phase 1 Complete. Models ready for streaming pipeline.")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
