#!/usr/bin/env python3
"""
01_batch_train.py
Phase 1 Offline Batch Pipeline: Feature Engineering + Spark MLlib Model Training.

Loads BTS On-Time Performance and NOAA hourly weather using native Spark SQL expressions,
engineers features without validation leakage, trains GBTRegressor (primary) and
RandomForestRegressor (baseline) once each, evaluates on the configured holdout year, and serializes
PipelineModels, lane_mean.parquet, and training_metrics.json to disk.

Usage:
    python pipeline/01_batch_train.py --config config/pipeline_config.yaml --cores 4
    python pipeline/01_batch_train.py --config config/pipeline_config_colab.yaml --skip-rf
"""

import argparse
import glob
import json
import os
import sys
import time
from pathlib import Path

import yaml
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor, RandomForestRegressor
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "pipeline_config.yaml"

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

FEATURE_COLS = [
    "DepDelay", "Distance", "DepHourUTC", "ArrHourUTC", "DayOfWeek",
    "DestWindSpeed", "DestVisibility", "DestTemp", "LaneMeanDelay",
]
LABEL_COL = "delta_delay"


def load_config(config_path: str = None) -> dict:
    resolved = config_path or os.environ.get("HYPERLOGISTICS_CONFIG", str(DEFAULT_CONFIG_PATH))
    if not os.path.isabs(resolved):
        resolved = str(PROJECT_ROOT / resolved)
    with open(resolved, "r") as f:
        return yaml.safe_load(f)


def resolve_path(cfg_paths: dict, key: str) -> Path:
    p = cfg_paths[key]
    return Path(p) if os.path.isabs(p) else (PROJECT_ROOT / p)


def build_spark(cores: int = None, shuffle_partitions: int = 64, driver_memory: str = "4g",
                app_name: str = "HyperLogistics-Batch") -> SparkSession:
    core_str = f"local[{cores}]" if cores else "local[*]"
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(core_str)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.driver.memory", driver_memory)
        .config("spark.executor.memory", driver_memory)
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_bts_csv(spark: SparkSession, bts_dir: str, years: list, airports: dict = None):
    known_airports = list((airports or AIRPORT_TZ).keys())
    all_paths = []
    for year in years:
        matches = glob.glob(os.path.join(bts_dir, f"*{year}*.csv"))
        if not matches:
            matches = glob.glob(os.path.join(bts_dir, str(year), "*.csv"))
        all_paths.extend(matches)

    if not all_paths:
        raise FileNotFoundError(
            f"No BTS CSVs found for years {years} in {bts_dir}. "
            f"Run: python scripts/download_bts.py --years {' '.join(map(str, years))} --extract"
        )

    print(f"[BTS] Found {len(all_paths)} CSV files for years {years}")

    raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(all_paths)
    )

    df = raw.select(
        F.col("FlightDate").cast(StringType()),
        F.col("Reporting_Airline").cast(StringType()),
        F.col("Tail_Number").cast(StringType()),
        F.col("Origin").cast(StringType()),
        F.col("Dest").cast(StringType()),
        F.col("CRSDepTime").cast(DoubleType()),
        F.col("DepTime").cast(DoubleType()),
        F.col("DepDelay").cast(DoubleType()),
        F.col("WheelsOff").cast(DoubleType()),
        F.col("WheelsOn").cast(DoubleType()),
        F.col("CRSArrTime").cast(DoubleType()),
        F.col("ArrTime").cast(DoubleType()),
        F.col("ArrDelay").cast(DoubleType()),
        F.col("Cancelled").cast(DoubleType()),
        F.col("Diverted").cast(DoubleType()),
        F.col("Distance").cast(DoubleType()),
    )

    df = (
        df
        .filter((F.col("Cancelled") == 0) & (F.col("Diverted") == 0))
        .filter(F.col("DepDelay").isNotNull() & F.col("ArrDelay").isNotNull())
        .filter(F.col("WheelsOff").isNotNull() & F.col("CRSDepTime").isNotNull() & F.col("CRSArrTime").isNotNull())
        .filter(F.col("Origin").isin(known_airports) & F.col("Dest").isin(known_airports))
        .withColumn("Year", F.year(F.to_date("FlightDate", "yyyy-MM-dd")))
        .withColumn("DayOfWeek", F.dayofweek(F.to_date("FlightDate", "yyyy-MM-dd")).cast(IntegerType()))
        .withColumn("delta_delay", F.col("ArrDelay") - F.col("DepDelay"))
    )
    return df


def _hhmm_to_local_ts(date_col: str, hhmm_col: str):
    """Convert FlightDate + HHMM float (handling 2400 rollover) into a local Timestamp column."""
    hhmm_int = F.col(hhmm_col).cast(IntegerType())
    is_2400 = hhmm_int == 2400
    hour_val = F.when(is_2400, F.lit(0)).otherwise(F.floor(hhmm_int / 100))
    min_val = F.when(is_2400, F.lit(0)).otherwise(hhmm_int % 100)
    valid_hm = (hour_val >= 0) & (hour_val <= 23) & (min_val >= 0) & (min_val <= 59)

    hour_str = F.lpad(hour_val.cast(StringType()), 2, "0")
    min_str = F.lpad(min_val.cast(StringType()), 2, "0")
    dt_str = F.concat_ws(" ", F.col(date_col), F.concat(hour_str, F.lit(":"), min_str))
    base_ts = F.when(valid_hm, F.to_timestamp(dt_str, "yyyy-MM-dd HH:mm")).otherwise(F.lit(None))
    return F.when(is_2400, base_ts + F.expr("INTERVAL 1 DAY")).otherwise(base_ts)


def add_utc_timestamps(spark: SparkSession, df, airports: dict = None):
    """Convert HHMM local times to UTC using native Spark SQL expressions and fix midnight rollover."""
    tz_dict = airports or AIRPORT_TZ
    tz_rows = list(tz_dict.items())
    tz_df = spark.createDataFrame(tz_rows, ["airport", "timezone"])

    df = df.join(
        F.broadcast(tz_df.withColumnRenamed("airport", "Origin").withColumnRenamed("timezone", "_orig_tz")),
        on="Origin", how="left"
    )
    df = df.join(
        F.broadcast(tz_df.withColumnRenamed("airport", "Dest").withColumnRenamed("timezone", "_dest_tz")),
        on="Dest", how="left"
    )

    df = (
        df
        .withColumn("_crs_dep_local", _hhmm_to_local_ts("FlightDate", "CRSDepTime"))
        .withColumn("_wo_local", _hhmm_to_local_ts("FlightDate", "WheelsOff"))
        .withColumn("_crs_arr_local", _hhmm_to_local_ts("FlightDate", "CRSArrTime"))
        .withColumn("crs_dep_utc", F.to_utc_timestamp("_crs_dep_local", F.col("_orig_tz")))
        .withColumn("_wo_utc_raw", F.to_utc_timestamp("_wo_local", F.col("_orig_tz")))
        .withColumn("_arr_utc_raw", F.to_utc_timestamp("_crs_arr_local", F.col("_dest_tz")))
        .withColumn(
            "wheels_off_utc",
            F.when(
                F.col("_wo_utc_raw") < (F.col("crs_dep_utc") - F.expr("INTERVAL 1 HOUR")),
                F.col("_wo_utc_raw") + F.expr("INTERVAL 1 DAY")
            ).otherwise(F.col("_wo_utc_raw"))
        )
        .withColumn(
            "sched_arr_utc",
            F.when(
                F.col("_arr_utc_raw") < F.col("crs_dep_utc"),
                F.col("_arr_utc_raw") + F.expr("INTERVAL 1 DAY")
            ).otherwise(F.col("_arr_utc_raw"))
        )
        .filter(F.col("wheels_off_utc").isNotNull() & F.col("sched_arr_utc").isNotNull())
        .withColumn("DepHourUTC", F.hour("wheels_off_utc").cast(DoubleType()))
        .withColumn("ArrHourUTC", F.hour("sched_arr_utc").cast(DoubleType()))
        .withColumn("sched_arr_hour_utc", F.date_trunc("hour", F.col("sched_arr_utc")))
        .drop("_crs_dep_local", "_wo_local", "_crs_arr_local", "_orig_tz", "_dest_tz", "_wo_utc_raw", "_arr_utc_raw")
    )
    return df


def load_noaa_weather(spark: SparkSession, noaa_dir: str, years: list):
    """Load and clean NOAA ISD hourly weather CSVs using native Spark SQL string ops."""
    all_paths = []
    for year in years:
        all_paths.extend(glob.glob(os.path.join(noaa_dir, str(year), "*.csv")))
        all_paths.extend(glob.glob(os.path.join(noaa_dir, f"*{year}*.csv")))
    all_paths = sorted(set(all_paths))

    if not all_paths:
        raise FileNotFoundError(
            f"No NOAA CSVs found in {noaa_dir} for years {years}. "
            f"Run: python scripts/download_noaa.py --years {' '.join(map(str, years))}"
        )

    print(f"[NOAA] Found {len(all_paths)} station-year files")

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
        .withColumn("_wnd_raw", F.split(F.col("WND"), ",").getItem(3).cast(DoubleType()))
        .withColumn("_vis_raw", F.split(F.col("VIS"), ",").getItem(0).cast(DoubleType()))
        .withColumn("_tmp_raw", F.regexp_extract(F.col("TMP"), r"^([+\-]?\d+)", 1).cast(DoubleType()))
        .withColumn(
            "wind_speed_mps",
            F.when(F.col("_wnd_raw") >= 9999, F.lit(None)).otherwise(F.col("_wnd_raw") / 10.0)
        )
        .withColumn(
            "visibility_m",
            F.when(F.col("_vis_raw") >= 999999, F.lit(None)).otherwise(F.col("_vis_raw"))
        )
        .withColumn(
            "temp_c",
            F.when(F.abs(F.col("_tmp_raw")) >= 9999, F.lit(None)).otherwise(F.col("_tmp_raw") / 10.0)
        )
        .select("icao", "utc_hour", "wind_speed_mps", "visibility_m", "temp_c")
        .dropDuplicates(["icao", "utc_hour"])
    )
    return weather_df


def join_weather(flights_df, weather_df):
    """Broadcast left-join flights with static NOAA weather on (dest_icao, sched_arr_hour_utc)."""
    flights_df = flights_df.withColumn("dest_icao", F.concat(F.lit("K"), F.col("Dest")))

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
        how="left"
    ).drop("w_icao", "w_utc_hour")

    joined = joined.fillna({
        "DestWindSpeed": 3.5,
        "DestVisibility": 9000.0,
        "DestTemp": 15.0,
    })
    return joined


def evaluate_model(model, test_df, label_col=LABEL_COL, model_name="Model"):
    predictions = model.transform(test_df)
    evaluator_mae = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="mae")
    evaluator_rmse = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="rmse")
    mae = evaluator_mae.evaluate(predictions)
    rmse = evaluator_rmse.evaluate(predictions)
    print(f"  [{model_name}] MAE={mae:.3f} min  RMSE={rmse:.3f} min")
    return mae, rmse


def compute_baselines(test_df):
    """Compute Baseline 1 (delta=0) and Baseline 2 (train lane mean) MAE/RMSE."""
    evaluator_mae = RegressionEvaluator(labelCol=LABEL_COL, predictionCol="b1_pred", metricName="mae")
    evaluator_rmse = RegressionEvaluator(labelCol=LABEL_COL, predictionCol="b1_pred", metricName="rmse")
    b1_df = test_df.withColumn("b1_pred", F.lit(0.0))
    b1_mae = evaluator_mae.evaluate(b1_df)
    b1_rmse = evaluator_rmse.evaluate(b1_df)
    print(f"  [Baseline-1 No-Change]  MAE={b1_mae:.3f} min  RMSE={b1_rmse:.3f} min")

    b2_df = test_df.withColumn("b2_pred", F.col("LaneMeanDelay"))
    evaluator_mae2 = RegressionEvaluator(labelCol=LABEL_COL, predictionCol="b2_pred", metricName="mae")
    evaluator_rmse2 = RegressionEvaluator(labelCol=LABEL_COL, predictionCol="b2_pred", metricName="rmse")
    b2_mae = evaluator_mae2.evaluate(b2_df)
    b2_rmse = evaluator_rmse2.evaluate(b2_df)
    print(f"  [Baseline-2 Lane Mean]  MAE={b2_mae:.3f} min  RMSE={b2_rmse:.3f} min")
    return {"b1_mae": b1_mae, "b1_rmse": b1_rmse, "b2_mae": b2_mae, "b2_rmse": b2_rmse}


def main():
    parser = argparse.ArgumentParser(description="HyperLogistics Batch Training Pipeline")
    parser.add_argument("--config", type=str, default=None, help="Path to YAML config file")
    parser.add_argument("--cores", type=int, default=None, help="Spark local cores (default: all)")
    parser.add_argument("--shuffle-partitions", type=int, default=64, help="spark.sql.shuffle.partitions")
    parser.add_argument("--skip-rf", action="store_true", help="Skip Random Forest training to save time")
    args = parser.parse_args()

    cfg = load_config(args.config)
    bts_dir = str(resolve_path(cfg["paths"], "raw_bts"))
    noaa_dir = str(resolve_path(cfg["paths"], "raw_noaa"))
    models_dir = resolve_path(cfg["paths"], "models")
    models_dir.mkdir(parents=True, exist_ok=True)
    airports = cfg.get("airports", AIRPORT_TZ)

    train_years = cfg["training"]["train_years"]
    val_year = cfg["training"]["val_year"]
    all_years = sorted(set(train_years + [val_year]))

    print("=" * 60)
    print("HyperLogistics-Stream: Phase 1 Batch Training")
    print(f"Train years: {train_years}  |  Val year: {val_year}")
    print("=" * 60)

    spark = build_spark(
        cores=args.cores,
        shuffle_partitions=args.shuffle_partitions,
        driver_memory=cfg.get("spark", {}).get("driver_memory", "4g"),
    )

    t0 = time.time()
    bts_raw = load_bts_csv(spark, bts_dir, all_years, airports=airports)
    bts_utc = add_utc_timestamps(spark, bts_raw, airports=airports)
    noaa_df = load_noaa_weather(spark, noaa_dir, all_years)

    print("[JOIN] Joining flights with destination weather...")
    joined = join_weather(bts_utc, noaa_df)

    train_data = joined.filter(F.col("Year").isin(train_years))
    val_data = joined.filter(F.col("Year") == val_year)

    lane_mean = (
        train_data
        .groupBy("Origin", "Dest")
        .agg(F.avg("delta_delay").alias("LaneMeanDelay"))
    )
    lane_mean_path = str(models_dir / "lane_mean.parquet")
    lane_mean.write.mode("overwrite").parquet(lane_mean_path)
    print(f"[SAVE] Lane mean table -> {lane_mean_path}")

    lane_mean_cached = spark.read.parquet(lane_mean_path).cache()
    train_cols = FEATURE_COLS + [LABEL_COL]
    train_df = (
        train_data
        .join(F.broadcast(lane_mean_cached), on=["Origin", "Dest"], how="left")
        .fillna(0.0, subset=["LaneMeanDelay"])
        .select(train_cols)
        .dropna(subset=train_cols)
        .cache()
    )
    val_df = (
        val_data
        .join(F.broadcast(lane_mean_cached), on=["Origin", "Dest"], how="left")
        .fillna(0.0, subset=["LaneMeanDelay"])
        .select(train_cols)
        .dropna(subset=train_cols)
        .cache()
    )

    train_count = train_df.count()
    val_count = val_df.count()
    print(f"[DATA] Train: {train_count:,}  Val: {val_count:,}  (elapsed {time.time()-t0:.1f}s)")

    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features", handleInvalid="skip")

    print(f"\n[EVAL] Computing holdout baselines on {val_year}:")
    baseline_metrics = compute_baselines(val_df)

    print("\n[TRAIN] Training GBTRegressor Pipeline...")
    gbt = GBTRegressor(
        featuresCol="features",
        labelCol=LABEL_COL,
        maxIter=cfg["training"]["gbt_max_iter"],
        maxDepth=cfg["training"]["gbt_max_depth"],
        seed=cfg["training"]["seed"],
    )
    gbt_pipeline = Pipeline(stages=[assembler, gbt])
    t_gbt = time.time()
    gbt_pipeline_model = gbt_pipeline.fit(train_df)
    print(f"  GBT training time: {time.time()-t_gbt:.1f}s")

    gbt_mae, gbt_rmse = evaluate_model(gbt_pipeline_model, val_df, model_name="GBT")
    gbt_model_path = str(models_dir / "gbt_pipeline_v1")
    gbt_pipeline_model.write().overwrite().save(gbt_model_path)
    print(f"[SAVE] GBT PipelineModel -> {gbt_model_path}")

    rf_mae, rf_rmse = None, None
    if not args.skip_rf:
        print("\n[TRAIN] Training RandomForestRegressor Pipeline...")
        rf = RandomForestRegressor(
            featuresCol="features",
            labelCol=LABEL_COL,
            numTrees=cfg["training"]["rf_num_trees"],
            seed=cfg["training"]["seed"],
        )
        rf_pipeline = Pipeline(stages=[assembler, rf])
        t_rf = time.time()
        rf_pipeline_model = rf_pipeline.fit(train_df)
        print(f"  RF training time: {time.time()-t_rf:.1f}s")
        rf_mae, rf_rmse = evaluate_model(rf_pipeline_model, val_df, model_name="RandomForest")

        rf_model_path = str(models_dir / "rf_pipeline_v1")
        rf_pipeline_model.write().overwrite().save(rf_model_path)
        print(f"[SAVE] RF PipelineModel -> {rf_model_path}")

    metrics = {
        "train_count": train_count,
        "val_count": val_count,
        **baseline_metrics,
        "gbt_mae": gbt_mae,
        "gbt_rmse": gbt_rmse,
        "gbt_feature_importances": dict(zip(
            FEATURE_COLS, gbt_pipeline_model.stages[-1].featureImportances.toArray().tolist()
        )),
        "rf_mae": rf_mae,
        "rf_rmse": rf_rmse,
        "train_years": train_years,
        "val_year": val_year,
        "total_elapsed_sec": time.time() - t0,
    }
    metrics_path = models_dir / "training_metrics.json"
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"\n[SAVE] Metrics -> {metrics_path}")

    print("\n" + "=" * 60)
    print("Phase 1 Complete. Models ready for streaming pipeline.")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
