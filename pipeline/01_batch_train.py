#!/usr/bin/env python3
"""
01_batch_train.py
Offline batch pipeline: load BTS flight data + NOAA weather, engineer features,
train GBT and RF models, evaluate on 2023 holdout, save serialized PipelineModels.

Usage:
    python pipeline/01_batch_train.py --config config/pipeline_config_colab.yaml
    python pipeline/01_batch_train.py --config config/pipeline_config.yaml --cores 4
"""

import argparse
import os
import sys
import glob
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def resolve_path(cfg_paths: dict, key: str, base_dir: str) -> str:
    p = cfg_paths[key]
    if not os.path.isabs(p):
        p = os.path.join(base_dir, p)
    return p


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def create_spark(cores: int, shuffle_partitions: int, app_name: str = "HyperLogistics-BatchTrain") -> SparkSession:
    master = f"local[{cores}]"
    return (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.adaptive.enabled", "false")   # deterministic for benchmarks
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# BTS loader  (no fixed schema — select columns by name after inference)
# ---------------------------------------------------------------------------

def load_bts(spark: SparkSession, raw_bts_dir: str, years: list, airports: dict) -> "DataFrame":
    known = set(airports.keys())
    all_csvs = []
    for year in years:
        pattern = os.path.join(raw_bts_dir, f"*{year}*.csv")
        matches = glob.glob(pattern)
        if not matches:
            # Also try year sub-folder
            pattern2 = os.path.join(raw_bts_dir, str(year), "*.csv")
            matches = glob.glob(pattern2)
        all_csvs.extend(matches)

    if not all_csvs:
        raise FileNotFoundError(
            f"No BTS CSVs found for years {years} under {raw_bts_dir}\n"
            "Download with: python scripts/download_bts.py --years 2018 2019 2021 2022 2023 --extract"
        )

    print(f"  Loading {len(all_csvs)} BTS CSV files for years {years}...")

    # Read with inferred string schema so column name lookup works regardless of column order
    raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(all_csvs)
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
        .filter(F.col("Origin").isin(known) & F.col("Dest").isin(known))
        .withColumn("delta_delay", F.col("ArrDelay") - F.col("DepDelay"))
        .withColumn("Year", F.year(F.to_date("FlightDate", "yyyy-MM-dd")))
        .withColumn("DayOfWeek", F.dayofweek(F.to_date("FlightDate", "yyyy-MM-dd")).cast(IntegerType()))
    )

    return df


# ---------------------------------------------------------------------------
# UTC timestamp conversion (pure Spark SQL, no Python UDFs)
# ---------------------------------------------------------------------------

def _hhmm_to_local_ts(date_col: str, hhmm_col: str) -> "Column":
    """Convert FlightDate + HHMM float (e.g. 1430.0 = 14:30) to a local timestamp."""
    hour_str = F.lpad(F.floor(F.col(hhmm_col) / 100).cast("string"), 2, "0")
    min_str  = F.lpad((F.col(hhmm_col) % 100).cast(IntegerType()).cast("string"), 2, "0")
    dt_str   = F.concat_ws(" ", F.col(date_col), F.concat(hour_str, F.lit(":"), min_str))
    return F.to_timestamp(dt_str, "yyyy-MM-dd HH:mm")


def add_utc_timestamps(df, airports: dict):
    """Add DepHourUTC and ArrHourUTC columns using Spark's built-in to_utc_timestamp."""
    # Build a lookup DataFrame: Origin/Dest → timezone string
    tz_rows = [(iata, tz) for iata, tz in airports.items()]
    spark = df.sparkSession
    tz_df = spark.createDataFrame(tz_rows, ["airport", "timezone"])

    # Join origin timezone
    df = df.join(tz_df.withColumnRenamed("airport", "Origin").withColumnRenamed("timezone", "_dep_tz"),
                 on="Origin", how="left")
    # Join dest timezone
    df = df.join(tz_df.withColumnRenamed("airport", "Dest").withColumnRenamed("timezone", "_arr_tz"),
                 on="Dest", how="left")

    # Local timestamp columns
    dep_local = _hhmm_to_local_ts("FlightDate", "CRSDepTime")
    arr_local = _hhmm_to_local_ts("FlightDate", "CRSArrTime")

    # Convert to UTC using the per-row timezone column
    df = (
        df
        .withColumn("_dep_local", dep_local)
        .withColumn("_arr_local", arr_local)
        .withColumn("DepUTC", F.to_utc_timestamp("_dep_local", F.col("_dep_tz")))
        .withColumn("ArrUTC", F.to_utc_timestamp("_arr_local", F.col("_arr_tz")))
        .withColumn("DepHourUTC", F.hour("DepUTC").cast(DoubleType()))
        .withColumn("ArrHourUTC", F.hour("ArrUTC").cast(DoubleType()))
        .withColumn("ArrDateHourUTC",                              # for NOAA join
                    F.date_trunc("hour", "ArrUTC"))
        .drop("_dep_local", "_arr_local", "_dep_tz", "_arr_tz")
    )

    return df


# ---------------------------------------------------------------------------
# Lane mean delay feature
# ---------------------------------------------------------------------------

def add_lane_mean_delay(df):
    """Mean ArrDelay per Origin-Dest lane over all training rows as a static feature."""
    lane_stats = (
        df.groupBy("Origin", "Dest")
        .agg(F.mean("ArrDelay").alias("LaneMeanDelay"))
    )
    return df.join(lane_stats, on=["Origin", "Dest"], how="left")


# ---------------------------------------------------------------------------
# NOAA loader (no Python UDFs — parse WND/VIS/TMP with Spark string ops)
# ---------------------------------------------------------------------------

def load_noaa(spark: SparkSession, raw_noaa_dir: str, years: list) -> "DataFrame":
    all_csvs = []
    for year in years:
        pattern  = os.path.join(raw_noaa_dir, str(year), "*.csv")
        pattern2 = os.path.join(raw_noaa_dir, f"*{year}*.csv")
        all_csvs.extend(glob.glob(pattern))
        all_csvs.extend(glob.glob(pattern2))

    all_csvs = list(set(all_csvs))
    if not all_csvs:
        print(f"  WARNING: No NOAA CSVs found for years {years} under {raw_noaa_dir}. "
              "Weather features will be filled with 0.")
        return None

    print(f"  Loading {len(all_csvs)} NOAA weather files...")

    raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(all_csvs)
    )

    # NOAA ISD compound field format:
    #   WND: "ddd,q,T,ffff,q"  — speed is element [3], in 10ths m/s; 9999 = missing
    #   VIS: "dddddd,q,V,q"    — visibility in meters, element [0]; 999999 = missing
    #   TMP: "+dddd,q" or "-dddd,q" — temp in 10ths Celsius, element [0]; +9999 = missing

    noaa = (
        raw
        .filter(F.col("DATE").isNotNull())
        # Derive ICAO from filename: download_noaa.py names files "KATL_722190014732.csv"
        # input_file_name() returns the full path; extract the leading 4-char ICAO (K + 3 letters)
        .withColumn("_fname", F.input_file_name())
        .withColumn("DestICAO",
                    F.regexp_extract(F.col("_fname"), r'/(K[A-Z]{3})_', 1))
        .withColumn("_wnd_speed_raw",
                    F.split(F.col("WND"), ",").getItem(3).cast(DoubleType()))
        .withColumn("_vis_raw",
                    F.split(F.col("VIS"), ",").getItem(0).cast(DoubleType()))
        .withColumn("_tmp_raw",
                    F.regexp_extract(F.col("TMP"), r'^([+\-]?\d+)', 1).cast(DoubleType()))
        .withColumn("DestWindSpeed",
                    F.when(F.col("_wnd_speed_raw") >= 9999, F.lit(None))
                     .otherwise(F.col("_wnd_speed_raw") / 10.0))
        .withColumn("DestVisibility",
                    F.when(F.col("_vis_raw") >= 999999, F.lit(None))
                     .otherwise(F.col("_vis_raw")))
        .withColumn("DestTemp",
                    F.when(F.col("_tmp_raw") >= 9999, F.lit(None))
                     .otherwise(F.col("_tmp_raw") / 10.0))
        # DATE looks like "2023-01-15T14:00:00"
        .withColumn("WeatherHourUTC",
                    F.date_trunc("hour", F.to_timestamp("DATE", "yyyy-MM-dd'T'HH:mm:ss")))
        .filter(F.col("DestICAO") != "")
        .select(
            F.col("DestICAO"),
            F.col("WeatherHourUTC"),
            F.col("DestWindSpeed"),
            F.col("DestVisibility"),
            F.col("DestTemp"),
        )
        .dropDuplicates(["DestICAO", "WeatherHourUTC"])
    )

    return noaa


def join_weather(df, noaa_df, airports: dict):
    """Join NOAA weather to flights on (Dest ICAO, ArrDateHourUTC).
    US airports: ICAO = 'K' + IATA  (e.g. ORD → KORD)."""
    if noaa_df is None:
        return (
            df
            .withColumn("DestWindSpeed", F.lit(3.0))
            .withColumn("DestVisibility", F.lit(10000.0))
            .withColumn("DestTemp", F.lit(15.0))
        )

    # Add ICAO column to flights (K + 3-char IATA)
    df = df.withColumn("_DestICAO", F.concat(F.lit("K"), F.col("Dest")))

    df = df.join(
        F.broadcast(noaa_df.withColumnRenamed("WeatherHourUTC", "_WeatherHour")),
        on=(F.col("_DestICAO") == F.col("DestICAO")) &
           (F.col("ArrDateHourUTC") == F.col("_WeatherHour")),
        how="left",
    ).drop("_WeatherHour", "DestICAO", "_DestICAO")

    df = (
        df
        .withColumn("DestWindSpeed",  F.coalesce(F.col("DestWindSpeed"),  F.lit(3.0)))
        .withColumn("DestVisibility", F.coalesce(F.col("DestVisibility"), F.lit(10000.0)))
        .withColumn("DestTemp",       F.coalesce(F.col("DestTemp"),       F.lit(15.0)))
    )

    return df


# ---------------------------------------------------------------------------
# Model training
# ---------------------------------------------------------------------------

FEATURE_COLS = [
    "DepDelay", "Distance", "DepHourUTC", "ArrHourUTC",
    "DayOfWeek", "DestWindSpeed", "DestVisibility", "DestTemp", "LaneMeanDelay",
]
LABEL_COL = "delta_delay"


def build_pipeline(regressor):
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features", handleInvalid="skip")
    return Pipeline(stages=[assembler, regressor])


def train_models(train_df, cfg: dict, seed: int, skip_rf: bool):
    gbt = GBTRegressor(
        featuresCol="features", labelCol=LABEL_COL,
        maxIter=cfg["gbt_max_iter"], maxDepth=cfg["gbt_max_depth"], seed=seed,
    )
    gbt_pipeline = build_pipeline(gbt)
    print("  Training GBT...")
    gbt_model = gbt_pipeline.fit(train_df)
    print("  GBT training complete.")

    rf_model = None
    if not skip_rf:
        rf = RandomForestRegressor(
            featuresCol="features", labelCol=LABEL_COL,
            numTrees=cfg["rf_num_trees"], seed=seed,
        )
        rf_pipeline = build_pipeline(rf)
        print("  Training RandomForest...")
        rf_model = rf_pipeline.fit(train_df)
        print("  RandomForest training complete.")

    return gbt_model, rf_model


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------

def evaluate(model, val_df, model_name: str):
    preds = model.transform(val_df)
    evaluator_mae  = RegressionEvaluator(labelCol=LABEL_COL, predictionCol="prediction", metricName="mae")
    evaluator_rmse = RegressionEvaluator(labelCol=LABEL_COL, predictionCol="prediction", metricName="rmse")
    mae  = evaluator_mae.evaluate(preds)
    rmse = evaluator_rmse.evaluate(preds)

    # Baseline 1: no-change (delta_delay = 0)
    preds_b1 = preds.withColumn("prediction", F.lit(0.0))
    mae_b1   = evaluator_mae.evaluate(preds_b1)

    # Baseline 2: lane mean delay
    preds_b2 = preds.withColumn("prediction", F.col("LaneMeanDelay") - F.col("DepDelay"))
    mae_b2   = evaluator_mae.evaluate(preds_b2)

    print(f"\n  --- {model_name} Evaluation (val_year holdout) ---")
    print(f"  {model_name}       MAE={mae:.3f}  RMSE={rmse:.3f}")
    print(f"  Baseline (no-change)  MAE={mae_b1:.3f}")
    print(f"  Baseline (lane mean)  MAE={mae_b2:.3f}")
    return {"mae": mae, "rmse": rmse, "baseline_nochange_mae": mae_b1, "baseline_lanemean_mae": mae_b2}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="HyperLogistics-Stream: Offline Batch Training Pipeline")
    parser.add_argument("--config", default="config/pipeline_config.yaml",
                        help="Path to YAML config file (default: config/pipeline_config.yaml)")
    parser.add_argument("--cores", type=int, default=4,
                        help="Number of Spark local cores (default: 4)")
    parser.add_argument("--shuffle-partitions", type=int, default=64,
                        help="spark.sql.shuffle.partitions (default: 64)")
    parser.add_argument("--skip-rf", action="store_true",
                        help="Skip RandomForest training to save time")
    args = parser.parse_args()

    # Resolve config path relative to script location
    script_dir = Path(__file__).parent.parent
    cfg_path   = args.config if os.path.isabs(args.config) else str(script_dir / args.config)

    print(f"Loading config from: {cfg_path}")
    cfg = load_config(cfg_path)

    base_dir = str(script_dir)
    paths    = cfg["paths"]
    train_cfg = cfg["training"]
    airports  = cfg["airports"]

    raw_bts  = resolve_path(paths, "raw_bts",  base_dir)
    raw_noaa = resolve_path(paths, "raw_noaa", base_dir)
    models_dir = resolve_path(paths, "models",  base_dir)
    Path(models_dir).mkdir(parents=True, exist_ok=True)

    seed       = train_cfg["seed"]
    train_yrs  = train_cfg["train_years"]
    val_yr     = train_cfg["val_year"]
    all_years  = sorted(set(train_yrs + [val_yr]))

    print(f"\n=== HyperLogistics-Stream: Batch Training Pipeline ===")
    print(f"  Train years : {train_yrs}")
    print(f"  Val year    : {val_yr}")
    print(f"  Spark cores : {args.cores}")
    print(f"  Shuffle ptns: {args.shuffle_partitions}")

    spark = create_spark(args.cores, args.shuffle_partitions)
    spark.sparkContext.setLogLevel("WARN")

    # 1. Load BTS
    print("\n[1/6] Loading BTS flight data...")
    flights = load_bts(spark, raw_bts, all_years, airports)

    # 2. UTC timestamps
    print("[2/6] Adding UTC timestamps...")
    flights = add_utc_timestamps(flights, airports)

    # 3. Lane mean delay (computed on all loaded rows, then split)
    print("[3/6] Computing lane mean delay...")
    flights = add_lane_mean_delay(flights)

    # 4. NOAA weather join
    print("[4/6] Loading and joining NOAA weather...")
    noaa = load_noaa(spark, raw_noaa, all_years)
    flights = join_weather(flights, noaa, airports)

    # 5. Drop nulls in feature columns, split train/val
    feature_select = FEATURE_COLS + [LABEL_COL, "Origin", "Dest", "Year",
                                     "FlightDate", "Reporting_Airline"]
    flights_clean = flights.select(feature_select).dropna(subset=FEATURE_COLS + [LABEL_COL])
    flights_clean.cache()

    train_df = flights_clean.filter(F.col("Year").isin(train_yrs))
    val_df   = flights_clean.filter(F.col("Year") == val_yr)

    n_train = train_df.count()
    n_val   = val_df.count()
    print(f"  Train rows: {n_train:,}  |  Val rows: {n_val:,}")

    # 6. Train
    print("[5/6] Training models...")
    gbt_model, rf_model = train_models(train_df, train_cfg, seed, args.skip_rf)

    # 7. Evaluate
    print("[6/6] Evaluating on holdout set...")
    gbt_metrics = evaluate(gbt_model, val_df, "GBT")
    if rf_model:
        rf_metrics = evaluate(rf_model, val_df, "RandomForest")

    # 8. Save models
    gbt_path = os.path.join(models_dir, "gbt_pipeline_v1")
    gbt_model.write().overwrite().save(gbt_path)
    print(f"\n  GBT model saved → {gbt_path}")

    if rf_model:
        rf_path = os.path.join(models_dir, "rf_pipeline_v1")
        rf_model.write().overwrite().save(rf_path)
        print(f"  RF  model saved → {rf_path}")

    print("\n=== Batch Training Complete ===")
    spark.stop()


if __name__ == "__main__":
    main()
