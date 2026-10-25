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
import json
import os
import glob
import time
from pathlib import Path

import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator


# ---------------------------------------------------------------------------
# Config
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

def create_spark(cores: int, shuffle_partitions: int) -> SparkSession:
    return (
        SparkSession.builder
        .appName("HyperLogistics-BatchTrain")
        .master(f"local[{cores}]")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEM", "4g"))
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# BTS loader — reads by column name, no fixed positional schema
# ---------------------------------------------------------------------------

def load_bts(spark: SparkSession, raw_bts_dir: str, years: list, airports: dict):
    known = set(airports.keys())
    all_csvs = []
    for year in years:
        for pattern in [
            os.path.join(raw_bts_dir, f"*{year}*.csv"),
            os.path.join(raw_bts_dir, str(year), "*.csv"),
        ]:
            all_csvs.extend(glob.glob(pattern))

    if not all_csvs:
        raise FileNotFoundError(
            f"No BTS CSVs found for years {years} under {raw_bts_dir}\n"
            "Download: python scripts/download_bts.py --years 2018 2019 2021 2022 2023 --extract"
        )

    print(f"  Loading {len(all_csvs)} BTS CSV files for years {years}...")

    # Read all columns as strings first so column selection works regardless of order
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

    return (
        df
        .filter((F.col("Cancelled") == 0) & (F.col("Diverted") == 0))
        .filter(F.col("DepDelay").isNotNull() & F.col("ArrDelay").isNotNull())
        .filter(F.col("Origin").isin(known) & F.col("Dest").isin(known))
        .withColumn("delta_delay", F.col("ArrDelay") - F.col("DepDelay"))
        .withColumn("Year", F.year(F.to_date("FlightDate", "yyyy-MM-dd")))
        .withColumn("DayOfWeek",
                    F.dayofweek(F.to_date("FlightDate", "yyyy-MM-dd")).cast(IntegerType()))
    )


# ---------------------------------------------------------------------------
# UTC timestamps — pure Spark built-ins, no Python UDFs
# ---------------------------------------------------------------------------

def _hhmm_to_local_ts(date_col: str, hhmm_col: str):
    """Convert FlightDate + HHMM float (1430.0 → 14:30) to local timestamp."""
    hour_str = F.lpad(F.floor(F.col(hhmm_col) / 100).cast("string"), 2, "0")
    min_str  = F.lpad((F.col(hhmm_col) % 100).cast(IntegerType()).cast("string"), 2, "0")
    dt_str   = F.concat_ws(" ", F.col(date_col), F.concat(hour_str, F.lit(":"), min_str))
    return F.to_timestamp(dt_str, "yyyy-MM-dd HH:mm")


def add_utc_timestamps(df, airports: dict):
    spark = df.sparkSession
    tz_df = spark.createDataFrame(
        [(iata, tz) for iata, tz in airports.items()],
        ["airport", "timezone"]
    )

    df = (
        df
        .join(tz_df.withColumnRenamed("airport", "Origin").withColumnRenamed("timezone", "_dep_tz"),
              on="Origin", how="left")
        .join(tz_df.withColumnRenamed("airport", "Dest").withColumnRenamed("timezone", "_arr_tz"),
              on="Dest", how="left")
        .withColumn("_dep_local", _hhmm_to_local_ts("FlightDate", "CRSDepTime"))
        .withColumn("_arr_local", _hhmm_to_local_ts("FlightDate", "CRSArrTime"))
        .withColumn("DepUTC", F.to_utc_timestamp("_dep_local", F.col("_dep_tz")))
        .withColumn("ArrUTC", F.to_utc_timestamp("_arr_local", F.col("_arr_tz")))
        .withColumn("DepHourUTC", F.hour("DepUTC").cast(DoubleType()))
        .withColumn("ArrHourUTC", F.hour("ArrUTC").cast(DoubleType()))
        .withColumn("ArrDateHourUTC", F.date_trunc("hour", "ArrUTC"))
        .drop("_dep_local", "_arr_local", "_dep_tz", "_arr_tz")
    )
    return df


# ---------------------------------------------------------------------------
# Lane mean delay
# ---------------------------------------------------------------------------

def add_lane_mean_delay(df, train_years):
    """Lane statistics computed from TRAINING years only, so validation labels
    never leak into the features or the lane-mean baseline."""
    lane_stats = (
        df.filter(F.col("Year").isin(train_years))
        .groupBy("Origin", "Dest")
        .agg(F.mean("ArrDelay").alias("LaneMeanDelay"),
             F.mean("delta_delay").alias("LaneMeanDelta"))
    )
    return df.join(lane_stats, on=["Origin", "Dest"], how="left")


# ---------------------------------------------------------------------------
# NOAA loader — parses WND/VIS/TMP with Spark string ops, no Python UDFs
# ---------------------------------------------------------------------------

def load_noaa(spark: SparkSession, raw_noaa_dir: str, years: list):
    all_csvs = []
    for year in years:
        for pattern in [
            os.path.join(raw_noaa_dir, str(year), "*.csv"),
            os.path.join(raw_noaa_dir, f"*{year}*.csv"),
        ]:
            all_csvs.extend(glob.glob(pattern))

    all_csvs = list(set(all_csvs))
    if not all_csvs:
        print(f"  WARNING: No NOAA CSVs found under {raw_noaa_dir}. Weather features → defaults.")
        return None

    print(f"  Loading {len(all_csvs)} NOAA weather files...")

    raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(all_csvs)
    )

    # NOAA ISD compound field format:
    #   WND: "ddd,q,T,ffff,q"  — speed element [3] in 10ths m/s; 9999 = missing
    #   VIS: "dddddd,q,V,q"    — visibility meters element [0]; 999999 = missing
    #   TMP: "+dddd,q"         — temp 10ths Celsius element [0]; +9999 = missing
    # Files named KATL_722190014732.csv — derive ICAO from filename via input_file_name()

    return (
        raw
        .filter(F.col("DATE").isNotNull())
        .withColumn("DestICAO",
                    F.regexp_extract(F.input_file_name(), r'/(K[A-Z]{3})_', 1))
        .withColumn("_wnd",  F.split(F.col("WND"), ",").getItem(3).cast(DoubleType()))
        .withColumn("_vis",  F.split(F.col("VIS"), ",").getItem(0).cast(DoubleType()))
        .withColumn("_tmp",  F.regexp_extract(F.col("TMP"), r'^([+\-]?\d+)', 1).cast(DoubleType()))
        .withColumn("DestWindSpeed",
                    F.when(F.col("_wnd") >= 9999, F.lit(None)).otherwise(F.col("_wnd") / 10.0))
        .withColumn("DestVisibility",
                    F.when(F.col("_vis") >= 999999, F.lit(None)).otherwise(F.col("_vis")))
        .withColumn("DestTemp",
                    F.when(F.col("_tmp") >= 9999, F.lit(None)).otherwise(F.col("_tmp") / 10.0))
        .withColumn("WeatherHourUTC",
                    F.date_trunc("hour", F.to_timestamp("DATE", "yyyy-MM-dd'T'HH:mm:ss")))
        .filter(F.col("DestICAO") != "")
        .select("DestICAO", "WeatherHourUTC", "DestWindSpeed", "DestVisibility", "DestTemp")
        .dropDuplicates(["DestICAO", "WeatherHourUTC"])
    )


def join_weather(df, noaa_df, airports: dict):
    if noaa_df is None:
        return (df
                .withColumn("DestWindSpeed",  F.lit(3.0))
                .withColumn("DestVisibility", F.lit(10000.0))
                .withColumn("DestTemp",       F.lit(15.0)))

    # US airports: ICAO = 'K' + IATA  (ORD → KORD)
    df = df.withColumn("_DestICAO", F.concat(F.lit("K"), F.col("Dest")))

    df = df.join(
        F.broadcast(noaa_df.withColumnRenamed("WeatherHourUTC", "_wHour")),
        on=(F.col("_DestICAO") == F.col("DestICAO")) &
           (F.col("ArrDateHourUTC") == F.col("_wHour")),
        how="left",
    ).drop("_wHour", "DestICAO", "_DestICAO")

    return (
        df
        .withColumn("DestWindSpeed",  F.coalesce("DestWindSpeed",  F.lit(3.0)))
        .withColumn("DestVisibility", F.coalesce("DestVisibility", F.lit(10000.0)))
        .withColumn("DestTemp",       F.coalesce("DestTemp",       F.lit(15.0)))
    )


# ---------------------------------------------------------------------------
# Training
# ---------------------------------------------------------------------------

FEATURE_COLS = [
    "DepDelay", "Distance", "DepHourUTC", "ArrHourUTC",
    "DayOfWeek", "DestWindSpeed", "DestVisibility", "DestTemp", "LaneMeanDelay",
]
LABEL_COL = "delta_delay"


def train_and_evaluate(train_df, val_df, train_cfg: dict, skip_rf: bool):
    seed = train_cfg["seed"]
    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features", handleInvalid="skip")

    gbt = GBTRegressor(
        featuresCol="features", labelCol=LABEL_COL,
        maxIter=train_cfg["gbt_max_iter"], maxDepth=train_cfg["gbt_max_depth"], seed=seed,
    )
    print("  Training GBT...")
    gbt_model = Pipeline(stages=[assembler, gbt]).fit(train_df)
    metrics = {"gbt": _print_metrics(gbt_model, val_df, "GBT")}

    rf_model = None
    if not skip_rf:
        rf = RandomForestRegressor(
            featuresCol="features", labelCol=LABEL_COL,
            numTrees=train_cfg["rf_num_trees"], seed=seed,
        )
        print("  Training RandomForest...")
        rf_model = Pipeline(stages=[assembler, rf]).fit(train_df)
        metrics["rf"] = _print_metrics(rf_model, val_df, "RandomForest")

    return gbt_model, rf_model, metrics


def _print_metrics(model, val_df, name: str) -> dict:
    preds = model.transform(val_df)
    ev = lambda m, d: RegressionEvaluator(labelCol=LABEL_COL, metricName=m).evaluate(d)
    mae, rmse = ev("mae", preds), ev("rmse", preds)

    # Baseline 1: delay unchanged  -> predicted delta = 0
    b1 = preds.withColumn("prediction", F.lit(0.0))
    # Baseline 2: lane mean of delta_delay (training years only)
    b2 = preds.withColumn("prediction", F.col("LaneMeanDelta"))

    out = {
        "mae": mae, "rmse": rmse,
        "baseline_nochange_mae": ev("mae", b1), "baseline_nochange_rmse": ev("rmse", b1),
        "baseline_lanemean_mae": ev("mae", b2), "baseline_lanemean_rmse": ev("rmse", b2),
    }
    print(f"\n  --- {name} (validation holdout) ---")
    print(f"  {name:<20} MAE={mae:.3f}  RMSE={rmse:.3f}")
    print(f"  Baseline no-change     MAE={out['baseline_nochange_mae']:.3f}  RMSE={out['baseline_nochange_rmse']:.3f}")
    print(f"  Baseline lane-mean     MAE={out['baseline_lanemean_mae']:.3f}  RMSE={out['baseline_lanemean_rmse']:.3f}")
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="HyperLogistics-Stream: Offline Batch Training")
    parser.add_argument("--config", default="config/pipeline_config.yaml",
                        help="Path to YAML config (default: config/pipeline_config.yaml)")
    parser.add_argument("--cores", type=int, default=4,
                        help="Spark local cores (default: 4)")
    parser.add_argument("--shuffle-partitions", type=int, default=64,
                        help="spark.sql.shuffle.partitions (default: 64)")
    parser.add_argument("--skip-rf", action="store_true",
                        help="Skip RandomForest to save time")
    args = parser.parse_args()

    script_dir = Path(__file__).parent.parent
    cfg_path   = args.config if os.path.isabs(args.config) else str(script_dir / args.config)

    print(f"Config: {cfg_path}")
    cfg = load_config(cfg_path)

    base_dir   = str(script_dir)
    paths      = cfg["paths"]
    train_cfg  = cfg["training"]
    airports   = cfg["airports"]

    raw_bts    = resolve_path(paths, "raw_bts",  base_dir)
    raw_noaa   = resolve_path(paths, "raw_noaa", base_dir)
    models_dir = resolve_path(paths, "models",   base_dir)
    Path(models_dir).mkdir(parents=True, exist_ok=True)

    train_yrs = train_cfg["train_years"]
    val_yr    = train_cfg["val_year"]
    all_years = sorted(set(train_yrs + [val_yr]))

    print(f"\n=== HyperLogistics-Stream: Batch Training ===")
    print(f"  Train years : {train_yrs}  |  Val year: {val_yr}")
    print(f"  Cores: {args.cores}  |  Shuffle partitions: {args.shuffle_partitions}")

    t0 = time.time()
    spark = create_spark(args.cores, args.shuffle_partitions)
    spark.sparkContext.setLogLevel("WARN")

    print("\n[1/5] Loading BTS data...")
    flights = load_bts(spark, raw_bts, all_years, airports)

    print("[2/5] Adding UTC timestamps...")
    flights = add_utc_timestamps(flights, airports)

    print("[3/5] Computing lane mean delay...")
    flights = add_lane_mean_delay(flights, train_yrs)

    print("[4/5] Loading and joining NOAA weather...")
    noaa    = load_noaa(spark, raw_noaa, all_years)
    flights = join_weather(flights, noaa, airports)

    keep_cols = FEATURE_COLS + [LABEL_COL, "LaneMeanDelta", "Year", "Origin", "Dest",
                                "FlightDate", "Reporting_Airline"]
    flights_clean = (
        flights.select(keep_cols)
        .dropna(subset=FEATURE_COLS + [LABEL_COL, "LaneMeanDelta"])
        .cache()
    )

    train_df = flights_clean.filter(F.col("Year").isin(train_yrs))
    val_df   = flights_clean.filter(F.col("Year") == val_yr)
    n_train, n_val = train_df.count(), val_df.count()
    print(f"  Train rows: {n_train:,}  |  Val rows: {n_val:,}")

    print("[5/5] Training models...")
    gbt_model, rf_model, metrics = train_and_evaluate(train_df, val_df, train_cfg, args.skip_rf)

    gbt_path = os.path.join(models_dir, "gbt_pipeline_v1")
    gbt_model.write().overwrite().save(gbt_path)
    print(f"\n  GBT saved → {gbt_path}")

    if rf_model:
        rf_path = os.path.join(models_dir, "rf_pipeline_v1")
        rf_model.write().overwrite().save(rf_path)
        print(f"  RF  saved → {rf_path}")

    out = {
        "train_years": train_yrs, "val_year": val_yr,
        "train_count": n_train, "val_count": n_val,
        "gbt_mae": metrics["gbt"]["mae"], "gbt_rmse": metrics["gbt"]["rmse"],
        "total_elapsed_sec": round(time.time() - t0, 1),
        "details": metrics,
    }
    metrics_path = os.path.join(models_dir, "training_metrics.json")
    with open(metrics_path, "w") as f:
        json.dump(out, f, indent=2)
    print(f"  Metrics saved → {metrics_path}")

    print("\n=== Batch Training Complete ===")
    spark.stop()


if __name__ == "__main__":
    main()
