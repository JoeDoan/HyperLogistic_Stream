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
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor, RandomForestRegressor
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.hubs import AIRPORT_TZ  # noqa: E402

DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "pipeline_config.yaml"

BASE_COLS = ["DepDelay", "Distance", "DepHourUTC", "ArrHourUTC", "DayOfWeek", "LaneMeanDelay"]
DEST_WX_V1 = ["DestWindSpeed", "DestVisibility", "DestTemp"]
DEST_WX_CEIL = DEST_WX_V1 + ["DestCeiling"]
ORIGIN_WX = ["OriginWindSpeed", "OriginVisibility", "OriginCeiling", "OriginTemp"]
WEATHER_COLS = DEST_WX_CEIL + ORIGIN_WX

# Final (served) model uses every feature; ablation steps add weather in stages.
FEATURE_COLS = BASE_COLS + WEATHER_COLS
ABLATION_SETS = [
    ("gbt_no_weather", "lane + schedule only", BASE_COLS),
    ("gbt_dest_wind_vis_temp", "+ destination wind, visibility, temp (v1)", BASE_COLS + DEST_WX_V1),
    ("gbt_dest_plus_ceiling", "+ destination ceiling", BASE_COLS + DEST_WX_CEIL),
    ("gbt_full_weather", "+ origin wind, visibility, ceiling, temp", FEATURE_COLS),
]
WX_FILL = {"WindSpeed": 3.5, "Visibility": 9000.0, "Ceiling": 22000.0, "Temp": 15.0}
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
        .config("spark.sql.session.timeZone", "UTC")
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
        .withColumn("wheels_off_hour_utc", F.date_trunc("hour", F.col("wheels_off_utc")))
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
            F.col("CIG"),
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
        # CIG = "height_m,quality,method,cavok"; 99999 means no ceiling reported (clear sky)
        .withColumn("_cig_raw", F.split(F.col("CIG"), ",").getItem(0).cast(DoubleType()))
        .withColumn(
            "ceiling_m",
            F.when(F.col("_cig_raw").isNull() | (F.col("_cig_raw") >= 99999), F.lit(22000.0))
             .otherwise(F.col("_cig_raw"))
        )
        .select("icao", "utc_hour", "wind_speed_mps", "visibility_m", "temp_c", "ceiling_m")
        .dropDuplicates(["icao", "utc_hour"])
    )
    return weather_df


def join_weather(flights_df, weather_df):
    """Broadcast left-join NOAA weather twice: origin at wheels-off UTC hour
    (known at departure), whereas destination weather is joined from observed NOAA ISD
    at the scheduled arrival UTC hour (sched_arr_hour_utc, 1 to 6 hours after departure,
    acting as an oracle forecast upper bound for destination conditions)."""
    flights_df = (
        flights_df
        .withColumn("dest_icao", F.concat(F.lit("K"), F.col("Dest")))
        .withColumn("origin_icao", F.concat(F.lit("K"), F.col("Origin")))
    )

    def renamed(prefix, key):
        return weather_df.select(
            F.col("icao").alias(f"{key}_icao"),
            F.col("utc_hour").alias(f"{key}_hour"),
            F.col("wind_speed_mps").alias(f"{prefix}WindSpeed"),
            F.col("visibility_m").alias(f"{prefix}Visibility"),
            F.col("temp_c").alias(f"{prefix}Temp"),
            F.col("ceiling_m").alias(f"{prefix}Ceiling"),
        )

    dest_w = renamed("Dest", "dw")
    orig_w = renamed("Origin", "ow")
    joined = (
        flights_df
        .join(F.broadcast(dest_w),
              (flights_df.dest_icao == dest_w.dw_icao) &
              (flights_df.sched_arr_hour_utc == dest_w.dw_hour), how="left")
        .drop("dw_icao", "dw_hour")
        .join(F.broadcast(orig_w),
              (flights_df.origin_icao == orig_w.ow_icao) &
              (flights_df.wheels_off_hour_utc == orig_w.ow_hour), how="left")
        .drop("ow_icao", "ow_hour")
    )
    fills = {f"{side}{k}": v for side in ("Dest", "Origin") for k, v in WX_FILL.items()}
    return joined.fillna(fills)


def build_baseline_table(train_data):
    """Typical delay by hub x local hour x weekday from the training years only.

    Departures use the origin hub and scheduled local departure hour; arrivals use the
    destination hub and scheduled local arrival hour. Local hour comes straight from the
    BTS HHMM fields, so "Thursday evening" means evening at the hub.
    """
    def hhmm_hour(col):
        return (F.floor(F.col(col).cast("int") / 100) % 24).cast("int")

    dep = (
        train_data
        .withColumn("hour_local", hhmm_hour("CRSDepTime"))
        .groupBy(F.col("Origin").alias("hub"), "hour_local", "DayOfWeek")
        .agg(F.avg("DepDelay").alias("typical_dep_delay"),
             F.expr("percentile_approx(DepDelay, 0.5)").alias("median_dep_delay"),
             F.count("*").alias("dep_n"))
    )
    arr = (
        train_data
        .withColumn("hour_local", hhmm_hour("CRSArrTime"))
        .groupBy(F.col("Dest").alias("hub"), "hour_local", "DayOfWeek")
        .agg(F.avg("ArrDelay").alias("typical_arr_delay"),
             F.expr("percentile_approx(ArrDelay, 0.5)").alias("median_arr_delay"),
             F.count("*").alias("arr_n"))
    )
    return (
        dep.join(arr, on=["hub", "hour_local", "DayOfWeek"], how="full_outer")
        .withColumnRenamed("DayOfWeek", "day_of_week")
        .fillna(0, subset=["dep_n", "arr_n"])
    )


def resolve_gbt_params(training_cfg: dict, args) -> dict:
    spec = (
        ("maxIter", "gbt_max_iter", "gbt_max_iter", 50, int),
        ("maxDepth", "gbt_max_depth", "gbt_max_depth", 5, int),
        ("stepSize", "gbt_step_size", "gbt_step_size", 0.1, float),
        ("subsamplingRate", "gbt_subsampling_rate", "gbt_subsampling_rate", 1.0, float),
        ("minInstancesPerNode", "gbt_min_instances", "gbt_min_instances_per_node", 1, int),
        ("maxBins", "gbt_max_bins", "gbt_max_bins", 32, int),
    )
    params = {}
    for spark_key, cli_attr, yaml_key, default, cast in spec:
        cli_val = getattr(args, cli_attr, None)
        raw = cli_val if cli_val is not None else training_cfg.get(yaml_key, default)
        params[spark_key] = cast(raw)
    params["seed"] = int(training_cfg.get("seed", 42))
    return params


def evaluate_model(model, test_df, label_col=LABEL_COL, model_name="Model"):
    predictions = model.transform(test_df)
    row = predictions.select(
        F.avg(F.abs(F.col("prediction") - F.col(label_col))).alias("mae"),
        F.sqrt(F.avg(F.pow(F.col("prediction") - F.col(label_col), 2))).alias("rmse"),
    ).first()
    mae, rmse = float(row["mae"]), float(row["rmse"])
    print(f"  [{model_name}] MAE={mae:.3f} min  RMSE={rmse:.3f} min")
    return mae, rmse


def compute_baselines(test_df):
    """Compute Baseline 1 (delta=0) and Baseline 2 (train lane mean) MAE/RMSE."""
    b1_err = F.lit(0.0) - F.col(LABEL_COL)
    b2_err = F.col("LaneMeanDelay") - F.col(LABEL_COL)
    row = test_df.select(
        F.avg(F.abs(b1_err)).alias("b1_mae"),
        F.sqrt(F.avg(F.pow(b1_err, 2))).alias("b1_rmse"),
        F.avg(F.abs(b2_err)).alias("b2_mae"),
        F.sqrt(F.avg(F.pow(b2_err, 2))).alias("b2_rmse"),
    ).first()
    b1_mae, b1_rmse = float(row["b1_mae"]), float(row["b1_rmse"])
    b2_mae, b2_rmse = float(row["b2_mae"]), float(row["b2_rmse"])
    print(f"  [Baseline-1 No-Change]  MAE={b1_mae:.3f} min  RMSE={b1_rmse:.3f} min")
    print(f"  [Baseline-2 Lane Mean]  MAE={b2_mae:.3f} min  RMSE={b2_rmse:.3f} min")
    return {"b1_mae": b1_mae, "b1_rmse": b1_rmse, "b2_mae": b2_mae, "b2_rmse": b2_rmse}


def main():
    parser = argparse.ArgumentParser(description="HyperLogistics Batch Training Pipeline")
    parser.add_argument("--config", type=str, default=None, help="Path to YAML config file")
    parser.add_argument("--cores", type=int, default=None, help="Spark local cores (default: all)")
    parser.add_argument("--shuffle-partitions", type=int, default=64, help="spark.sql.shuffle.partitions")
    parser.add_argument("--skip-rf", action="store_true", help="Skip Random Forest training to save time")
    parser.add_argument(
        "--skip-ablation",
        action="store_true",
        help="Fit only the full 14-feature GBT instead of all 4 ablation sets",
    )
    parser.add_argument("--gbt-max-iter", type=int, default=None)
    parser.add_argument("--gbt-max-depth", type=int, default=None)
    parser.add_argument("--gbt-step-size", type=float, default=None)
    parser.add_argument("--gbt-subsampling-rate", type=float, default=None)
    parser.add_argument("--gbt-min-instances", type=int, default=None)
    parser.add_argument("--gbt-max-bins", type=int, default=None)
    parser.add_argument("--rf-num-trees", type=int, default=None)
    parser.add_argument("--rf-max-depth", type=int, default=None)
    parser.add_argument(
        "--cache-dir",
        type=str,
        default=None,
        help="Optional local directory to materialize/reuse prepared train_df and val_df Parquet across tuning runs",
    )
    parser.add_argument(
        "--metrics-out",
        type=str,
        default=None,
        help="Optional path for output metrics JSON instead of models/training_metrics.json",
    )
    parser.add_argument(
        "--no-save-model",
        action="store_true",
        help="Evaluate hyperparameters without overwriting models/gbt_pipeline_v1 on Drive",
    )
    args = parser.parse_args()

    cfg = load_config(args.config)
    bts_dir = str(resolve_path(cfg["paths"], "raw_bts"))
    noaa_dir = str(resolve_path(cfg["paths"], "raw_noaa"))
    models_dir = resolve_path(cfg["paths"], "models")
    models_dir.mkdir(parents=True, exist_ok=True)
    airports = cfg.get("airports", AIRPORT_TZ)
    gbt_params = resolve_gbt_params(cfg["training"], args)

    train_years = cfg["training"]["train_years"]
    val_year = cfg["training"]["val_year"]
    all_years = sorted(set(train_years + [val_year]))

    print("=" * 60)
    print("HyperLogistics-Stream: Phase 1 Batch Training")
    print(f"Train years: {train_years}  |  Val year: {val_year}  |  Hubs: {len(airports)}")
    print(f"GBT params: {gbt_params}")
    print("=" * 60)

    spark = build_spark(
        cores=args.cores,
        shuffle_partitions=args.shuffle_partitions,
        driver_memory=cfg.get("spark", {}).get("driver_memory", "4g"),
    )

    t0 = time.time()
    cache_dir = Path(args.cache_dir) if args.cache_dir else None
    cache_train = cache_dir / "train.parquet" if cache_dir else None
    cache_val = cache_dir / "val.parquet" if cache_dir else None

    if cache_dir and cache_train.exists() and cache_val.exists():
        print(f"[CACHE] Reusing prepared train/val Parquet from {cache_dir}")
        train_df = spark.read.parquet(str(cache_train)).cache()
        val_df = spark.read.parquet(str(cache_val)).cache()
    else:
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

        baseline_tbl = build_baseline_table(train_data).coalesce(1)
        baseline_path = str(models_dir / "baseline_hub_hour_dow.parquet")
        baseline_tbl.write.mode("overwrite").parquet(baseline_path)
        baseline_pd = spark.read.parquet(baseline_path).toPandas().sort_values(
            ["hub", "day_of_week", "hour_local"])
        baseline_csv = models_dir / "baseline_hub_hour_dow.csv"
        baseline_pd.round(3).to_csv(baseline_csv, index=False)
        print(f"[SAVE] Baseline table ({len(baseline_pd):,} hub-hour-weekday cells, "
              f"train years {train_years}) -> {baseline_path} + {baseline_csv.name}")

        lane_mean_cached = spark.read.parquet(lane_mean_path).cache()
        train_cols = FEATURE_COLS + [LABEL_COL]
        train_prepared = (
            train_data
            .join(F.broadcast(lane_mean_cached), on=["Origin", "Dest"], how="left")
            .fillna(0.0, subset=["LaneMeanDelay"])
            .select(train_cols)
            .dropna(subset=train_cols)
        )
        val_prepared = (
            val_data
            .join(F.broadcast(lane_mean_cached), on=["Origin", "Dest"], how="left")
            .fillna(0.0, subset=["LaneMeanDelay"])
            .select(train_cols)
            .dropna(subset=train_cols)
        )
        if cache_dir:
            cache_dir.mkdir(parents=True, exist_ok=True)
            train_prepared.write.mode("overwrite").parquet(str(cache_train))
            val_prepared.write.mode("overwrite").parquet(str(cache_val))
            print(f"[CACHE] Materialized prepared train/val Parquet -> {cache_dir}")
            train_df = spark.read.parquet(str(cache_train)).cache()
            val_df = spark.read.parquet(str(cache_val)).cache()
        else:
            train_df = train_prepared.cache()
            val_df = val_prepared.cache()

    train_count = train_df.count()
    val_count = val_df.count()
    print(f"[DATA] Train: {train_count:,}  Val: {val_count:,}  (elapsed {time.time()-t0:.1f}s)")

    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features", handleInvalid="skip")

    print(f"\n[EVAL] Computing holdout baselines on {val_year}:")
    baseline_metrics = compute_baselines(val_df)

    if args.skip_ablation:
        print("\n[TRAIN] Training full 14-feature GBTRegressor Pipeline (--skip-ablation)...")
        est = GBTRegressor(featuresCol="features", labelCol=LABEL_COL, **gbt_params)
        t_gbt = time.time()
        gbt_pipeline_model = Pipeline(stages=[assembler, est]).fit(train_df)
        print(f"  GBT training time: {time.time()-t_gbt:.1f}s")
        gbt_train_mae, gbt_train_rmse = evaluate_model(gbt_pipeline_model, train_df, model_name="GBT-Train")
        gbt_mae, gbt_rmse = evaluate_model(gbt_pipeline_model, val_df, model_name="GBT-Val")
    else:
        print("\n[EVAL] Computing in-sample baselines on train years:")
        train_baselines = compute_baselines(train_df)
        print("\n[ABLATION] Weather feature ablation (GBT, same hyperparameters):")
        ablation_rows = [
            {
                "model": "baseline_no_change",
                "description": "predict delta = 0",
                "n_features": 0,
                "train_mae": train_baselines["b1_mae"],
                "train_rmse": train_baselines["b1_rmse"],
                "val_mae": baseline_metrics["b1_mae"],
                "val_rmse": baseline_metrics["b1_rmse"],
                "train_sec": 0.0,
            },
            {
                "model": "baseline_lane_mean",
                "description": "training lane mean delta",
                "n_features": 1,
                "train_mae": train_baselines["b2_mae"],
                "train_rmse": train_baselines["b2_rmse"],
                "val_mae": baseline_metrics["b2_mae"],
                "val_rmse": baseline_metrics["b2_rmse"],
                "train_sec": 0.0,
            },
        ]
        gbt_pipeline_model = None
        for name, desc, cols in ABLATION_SETS:
            asm = VectorAssembler(inputCols=cols, outputCol="features", handleInvalid="skip")
            est = GBTRegressor(featuresCol="features", labelCol=LABEL_COL, **gbt_params)
            t_a = time.time()
            fitted = Pipeline(stages=[asm, est]).fit(train_df)
            sec = time.time() - t_a
            a_train_mae, a_train_rmse = evaluate_model(fitted, train_df, model_name=f"{name}-train")
            a_mae, a_rmse = evaluate_model(fitted, val_df, model_name=name)
            ablation_rows.append({
                "model": name,
                "description": desc,
                "n_features": len(cols),
                "train_mae": a_train_mae,
                "train_rmse": a_train_rmse,
                "val_mae": a_mae,
                "val_rmse": a_rmse,
                "train_sec": round(sec, 1),
            })
            if cols == FEATURE_COLS:
                gbt_pipeline_model = fitted  # the full-weather model is the one served
                gbt_train_mae, gbt_train_rmse = a_train_mae, a_train_rmse
                gbt_mae, gbt_rmse = a_mae, a_rmse

        import pandas as pd
        ablation = pd.DataFrame(ablation_rows)
        no_wx = ablation.loc[ablation["model"] == "gbt_no_weather", "val_mae"].iloc[0]
        ablation["mae_gain_vs_no_weather_pct"] = ((no_wx - ablation["val_mae"]) / no_wx * 100).round(2)
        ablation[["train_mae", "train_rmse", "val_mae", "val_rmse"]] = (
            ablation[["train_mae", "train_rmse", "val_mae", "val_rmse"]].round(4)
        )
        ablation["train_years"] = ",".join(map(str, train_years))
        ablation["val_year"] = val_year
        ablation_path = models_dir / "ablation.csv"
        ablation.to_csv(ablation_path, index=False)
        print(f"[SAVE] Ablation table -> {ablation_path}")

    gbt_overfit_gap_mae = gbt_mae - gbt_train_mae
    gbt_overfit_gap_rmse = gbt_rmse - gbt_train_rmse
    print(
        f"  [GBT Generalization] Train MAE={gbt_train_mae:.3f}  Val MAE={gbt_mae:.3f}  "
        f"Gap (val - train)={gbt_overfit_gap_mae:+.3f} min  "
        f"(RMSE gap={gbt_overfit_gap_rmse:+.3f} min)"
    )

    if not args.no_save_model:
        gbt_model_path = str(models_dir / "gbt_pipeline_v1")
        gbt_pipeline_model.write().overwrite().save(gbt_model_path)
        print(f"[SAVE] GBT PipelineModel -> {gbt_model_path}")
    else:
        print("[SKIP] --no-save-model set; skipping gbt_pipeline_v1 write")

    rf_train_mae, rf_train_rmse = None, None
    rf_mae, rf_rmse = None, None
    if not args.skip_rf:
        print("\n[TRAIN] Training RandomForestRegressor Pipeline...")
        rf_kwargs = {
            "numTrees": args.rf_num_trees if args.rf_num_trees is not None else cfg["training"]["rf_num_trees"],
            "seed": int(cfg["training"].get("seed", 42)),
        }
        rf_max_depth = args.rf_max_depth if args.rf_max_depth is not None else cfg["training"].get("rf_max_depth")
        if rf_max_depth is not None:
            rf_kwargs["maxDepth"] = int(rf_max_depth)
        rf = RandomForestRegressor(featuresCol="features", labelCol=LABEL_COL, **rf_kwargs)
        rf_pipeline = Pipeline(stages=[assembler, rf])
        t_rf = time.time()
        rf_pipeline_model = rf_pipeline.fit(train_df)
        print(f"  RF training time: {time.time()-t_rf:.1f}s")
        rf_train_mae, rf_train_rmse = evaluate_model(rf_pipeline_model, train_df, model_name="RandomForest-Train")
        rf_mae, rf_rmse = evaluate_model(rf_pipeline_model, val_df, model_name="RandomForest-Val")
        print(
            f"  [RF Generalization]  Train MAE={rf_train_mae:.3f}  Val MAE={rf_mae:.3f}  "
            f"Gap (val - train)={rf_mae - rf_train_mae:+.3f} min"
        )
        if not args.no_save_model:
            rf_model_path = str(models_dir / "rf_pipeline_v1")
            rf_pipeline_model.write().overwrite().save(rf_model_path)
            print(f"[SAVE] RF PipelineModel -> {rf_model_path}")

    metrics = {
        "hub_count": len(airports),
        "train_count": train_count,
        "val_count": val_count,
        **baseline_metrics,
        "gbt_params": gbt_params,
        "gbt_train_mae": gbt_train_mae,
        "gbt_train_rmse": gbt_train_rmse,
        "gbt_mae": gbt_mae,
        "gbt_rmse": gbt_rmse,
        "gbt_overfit_gap_mae": gbt_overfit_gap_mae,
        "gbt_overfit_gap_rmse": gbt_overfit_gap_rmse,
        "gbt_feature_importances": dict(zip(
            FEATURE_COLS, gbt_pipeline_model.stages[-1].featureImportances.toArray().tolist()
        )),
        "rf_train_mae": rf_train_mae,
        "rf_train_rmse": rf_train_rmse,
        "rf_mae": rf_mae,
        "rf_rmse": rf_rmse,
        "train_years": train_years,
        "val_year": val_year,
        "total_elapsed_sec": time.time() - t0,
    }
    metrics_path = Path(args.metrics_out) if args.metrics_out else (models_dir / "training_metrics.json")
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"\n[SAVE] Metrics -> {metrics_path}")

    print("\n" + "=" * 60)
    print("Phase 1 Complete. Models ready for streaming pipeline.")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
