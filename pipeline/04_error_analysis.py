"""
HyperLogistics-Stream: error analysis of the served GBT on the validation year.

Reuses the data loading from 01_batch_train.py, scores the saved gbt_pipeline_v1 and writes
small CSVs the dashboard's Model Insights tab reads:

    models/error_by_hub.csv        MAE / bias per origin hub (GBT vs lane-mean vs no-change)
    models/error_by_hour.csv       MAE per wheels-off hour (UTC)
    models/error_by_month_hub.csv  MAE and mean actual delta delay per month x origin hub
    models/error_by_bucket.csv     MAE / bias per actual-delta-delay bucket

Run after 01_batch_train.py (needs models/gbt_pipeline_v1 and models/lane_mean.parquet):
    python pipeline/04_error_analysis.py
"""
import argparse
import importlib.util
import json
import sys
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parent.parent

BUCKET_EDGES = [(-1e9, -15, "early >15 min"), (-15, -5, "early 5-15 min"),
                (-5, 5, "within 5 min"), (5, 15, "late 5-15 min"), (15, 1e9, "late >15 min")]


def _load_train_module():
    """01_batch_train.py starts with a digit, so import it by path."""
    spec = importlib.util.spec_from_file_location("batch_train", PROJECT_ROOT / "pipeline" / "01_batch_train.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def error_table(scored, group_cols):
    """MAE per model, GBT bias (prediction minus actual) and mean actual delta per group."""
    err = lambda pred: F.abs(pred - F.col("delta_delay"))  # noqa: E731
    return (
        scored.groupBy(*group_cols)
        .agg(
            F.count("*").alias("n"),
            F.avg(err(F.col("prediction"))).alias("mae_gbt"),
            F.avg(err(F.col("LaneMeanDelay"))).alias("mae_lane_mean"),
            F.avg(err(F.lit(0.0))).alias("mae_no_change"),
            F.avg(F.col("prediction") - F.col("delta_delay")).alias("bias_gbt"),
            F.avg("delta_delay").alias("mean_actual"),
        )
        .orderBy(*group_cols)
    )


def bucket_column(label_col="delta_delay"):
    expr = None
    for lo, hi, name in BUCKET_EDGES:
        cond = (F.col(label_col) >= lo) & (F.col(label_col) < hi)
        expr = F.when(cond, name) if expr is None else expr.when(cond, name)
    return expr.otherwise("within 5 min")


def main():
    parser = argparse.ArgumentParser(description="Error analysis of the served GBT")
    parser.add_argument("--config", type=str, default=None)
    parser.add_argument("--cores", type=int, default=None)
    args = parser.parse_args()

    bt = _load_train_module()
    cfg = bt.load_config(args.config)
    models_dir = bt.resolve_path(cfg["paths"], "models")
    airports = cfg.get("airports", bt.AIRPORT_TZ)
    val_year = cfg["training"]["val_year"]

    metrics_file = models_dir / "training_metrics.json"
    if metrics_file.exists():
        trained_on = json.loads(metrics_file.read_text()).get("train_years", [])
        print(f"[ERR] Model trained on {trained_on}; scoring {val_year}")
        if val_year in trained_on:
            sys.exit(f"[ERR] val_year {val_year} is in the model's training years: that would score "
                     f"in-sample data. Pass the config used for training (e.g. "
                     f"--config config/pipeline_config_colab.yaml).")

    spark = bt.build_spark(cores=args.cores)
    flights = bt.add_utc_timestamps(
        spark, bt.load_bts_csv(spark, str(bt.resolve_path(cfg["paths"], "raw_bts")), [val_year], airports),
        airports)
    noaa = bt.load_noaa_weather(spark, str(bt.resolve_path(cfg["paths"], "raw_noaa")), [val_year])
    lane_mean = spark.read.parquet(str(models_dir / "lane_mean.parquet"))

    val = (
        bt.join_weather(flights, noaa)
        .filter(F.col("Year") == val_year)
        .withColumn("Month", F.month(F.to_date("FlightDate", "yyyy-MM-dd")))
        .join(F.broadcast(lane_mean), on=["Origin", "Dest"], how="left")
        .fillna(0.0, subset=["LaneMeanDelay"])
        .dropna(subset=bt.FEATURE_COLS + [bt.LABEL_COL])
    )
    model = PipelineModel.load(str(models_dir / "gbt_pipeline_v1"))
    scored = model.transform(val).withColumn("bucket", bucket_column()).cache()
    print(f"[ERR] Scored {scored.count():,} validation flights from {val_year}")

    outputs = {
        "error_by_hub.csv": error_table(scored, ["Origin"]).withColumnRenamed("Origin", "hub"),
        "error_by_hour.csv": error_table(scored, ["DepHourUTC"]),
        "error_by_month_hub.csv": error_table(scored, ["Month", "Origin"]).withColumnRenamed("Origin", "hub"),
        "error_by_bucket.csv": error_table(scored, ["bucket"]),
    }
    for name, sdf in outputs.items():
        pdf = sdf.toPandas().round(3)
        pdf.to_csv(models_dir / name, index=False)
        print(f"[SAVE] {name}: {len(pdf)} rows")


if __name__ == "__main__":
    main()
