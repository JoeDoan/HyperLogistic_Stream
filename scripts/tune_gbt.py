#!/usr/bin/env python3
"""
Warm-JVM GBTRegressor hyperparameter hillclimb for HyperLogistics-Stream.

Reuses pre-materialized train.parquet and val.parquet in a single Spark JVM, caches
assembled feature vectors once, evaluates 5 structured capacity/regularization iterations
against overfitting and RMSE guardrails, writes models/tuning_results.csv after every
step, and promotes accepted champions to models/gbt_pipeline_v1 and training_metrics.json.
"""

import argparse
import csv
import importlib.util
import json
import math
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

BASE_CANDIDATES = (
    (
        "iter1_fine_bins_128",
        {
            "maxDepth": 5,
            "maxIter": 50,
            "stepSize": 0.10,
            "maxBins": 128,
            "minInstancesPerNode": 100,
            "subsamplingRate": 1.0,
        },
    ),
    (
        "iter2_depth6_leaf200",
        {
            "maxDepth": 6,
            "maxIter": 65,
            "stepSize": 0.10,
            "maxBins": 128,
            "minInstancesPerNode": 200,
            "subsamplingRate": 1.0,
        },
    ),
    (
        "iter3_depth7_stochastic",
        {
            "maxDepth": 7,
            "maxIter": 85,
            "stepSize": 0.08,
            "maxBins": 128,
            "minInstancesPerNode": 300,
            "subsamplingRate": 0.8,
        },
    ),
    (
        "iter4_depth8_capacity_probe",
        {
            "maxDepth": 8,
            "maxIter": 90,
            "stepSize": 0.07,
            "maxBins": 128,
            "minInstancesPerNode": 500,
            "subsamplingRate": 0.8,
        },
    ),
)

TUNING_COLUMNS = (
    "iteration",
    "maxDepth",
    "maxIter",
    "stepSize",
    "maxBins",
    "minInstancesPerNode",
    "subsamplingRate",
    "train_mae",
    "train_rmse",
    "val_mae",
    "val_rmse",
    "overfit_gap_mae",
    "overfit_gap_rmse",
    "excess_overfit_gap_mae",
    "excess_overfit_gap_rmse",
    "delta_val_mae_vs_iter0",
    "delta_val_mae_vs_prev_best",
    "train_sec",
    "accepted",
    "status",
)


def should_accept(
    cand_val_mae: float,
    cand_val_rmse: float,
    excess_gap_mae: float,
    excess_gap_rmse: float,
    best_val_mae: float,
    best_val_rmse: float,
) -> tuple[bool, str]:
    if excess_gap_mae > 0.150 or excess_gap_rmse > 0.350:
        return False, "rejected_overfit"
    if cand_val_rmse > best_val_rmse + 0.010:
        return False, "rejected_rmse_regression"
    if cand_val_mae >= best_val_mae - 0.002:
        return False, "rejected_no_mae_gain"
    return True, "accepted_new_champion"


def resolve_iter5_params(iter4_accepted: bool, champion_max_depth: int) -> dict:
    if iter4_accepted:
        return {
            "maxDepth": 8,
            "maxIter": 115,
            "stepSize": 0.055,
            "maxBins": 128,
            "minInstancesPerNode": 600,
            "subsamplingRate": 0.8,
        }
    return {
        "maxDepth": max(6, int(champion_max_depth)),
        "maxIter": 110,
        "stepSize": 0.06,
        "maxBins": 128,
        "minInstancesPerNode": 350,
        "subsamplingRate": 0.8,
    }


def load_baseline_shift(
    ablation_path: Path,
    default_mae: float = 0.288,
    default_rmse: float = 0.507,
) -> tuple[float, float]:
    if not ablation_path.exists():
        return default_mae, default_rmse
    with open(ablation_path, "r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            if row.get("model") == "baseline_lane_mean":
                try:
                    t_mae = float(row["train_mae"])
                    t_rmse = float(row["train_rmse"])
                    v_mae = float(row["val_mae"])
                    v_rmse = float(row["val_rmse"])
                    if math.isfinite(t_mae) and math.isfinite(t_rmse):
                        return v_mae - t_mae, v_rmse - t_rmse
                except (KeyError, TypeError, ValueError):
                    break
    return default_mae, default_rmse


def build_champion_metrics(
    base_metrics: dict,
    *,
    iter_name: str,
    params: dict,
    seed: int,
    train_mae: float,
    train_rmse: float,
    val_mae: float,
    val_rmse: float,
    excess_gap_mae: float,
    feature_importances: dict,
) -> dict:
    updated = dict(base_metrics)
    updated.update({
        "gbt_params": {**params, "seed": seed},
        "gbt_champion_iter": iter_name,
        "gbt_train_mae": train_mae,
        "gbt_train_rmse": train_rmse,
        "gbt_mae": val_mae,
        "gbt_rmse": val_rmse,
        "gbt_overfit_gap_mae": val_mae - train_mae,
        "gbt_overfit_gap_rmse": val_rmse - train_rmse,
        "gbt_excess_overfit_gap_mae": excess_gap_mae,
        "gbt_feature_importances": feature_importances,
    })
    return updated


def _write_tuning_csv_atomic(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".csv.tmp")
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(TUNING_COLUMNS))
        writer.writeheader()
        for r in rows:
            formatted = {
                k: (round(v, 5) if isinstance(v, float) else v)
                for k, v in r.items()
            }
            writer.writerow(formatted)
    tmp.replace(path)


def _write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    tmp.replace(path)


def _load_train_module():
    spec = importlib.util.spec_from_file_location(
        "batch_train", PROJECT_ROOT / "pipeline" / "01_batch_train.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Warm-JVM GBTRegressor Hyperparameter Tuner")
    parser.add_argument("--config", type=str, default=None, help="Path to YAML config file")
    parser.add_argument(
        "--cache-dir",
        type=str,
        required=True,
        help="Directory containing prepared train.parquet and val.parquet",
    )
    parser.add_argument(
        "--baseline-metrics",
        type=str,
        required=True,
        help="Path to Iteration 0 baseline training_metrics.json",
    )
    parser.add_argument("--driver-memory", type=str, default="28g")
    parser.add_argument("--shuffle-partitions", type=int, default=64)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    cache_dir = Path(args.cache_dir)
    train_parquet = cache_dir / "train.parquet"
    val_parquet = cache_dir / "val.parquet"
    baseline_metrics_path = Path(args.baseline_metrics)

    if not train_parquet.exists() or not val_parquet.exists():
        raise FileNotFoundError(
            f"Prepared Parquet cache missing under {cache_dir}; expected train.parquet and val.parquet."
        )
    if not baseline_metrics_path.exists():
        raise FileNotFoundError(f"Baseline metrics JSON not found: {baseline_metrics_path}")

    base_metrics = json.loads(baseline_metrics_path.read_text(encoding="utf-8"))

    from pyspark.ml import PipelineModel
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.regression import GBTRegressor

    bt = _load_train_module()
    cfg = bt.load_config(args.config)
    models_dir = bt.resolve_path(cfg["paths"], "models")
    models_dir.mkdir(parents=True, exist_ok=True)
    seed = int(cfg["training"].get("seed", 42))

    tuning_csv_path = models_dir / "tuning_results.csv"
    champion_metrics_path = models_dir / "training_metrics.json"
    champion_model_path = models_dir / "gbt_pipeline_v1"

    b2_shift_mae, b2_shift_rmse = load_baseline_shift(models_dir / "ablation.csv")

    base_params = base_metrics.get("gbt_params", {})
    iter0_max_depth = int(base_params.get("maxDepth", cfg["training"].get("gbt_max_depth", 5)))
    iter0_max_iter = int(base_params.get("maxIter", cfg["training"].get("gbt_max_iter", 50)))
    iter0_step_size = float(base_params.get("stepSize", cfg["training"].get("gbt_step_size", 0.1)))
    iter0_max_bins = int(base_params.get("maxBins", cfg["training"].get("gbt_max_bins", 32)))
    iter0_min_inst = int(
        base_params.get("minInstancesPerNode", cfg["training"].get("gbt_min_instances_per_node", 1))
    )
    iter0_subsample = float(
        base_params.get("subsamplingRate", cfg["training"].get("gbt_subsampling_rate", 1.0))
    )

    iter0_val_mae = float(base_metrics["gbt_mae"])
    iter0_val_rmse = float(base_metrics["gbt_rmse"])
    iter0_train_mae = (
        float(base_metrics["gbt_train_mae"])
        if base_metrics.get("gbt_train_mae") is not None
        else iter0_val_mae - b2_shift_mae
    )
    iter0_train_rmse = (
        float(base_metrics["gbt_train_rmse"])
        if base_metrics.get("gbt_train_rmse") is not None
        else iter0_val_rmse - b2_shift_rmse
    )
    iter0_gap_mae = iter0_val_mae - iter0_train_mae
    iter0_gap_rmse = iter0_val_rmse - iter0_train_rmse

    best_val_mae = iter0_val_mae
    best_val_rmse = iter0_val_rmse
    champion_iter = "iter0_baseline_default"
    champion_max_depth = iter0_max_depth

    tuning_rows = [
        {
            "iteration": "iter0_baseline_default",
            "maxDepth": iter0_max_depth,
            "maxIter": iter0_max_iter,
            "stepSize": iter0_step_size,
            "maxBins": iter0_max_bins,
            "minInstancesPerNode": iter0_min_inst,
            "subsamplingRate": iter0_subsample,
            "train_mae": iter0_train_mae,
            "train_rmse": iter0_train_rmse,
            "val_mae": iter0_val_mae,
            "val_rmse": iter0_val_rmse,
            "overfit_gap_mae": iter0_gap_mae,
            "overfit_gap_rmse": iter0_gap_rmse,
            "excess_overfit_gap_mae": iter0_gap_mae - b2_shift_mae,
            "excess_overfit_gap_rmse": iter0_gap_rmse - b2_shift_rmse,
            "delta_val_mae_vs_iter0": 0.0,
            "delta_val_mae_vs_prev_best": 0.0,
            "train_sec": 0.0,
            "accepted": True,
            "status": "baseline",
        }
    ]
    _write_tuning_csv_atomic(tuning_csv_path, tuning_rows)

    print("=" * 68)
    print("HyperLogistics-Stream: Warm-JVM GBT Hyperparameter Hillclimb")
    print(f"Iter0 champion: Val MAE={best_val_mae:.4f}  Val RMSE={best_val_rmse:.4f}")
    print(f"Year-shift baseline (B2 val - train): dMAE={b2_shift_mae:+.4f}  dRMSE={b2_shift_rmse:+.4f}")
    print("=" * 68)

    spark = bt.build_spark(
        shuffle_partitions=args.shuffle_partitions,
        driver_memory=args.driver_memory,
        app_name="HyperLogistics-GBTTune",
    )

    try:
        t_prep = time.time()
        train_df = spark.read.parquet(str(train_parquet))
        val_df = spark.read.parquet(str(val_parquet))
        assembler = VectorAssembler(
            inputCols=bt.FEATURE_COLS, outputCol="features", handleInvalid="skip"
        )
        train_vec = assembler.transform(train_df).select("features", bt.LABEL_COL).cache()
        val_vec = assembler.transform(val_df).select("features", bt.LABEL_COL).cache()
        train_n = train_vec.count()
        val_n = val_vec.count()
        print(
            f"[CACHE] Pre-assembled vectors cached: train={train_n:,}  val={val_n:,}  "
            f"({time.time() - t_prep:.1f}s)"
        )

        iter4_accepted = False
        candidates = list(BASE_CANDIDATES)

        for idx in range(5):
            if idx == 4:
                candidates.append((
                    "iter5_adaptive_refinement",
                    resolve_iter5_params(iter4_accepted, champion_max_depth),
                ))
            iter_name, params = candidates[idx]
            print(f"\n[{iter_name}] Fitting GBTRegressor with {params} (seed={seed})...")

            est = GBTRegressor(
                featuresCol="features",
                labelCol=bt.LABEL_COL,
                seed=seed,
                **params,
            )
            t_fit = time.time()
            fitted_gbt = est.fit(train_vec)
            fit_sec = time.time() - t_fit

            cand_train_mae, cand_train_rmse = bt.evaluate_model(
                fitted_gbt, train_vec, label_col=bt.LABEL_COL, model_name=f"{iter_name}-train"
            )
            cand_val_mae, cand_val_rmse = bt.evaluate_model(
                fitted_gbt, val_vec, label_col=bt.LABEL_COL, model_name=f"{iter_name}-val"
            )

            gap_mae = cand_val_mae - cand_train_mae
            gap_rmse = cand_val_rmse - cand_train_rmse
            excess_gap_mae = gap_mae - b2_shift_mae
            excess_gap_rmse = gap_rmse - b2_shift_rmse
            delta_vs_prev_best = cand_val_mae - best_val_mae

            accepted, status = should_accept(
                cand_val_mae=cand_val_mae,
                cand_val_rmse=cand_val_rmse,
                excess_gap_mae=excess_gap_mae,
                excess_gap_rmse=excess_gap_rmse,
                best_val_mae=best_val_mae,
                best_val_rmse=best_val_rmse,
            )

            print(
                f"  [{iter_name}] fit={fit_sec:.1f}s  gap_mae={gap_mae:+.4f}  "
                f"excess_gap_mae={excess_gap_mae:+.4f}  dValMAE={delta_vs_prev_best:+.4f}  -> {status}"
            )

            if iter_name == "iter4_depth8_capacity_probe":
                iter4_accepted = accepted

            tuning_rows.append({
                "iteration": iter_name,
                "maxDepth": params["maxDepth"],
                "maxIter": params["maxIter"],
                "stepSize": params["stepSize"],
                "maxBins": params["maxBins"],
                "minInstancesPerNode": params["minInstancesPerNode"],
                "subsamplingRate": params["subsamplingRate"],
                "train_mae": cand_train_mae,
                "train_rmse": cand_train_rmse,
                "val_mae": cand_val_mae,
                "val_rmse": cand_val_rmse,
                "overfit_gap_mae": gap_mae,
                "overfit_gap_rmse": gap_rmse,
                "excess_overfit_gap_mae": excess_gap_mae,
                "excess_overfit_gap_rmse": excess_gap_rmse,
                "delta_val_mae_vs_iter0": cand_val_mae - iter0_val_mae,
                "delta_val_mae_vs_prev_best": delta_vs_prev_best,
                "train_sec": round(fit_sec, 1),
                "accepted": accepted,
                "status": status,
            })
            _write_tuning_csv_atomic(tuning_csv_path, tuning_rows)

            if accepted:
                best_val_mae = cand_val_mae
                best_val_rmse = cand_val_rmse
                champion_iter = iter_name
                champion_max_depth = params["maxDepth"]

                pm = PipelineModel(stages=[assembler, fitted_gbt])
                pm.write().overwrite().save(str(champion_model_path))

                feature_importances = dict(
                    zip(bt.FEATURE_COLS, fitted_gbt.featureImportances.toArray().tolist())
                )
                updated_metrics = build_champion_metrics(
                    base_metrics,
                    iter_name=iter_name,
                    params=params,
                    seed=seed,
                    train_mae=cand_train_mae,
                    train_rmse=cand_train_rmse,
                    val_mae=cand_val_mae,
                    val_rmse=cand_val_rmse,
                    excess_gap_mae=excess_gap_mae,
                    feature_importances=feature_importances,
                )
                _write_json_atomic(champion_metrics_path, updated_metrics)
                print(
                    f"  [PROMOTED] {iter_name} -> {champion_model_path} "
                    f"(Val MAE={best_val_mae:.4f}, Val RMSE={best_val_rmse:.4f})"
                )

        print("\n" + "=" * 68)
        print(
            f"Tuning complete. Champion: {champion_iter}  "
            f"Val MAE={best_val_mae:.4f} (vs Iter0 {best_val_mae - iter0_val_mae:+.4f})  "
            f"Val RMSE={best_val_rmse:.4f}"
        )
        print(f"Results table: {tuning_csv_path}")
        print("=" * 68)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
