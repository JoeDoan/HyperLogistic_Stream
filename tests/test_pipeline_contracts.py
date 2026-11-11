"""Pure-Python train/serve parity, model-loading guard, and Spark UTC contracts."""
import argparse
import ast
from datetime import datetime
import importlib.util
import json
from pathlib import Path

import pandas as pd
import pytest
import yaml

from config.hubs import HUBS

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def kafka_producer():
    spec = importlib.util.spec_from_file_location(
        "kafka_producer", ROOT / "pipeline" / "02_kafka_producer.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _spark_config_pairs(py_path: Path, func_name: str) -> set[tuple[str, str]]:
    tree = ast.parse(py_path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == func_name:
            pairs = set()
            for call in ast.walk(node):
                if (
                    isinstance(call, ast.Call)
                    and isinstance(call.func, ast.Attribute)
                    and call.func.attr == "config"
                    and len(call.args) >= 2
                    and isinstance(call.args[0], ast.Constant)
                    and isinstance(call.args[1], ast.Constant)
                ):
                    pairs.add((call.args[0].value, call.args[1].value))
            return pairs
    return set()


def test_parse_bts_row_preserves_local_flight_date_across_utc_midnight(kafka_producer):
    row = {
        "FlightDate": "2024-01-15",
        "Reporting_Airline": "UA",
        "Flight_Number_Reporting_Airline": "100",
        "Tail_Number": "N12345",
        "Origin": "LAX",
        "Dest": "JFK",
        "CRSDepTime": "2130",
        "WheelsOff": "2130",
        "CRSArrTime": "0545",
        "DepDelay": "0.0",
        "ArrDelay": "-5.0",
        "Distance": "2475.0",
        "Cancelled": "0.0",
        "Diverted": "0.0",
    }
    event = kafka_producer.parse_bts_row(row)
    assert event is not None
    assert event["flight_date"] == "2024-01-15"
    assert event["wheels_off_utc"] == "2024-01-16T05:30:00Z"
    assert (
        datetime.strptime(event["flight_date"], "%Y-%m-%d").weekday()
        != datetime.strptime(event["wheels_off_utc"][:10], "%Y-%m-%d").weekday()
    )


def test_load_scoring_model_raises_when_missing_by_default(stream_processor, tmp_path):
    with pytest.raises(FileNotFoundError, match="--allow-dummy-model"):
        stream_processor.load_scoring_model(str(tmp_path), allow_dummy_model=False)


def test_load_scoring_model_returns_none_when_dummy_allowed(stream_processor, tmp_path):
    assert stream_processor.load_scoring_model(str(tmp_path), allow_dummy_model=True) is None


@pytest.mark.parametrize(
    "rel_path,func_name",
    [
        ("pipeline/01_batch_train.py", "build_spark"),
        ("pipeline/03_stream_processor.py", "build_spark"),
        ("pipeline/04_live_processor.py", "build_spark"),
        ("tests/test_dedupe_spark.py", "spark"),
        ("tests/test_error_analysis_spark.py", "spark"),
        ("tests/test_live_processor_spark.py", "spark"),
    ],
)
def test_spark_session_timezone_pinned_to_utc(rel_path, func_name):
    configs = _spark_config_pairs(ROOT / rel_path, func_name)
    assert ("spark.sql.session.timeZone", "UTC") in configs, f"UTC timezone missing in {rel_path}:{func_name}"


def test_stream_schema_and_dayofweek_parity_contracts():
    stream_src = (ROOT / "pipeline" / "03_stream_processor.py").read_text(encoding="utf-8")
    tree = ast.parse(stream_src)

    schema_fields = set()
    silver_cols = set()
    gold_cols = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "FLIGHT_SCHEMA":
                    for call in ast.walk(node.value):
                        if (
                            isinstance(call, ast.Call)
                            and isinstance(call.func, ast.Name)
                            and call.func.id == "StructField"
                            and call.args
                            and isinstance(call.args[0], ast.Constant)
                        ):
                            schema_fields.add(call.args[0].value)
                elif isinstance(target, ast.Name) and target.id == "silver_cols" and isinstance(node.value, ast.List):
                    silver_cols.update(
                        elt.value for elt in node.value.elts if isinstance(elt, ast.Constant)
                    )
                elif isinstance(target, ast.Name) and target.id == "gold_cols" and isinstance(node.value, ast.List):
                    gold_cols.update(
                        elt.value for elt in node.value.elts if isinstance(elt, ast.Constant)
                    )

    assert "flight_date" in schema_fields
    assert "flight_date" in silver_cols
    assert "is_fallback_prediction" in gold_cols
    assert '--allow-dummy-model' in stream_src
    assert 'F.to_date(F.col("flight_date"), "yyyy-MM-dd")' in stream_src
    assert "F.coalesce(" in stream_src


def test_colab_config_driver_memory_40_hubs_and_gbt_tuning_keys():
    metrics = json.loads((ROOT / "models" / "training_metrics.json").read_text(encoding="utf-8"))
    expected_champion = {
        "maxIter": 90,
        "maxDepth": 8,
        "stepSize": 0.07,
        "subsamplingRate": 0.8,
        "minInstancesPerNode": 500,
        "maxBins": 128,
        "seed": 42,
    }
    assert metrics["gbt_params"] == expected_champion

    colab_cfg = yaml.safe_load((ROOT / "config" / "pipeline_config_colab.yaml").read_text(encoding="utf-8"))
    local_cfg = yaml.safe_load((ROOT / "config" / "pipeline_config.yaml").read_text(encoding="utf-8"))
    assert len(colab_cfg["airports"]) == 40
    assert len(local_cfg["airports"]) == 40
    assert colab_cfg["spark"]["driver_memory"] == "24g"

    colab_t = colab_cfg["training"]
    assert colab_t["rf_num_trees"] == 100
    assert {
        "maxIter": colab_t["gbt_max_iter"],
        "maxDepth": colab_t["gbt_max_depth"],
        "stepSize": colab_t["gbt_step_size"],
        "subsamplingRate": colab_t["gbt_subsampling_rate"],
        "minInstancesPerNode": colab_t["gbt_min_instances_per_node"],
        "maxBins": colab_t["gbt_max_bins"],
        "seed": colab_t["seed"],
    } == metrics["gbt_params"]

    local_t = local_cfg["training"]
    assert local_t["gbt_max_iter"] == 50
    assert local_t["gbt_max_depth"] == 5
    assert local_t["rf_num_trees"] == 100
    assert local_t["seed"] == 42


def test_champion_40_hub_model_artifacts_and_tuning_trail_parity():
    metrics = json.loads((ROOT / "models" / "training_metrics.json").read_text(encoding="utf-8"))
    assert metrics["hub_count"] == 40
    assert metrics["gbt_champion_iter"] == "iter4_depth8_capacity_probe"
    assert metrics["gbt_mae"] < metrics["gbt_train_mae"] + 0.35

    err_hub = pd.read_csv(ROOT / "models" / "error_by_hub.csv")
    assert len(HUBS) == 40
    assert set(err_hub["hub"]) == set(HUBS)

    tuning = pd.read_csv(ROOT / "models" / "tuning_results.csv")
    assert list(tuning["iteration"]) == [
        "iter0_baseline_default",
        "iter1_fine_bins_128",
        "iter2_depth6_leaf200",
        "iter3_depth7_stochastic",
        "iter4_depth8_capacity_probe",
        "iter5_adaptive_refinement",
    ]
    accepted = tuning[tuning["accepted"].astype(bool)]
    assert accepted.iloc[-1]["iteration"] == "iter4_depth8_capacity_probe"


def test_live_radar_dynamic_metrics_and_optional_tqdm_contracts():
    from live.aeroapi_predictor import evaluate_live_vs_historical

    metrics = json.loads((ROOT / "models" / "training_metrics.json").read_text(encoding="utf-8"))
    empty_ev = evaluate_live_vs_historical(pd.DataFrame())
    assert empty_ev["hist_val_count"] == int(metrics["val_count"])
    assert empty_ev["hist_gbt_mae"] == pytest.approx(float(metrics["gbt_mae"]))

    enroute_only = pd.DataFrame([{"status_category": "enroute", "actual_delta_delay": None, "DepDelay": 5.0}])
    enroute_ev = evaluate_live_vs_historical(enroute_only)
    assert enroute_ev["hist_val_count"] == int(metrics["val_count"])
    assert enroute_ev["hist_gbt_mae"] == pytest.approx(float(metrics["gbt_mae"]))

    radar_src = (ROOT / "dashboard" / "live_radar.py").read_text(encoding="utf-8")
    assert "1,665,362" not in radar_src
    assert "10.53 min" not in radar_src

    eval_src = (ROOT / "live" / "eval_live_vs_historical.py").read_text(encoding="utf-8")
    assert "1665362" not in eval_src

    poster_src = (ROOT / "docs" / "poster" / "build_figures.py").read_text(encoding="utf-8")
    assert "24 hubs" not in poster_src
    assert "1.67M flights" not in poster_src

    eda_src = (ROOT / "scripts" / "run_eda.py").read_text(encoding="utf-8")
    assert "from config.hubs import HUB_REGISTRY" in eda_src


def test_batch_train_gbt_tuning_overfit_and_cache_contracts():
    src = (ROOT / "pipeline" / "01_batch_train.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    assert "RegressionEvaluator" not in src

    resolve_fn_node = None
    cli_flags = set()
    metrics_keys = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "resolve_gbt_params":
            resolve_fn_node = node
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "add_argument"
            and node.args
            and isinstance(node.args[0], ast.Constant)
        ):
            cli_flags.add(node.args[0].value)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "metrics" and isinstance(node.value, ast.Dict):
                    metrics_keys.update(
                        k.value for k in node.value.keys if isinstance(k, ast.Constant)
                    )

    assert resolve_fn_node is not None, "resolve_gbt_params missing from 01_batch_train.py"

    mod_ast = ast.Module(body=[resolve_fn_node], type_ignores=[])
    ast.fix_missing_locations(mod_ast)
    ns: dict = {}
    exec(compile(mod_ast, "01_batch_train.py", "exec"), ns)
    resolve_gbt_params = ns["resolve_gbt_params"]

    empty_args = argparse.Namespace()
    defaults = resolve_gbt_params({"gbt_max_iter": 50, "gbt_max_depth": 5}, empty_args)
    assert defaults == {
        "maxIter": 50,
        "maxDepth": 5,
        "stepSize": 0.1,
        "subsamplingRate": 1.0,
        "minInstancesPerNode": 1,
        "maxBins": 32,
        "seed": 42,
    }

    override_args = argparse.Namespace(
        gbt_max_iter=80,
        gbt_max_depth=6,
        gbt_step_size=0.05,
        gbt_subsampling_rate=0.8,
        gbt_min_instances=50,
        gbt_max_bins=64,
    )
    overridden = resolve_gbt_params(
        {"gbt_max_iter": 50, "gbt_max_depth": 5, "gbt_step_size": 0.1, "seed": 7},
        override_args,
    )
    assert overridden == {
        "maxIter": 80,
        "maxDepth": 6,
        "stepSize": 0.05,
        "subsamplingRate": 0.8,
        "minInstancesPerNode": 50,
        "maxBins": 64,
        "seed": 7,
    }

    expected_flags = {
        "--skip-ablation",
        "--gbt-max-iter",
        "--gbt-max-depth",
        "--gbt-step-size",
        "--gbt-subsampling-rate",
        "--gbt-min-instances",
        "--gbt-max-bins",
        "--rf-num-trees",
        "--rf-max-depth",
        "--cache-dir",
        "--metrics-out",
        "--no-save-model",
    }
    assert expected_flags.issubset(cli_flags)

    expected_metrics_keys = {
        "hub_count",
        "train_count",
        "val_count",
        "gbt_params",
        "gbt_train_mae",
        "gbt_train_rmse",
        "gbt_mae",
        "gbt_rmse",
        "gbt_overfit_gap_mae",
        "gbt_overfit_gap_rmse",
        "gbt_feature_importances",
        "rf_train_mae",
        "rf_train_rmse",
        "rf_mae",
        "rf_rmse",
        "train_years",
        "val_year",
        "total_elapsed_sec",
    }
    assert expected_metrics_keys.issubset(metrics_keys)


def test_error_analysis_forwards_driver_memory_and_shuffle_partitions():
    src = (ROOT / "pipeline" / "04_error_analysis.py").read_text(encoding="utf-8")
    tree = ast.parse(src)

    cli_flags = set()
    build_spark_kwargs = set()

    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "add_argument"
            and node.args
            and isinstance(node.args[0], ast.Constant)
        ):
            cli_flags.add(node.args[0].value)
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "build_spark"
        ):
            build_spark_kwargs.update(kw.arg for kw in node.keywords if kw.arg is not None)

    assert "--shuffle-partitions" in cli_flags
    assert {"cores", "shuffle_partitions", "driver_memory"}.issubset(build_spark_kwargs)


@pytest.fixture(scope="module")
def tune_gbt():
    spec = importlib.util.spec_from_file_location("tune_gbt", ROOT / "scripts" / "tune_gbt.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_tune_gbt_should_accept_all_four_branches(tune_gbt):
    assert tune_gbt.should_accept(
        cand_val_mae=10.40,
        cand_val_rmse=14.70,
        excess_gap_mae=0.151,
        excess_gap_rmse=0.10,
        best_val_mae=10.50,
        best_val_rmse=14.80,
    ) == (False, "rejected_overfit")

    assert tune_gbt.should_accept(
        cand_val_mae=10.40,
        cand_val_rmse=14.70,
        excess_gap_mae=0.05,
        excess_gap_rmse=0.351,
        best_val_mae=10.50,
        best_val_rmse=14.80,
    ) == (False, "rejected_overfit")

    assert tune_gbt.should_accept(
        cand_val_mae=10.45,
        cand_val_rmse=14.815,
        excess_gap_mae=0.05,
        excess_gap_rmse=0.10,
        best_val_mae=10.50,
        best_val_rmse=14.80,
    ) == (False, "rejected_rmse_regression")

    assert tune_gbt.should_accept(
        cand_val_mae=10.499,
        cand_val_rmse=14.79,
        excess_gap_mae=0.05,
        excess_gap_rmse=0.10,
        best_val_mae=10.50,
        best_val_rmse=14.80,
    ) == (False, "rejected_no_mae_gain")

    assert tune_gbt.should_accept(
        cand_val_mae=10.48,
        cand_val_rmse=14.805,
        excess_gap_mae=0.08,
        excess_gap_rmse=0.15,
        best_val_mae=10.50,
        best_val_rmse=14.80,
    ) == (True, "accepted_new_champion")


def test_tune_gbt_contracts_schedule_shift_and_metrics_preservation(tune_gbt, tmp_path):
    src = (ROOT / "scripts" / "tune_gbt.py").read_text(encoding="utf-8")
    tree = ast.parse(src)

    top_imports = set()
    for node in tree.body:
        if isinstance(node, ast.Import):
            top_imports.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            top_imports.add(node.module.split(".")[0])
    assert "pyspark" not in top_imports

    parsed = tune_gbt.parse_args(["--cache-dir", "/tmp/cache", "--baseline-metrics", "/tmp/m0.json"])
    assert parsed.config is None
    assert parsed.cache_dir == "/tmp/cache"
    assert parsed.baseline_metrics == "/tmp/m0.json"
    assert parsed.driver_memory == "28g"
    assert parsed.shuffle_partitions == 64

    names = [name for name, _ in tune_gbt.BASE_CANDIDATES]
    assert names == [
        "iter1_fine_bins_128",
        "iter2_depth6_leaf200",
        "iter3_depth7_stochastic",
        "iter4_depth8_capacity_probe",
    ]
    assert tune_gbt.resolve_iter5_params(True, 8) == {
        "maxDepth": 8,
        "maxIter": 115,
        "stepSize": 0.055,
        "maxBins": 128,
        "minInstancesPerNode": 600,
        "subsamplingRate": 0.8,
    }
    assert tune_gbt.resolve_iter5_params(False, 5) == {
        "maxDepth": 6,
        "maxIter": 110,
        "stepSize": 0.06,
        "maxBins": 128,
        "minInstancesPerNode": 350,
        "subsamplingRate": 0.8,
    }

    assert tune_gbt.load_baseline_shift(tmp_path / "missing.csv") == pytest.approx((0.288, 0.507))
    ablation_csv = tmp_path / "ablation.csv"
    ablation_csv.write_text(
        "model,train_mae,train_rmse,val_mae,val_rmse\n"
        "baseline_lane_mean,10.5000,14.8000,10.8125,15.3250\n",
        encoding="utf-8",
    )
    assert tune_gbt.load_baseline_shift(ablation_csv) == pytest.approx((0.3125, 0.5250))

    base = {
        "hub_count": 40,
        "train_count": 12000000,
        "val_count": 2500000,
        "b1_mae": 12.7,
        "b1_rmse": 16.6,
        "b2_mae": 10.8,
        "b2_rmse": 15.3,
        "rf_mae": 10.67,
        "rf_rmse": 15.16,
        "train_years": [2018, 2019, 2021, 2022, 2023],
        "val_year": 2024,
    }
    promoted = tune_gbt.build_champion_metrics(
        base,
        iter_name="iter3_depth7_stochastic",
        params={"maxDepth": 7, "maxIter": 85},
        seed=42,
        train_mae=10.10,
        train_rmse=14.20,
        val_mae=10.42,
        val_rmse=14.72,
        excess_gap_mae=0.032,
        feature_importances={"DepDelay": 0.25},
    )
    assert promoted["rf_mae"] == 10.67
    assert promoted["b2_mae"] == 10.8
    assert promoted["train_count"] == 12000000
    assert promoted["gbt_champion_iter"] == "iter3_depth7_stochastic"
    assert promoted["gbt_mae"] == 10.42
    assert promoted["gbt_overfit_gap_mae"] == pytest.approx(0.32)
    assert promoted["gbt_excess_overfit_gap_mae"] == pytest.approx(0.032)
    assert promoted["gbt_params"] == {"maxDepth": 7, "maxIter": 85, "seed": 42}


