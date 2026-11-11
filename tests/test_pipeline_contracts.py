"""Pure-Python train/serve parity, model-loading guard, and Spark UTC contracts."""
import argparse
import ast
from datetime import datetime
import importlib.util
from pathlib import Path

import pytest
import yaml

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
    cfg = yaml.safe_load((ROOT / "config" / "pipeline_config_colab.yaml").read_text(encoding="utf-8"))
    assert cfg["spark"]["driver_memory"] == "24g"
    assert len(cfg["airports"]) == 40
    training = cfg["training"]
    assert training["gbt_max_iter"] == 50
    assert training["gbt_max_depth"] == 5
    assert training["gbt_step_size"] == 0.1
    assert training["gbt_subsampling_rate"] == 1.0
    assert training["gbt_min_instances_per_node"] == 1
    assert training["gbt_max_bins"] == 32
    assert training["seed"] == 42


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

