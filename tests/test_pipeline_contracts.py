"""Pure-Python train/serve parity, model-loading guard, and Spark UTC contracts."""
import ast
from datetime import datetime
import importlib.util
from pathlib import Path

import pytest

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
