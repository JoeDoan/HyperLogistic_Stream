"""04_error_analysis.py aggregation logic on in-memory data. Needs a real Spark."""
import importlib.util
import os
import sys
from pathlib import Path

import pandas as pd
import pytest

pytest.importorskip("pyspark")

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def spark():
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    from pyspark.sql import SparkSession
    try:
        s = (SparkSession.builder.master("local[1]").appName("err-test")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "1").getOrCreate())
    except Exception as exc:
        pytest.skip(f"Spark unavailable: {exc}")
    yield s
    s.stop()


@pytest.fixture(scope="module")
def ea():
    spec = importlib.util.spec_from_file_location("error_analysis", ROOT / "pipeline" / "04_error_analysis.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def scored(spark):
    return spark.createDataFrame(pd.DataFrame({
        "Origin": ["ATL", "ATL", "ORD", "ORD"],
        "delta_delay": [0.0, 20.0, -10.0, 4.0],
        "prediction": [1.0, 10.0, -10.0, 6.0],
        "LaneMeanDelay": [2.0, 2.0, 0.0, 0.0],
    }))


def test_error_table_metrics(ea, spark):
    out = ea.error_table(scored(spark), ["Origin"]).toPandas().set_index("Origin")
    assert out.loc["ATL", "n"] == 2
    assert out.loc["ATL", "mae_gbt"] == pytest.approx((1 + 10) / 2)
    assert out.loc["ATL", "mae_lane_mean"] == pytest.approx((2 + 18) / 2)
    assert out.loc["ATL", "mae_no_change"] == pytest.approx(10.0)
    assert out.loc["ATL", "bias_gbt"] == pytest.approx((1 - 10) / 2)  # under-predicts the late flight
    assert out.loc["ORD", "mae_gbt"] == pytest.approx(1.0)
    assert out.loc["ORD", "mean_actual"] == pytest.approx(-3.0)


def test_buckets(ea, spark):
    df = spark.createDataFrame(pd.DataFrame({"delta_delay": [-20.0, -10.0, 0.0, 10.0, 20.0, 5.0, -5.0]}))
    got = df.withColumn("b", ea.bucket_column()).toPandas()
    labels = dict(zip(got["delta_delay"], got["b"]))
    assert labels[-20.0] == "early >15 min"
    assert labels[-10.0] == "early 5-15 min"
    assert labels[0.0] == "within 5 min"
    assert labels[5.0] == "late 5-15 min"   # lower edge inclusive
    assert labels[-5.0] == "within 5 min"
    assert labels[20.0] == "late >15 min"
