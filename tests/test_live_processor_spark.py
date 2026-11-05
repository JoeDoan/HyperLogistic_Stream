"""04_live_processor.py hub traffic snapshot isolation and zero-fill tests. Needs a real Spark."""
import importlib.util
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

pytest.importorskip("pyspark")

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def spark():
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    from pyspark.sql import SparkSession
    if isinstance(SparkSession, MagicMock):
        pytest.skip("pyspark is stubbed")
    try:
        s = (
            SparkSession.builder.master("local[1]")
            .appName("live-processor-test")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
    except Exception as exc:
        pytest.skip(f"Spark unavailable: {exc}")
    yield s
    s.stop()


@pytest.fixture(scope="module")
def lp(spark):
    spec = importlib.util.spec_from_file_location("live_processor", ROOT / "pipeline" / "04_live_processor.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _flight_row(
    icao24: str,
    lat: float,
    lon: float,
    fetched_at: str,
    emit_epoch_ms: int = 1000,
    baro_altitude: float = 2500.0,
    vertical_rate: float = -2.0,
    on_ground: bool = False,
) -> dict:
    return {
        "icao24": icao24,
        "callsign": icao24.upper(),
        "lat": float(lat),
        "lon": float(lon),
        "baro_altitude": float(baro_altitude),
        "on_ground": bool(on_ground),
        "velocity_ms": 210.0,
        "vertical_rate": float(vertical_rate),
        "fetched_at": fetched_at,
        "stale": False,
        "stale_age_s": 0,
        "emit_epoch_ms": int(emit_epoch_ms),
    }


def _hubs_df(spark, lp, codes=("ATL", "SLC", "ORD")):
    rows = [(c, float(lp.HUBS[c][0]), float(lp.HUBS[c][1])) for c in codes]
    return spark.createDataFrame(rows, ["airport", "hub_lat", "hub_lon"])


def test_latest_snapshot_selected_and_zero_aircraft_hubs_emitted(lp, spark):
    atl_lat, atl_lon = lp.HUBS["ATL"]
    slc_lat, slc_lon = lp.HUBS["SLC"]
    t1 = "2026-10-30T21:00:00+00:00"
    t2 = "2026-10-30T21:02:00+00:00"

    rows = [
        _flight_row("a1", atl_lat + 0.05, atl_lon, t1, emit_epoch_ms=1000),
        _flight_row("a2", atl_lat - 0.05, atl_lon, t1, emit_epoch_ms=1000),
        _flight_row("a3", atl_lat, atl_lon + 0.05, t1, emit_epoch_ms=1000),
        _flight_row("s1", slc_lat + 0.02, slc_lon, t1, emit_epoch_ms=1000),
        _flight_row("s2", slc_lat - 0.02, slc_lon, t1, emit_epoch_ms=1000),
        _flight_row("a1", atl_lat + 0.03, atl_lon, t2, emit_epoch_ms=2000, baro_altitude=2200.0, vertical_rate=-3.0),
    ]
    df = spark.createDataFrame(rows, schema=lp.FLIGHT_SCHEMA)
    out = lp.transform_traffic(df, _hubs_df(spark, lp), 100.0).toPandas().set_index("airport")

    assert set(out.index) == {"ATL", "SLC", "ORD"}
    assert out.loc["ATL", "aircraft_nearby"] == 1
    assert out.loc["ATL", "low_altitude_count"] == 1
    assert out.loc["ATL", "descending_count"] == 1
    assert out.loc["ATL", "fetched_at"] == t2
    assert out.loc["ATL", "emit_epoch_ms"] == 2000

    assert out.loc["SLC", "aircraft_nearby"] == 0
    assert out.loc["SLC", "low_altitude_count"] == 0
    assert out.loc["SLC", "descending_count"] == 0
    assert pd.isna(out.loc["SLC", "avg_altitude_m"])
    assert out.loc["SLC", "fetched_at"] == t2
    assert out.loc["SLC", "emit_epoch_ms"] == 2000


def test_duplicate_icao24_in_latest_snapshot_counted_once(lp, spark):
    atl_lat, atl_lon = lp.HUBS["ATL"]
    t2 = "2026-10-30T21:02:00+00:00"
    rows = [
        _flight_row("dup01", atl_lat + 0.01, atl_lon, t2, emit_epoch_ms=2000),
        _flight_row("dup01", atl_lat + 0.01, atl_lon, t2, emit_epoch_ms=2000),
        _flight_row("uniq2", atl_lat - 0.01, atl_lon, t2, emit_epoch_ms=2000),
    ]
    df = spark.createDataFrame(rows, schema=lp.FLIGHT_SCHEMA)
    out = lp.transform_traffic(df, _hubs_df(spark, lp, codes=("ATL", "SLC")), 100.0).toPandas().set_index("airport")

    assert out.loc["ATL", "aircraft_nearby"] == 2
    assert out.loc["SLC", "aircraft_nearby"] == 0


def test_empty_batch_emits_zero_rows(lp, spark):
    empty_df = spark.createDataFrame([], schema=lp.FLIGHT_SCHEMA)
    out = lp.transform_traffic(empty_df, _hubs_df(spark, lp, codes=("ATL", "SLC")), 100.0)
    assert out.count() == 0
