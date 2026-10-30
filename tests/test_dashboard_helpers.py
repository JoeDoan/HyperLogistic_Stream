"""Story-chart helpers on small synthetic Gold data."""
import numpy as np
import pandas as pd
import pytest


def gold(n_clear=60, n_fog=40):
    clear = pd.DataFrame({
        "flight_id": [f"c{i}" for i in range(n_clear)],
        "origin": "ATL", "dest": "DFW",
        "predicted_delta_delay": 1.0, "is_high_risk": False,
        "DestWindSpeed": 2.0, "DestVisibility": 16000.0, "DestCeiling": 22000.0,
    })
    fog = pd.DataFrame({
        "flight_id": [f"f{i}" for i in range(n_fog)],
        "origin": "ATL", "dest": "DFW",
        "predicted_delta_delay": 6.0, "is_high_risk": True,
        "DestWindSpeed": 2.0, "DestVisibility": 800.0, "DestCeiling": 100.0,
    })
    return pd.concat([clear, fog], ignore_index=True)


def test_weather_table_buckets_visibility(dash):
    tbl = dash.weather_impact_table(gold())
    vis = tbl[tbl["factor"] == "Visibility"].set_index("bucket")
    assert vis.loc["<1 mi", "avg_delta"] == pytest.approx(6.0)
    assert vis.loc[">5 mi", "avg_delta"] == pytest.approx(1.0)
    assert vis.loc["<1 mi", "high_risk_pct"] == pytest.approx(100.0)
    assert vis.loc[">5 mi", "n"] == 60


def test_weather_table_drops_small_buckets(dash):
    tbl = dash.weather_impact_table(gold(n_clear=60, n_fog=5))
    assert "<1 mi" not in set(tbl["bucket"])


def test_weather_table_skips_missing_columns(dash):
    df = gold().drop(columns=["DestVisibility", "DestCeiling"])
    assert set(dash.weather_impact_table(df)["factor"]) == {"Wind"}


def test_so_what_names_worst_bucket(dash):
    msg = dash.weather_so_what(dash.weather_impact_table(gold()))
    assert "5.0 min later" in msg
    assert "<1 mi" in msg or "<500 ft" in msg


def test_so_what_flat_weather(dash):
    df = gold()
    df["predicted_delta_delay"] = 2.0
    assert "barely moves" in dash.weather_so_what(dash.weather_impact_table(df))


def test_so_what_empty(dash):
    empty = pd.DataFrame(columns=["factor", "bucket", "avg_delta", "n"])
    assert "Not enough" in dash.weather_so_what(empty)


def test_lane_matrix_filters_thin_lanes(dash):
    thin = pd.DataFrame({
        "origin": ["ORD"] * 5, "dest": ["DFW"] * 5,
        "predicted_delta_delay": 9.0, "flight_id": list("abcde"), "is_high_risk": False,
    })
    _z, _n, g = dash.lane_matrix(pd.concat([gold(), thin], ignore_index=True))
    assert list(zip(g["origin"], g["dest"])) == [("ATL", "DFW")]
    assert g["n"].iloc[0] == 100


def test_benchmark_headline_uses_measured_numbers(dash):
    agg = pd.DataFrame({
        "config": ["a", "b"], "avg_throughput": [100.0, 300.0],
        "p95_batch_ms": [900.0, 500.0], "p95_latency_ms": [np.nan, 2000.0],
    })
    s = dash.benchmark_headline(agg)
    assert "(b)" in s and "300 records/s" in s and "2,000 ms" in s
    assert "200 records/s" in s  # mean across configs


def test_benchmark_headline_without_latency(dash):
    agg = pd.DataFrame({"config": ["a"], "avg_throughput": [50.0],
                        "p95_batch_ms": [10.0], "p95_latency_ms": [np.nan]})
    assert "end-to-end" not in dash.benchmark_headline(agg)
