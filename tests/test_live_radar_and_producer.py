"""Pure-Python unit tests for Live Radar feed preparation, HTTP 429 handling, producer sink isolation, and dashboard helpers."""
import email.utils
import io
import json
import sys
import time
import urllib.error
from datetime import datetime, timedelta, timezone
from email.message import Message
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
for _p in (ROOT / "live", ROOT / "dashboard"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

import _common  # noqa: E402
import live_producer  # noqa: E402
import live_radar  # noqa: E402


MINI_HUBS = {
    "LAS": (36.0840, -115.1537, "Las Vegas"),
    "ATL": (33.6407, -84.4277, "Atlanta Hartsfield"),
}


def _http_429(url: str, retry_after: str | None = None) -> urllib.error.HTTPError:
    hdrs = Message()
    if retry_after is not None:
        hdrs["Retry-After"] = retry_after
    return urllib.error.HTTPError(url, 429, "Too Many Requests", hdrs, io.BytesIO(b"rate limited"))


def test_recorded_mode_preserves_three_day_old_faa_ground_stop():
    now_utc = datetime(2026, 10, 30, 22, 0, 0, tzinfo=timezone.utc)
    rec_ts = (now_utc - timedelta(days=3)).isoformat()
    wx_ts = (now_utc - timedelta(days=3) + timedelta(seconds=8)).isoformat()

    faa = pd.DataFrame([{
        "airport": "LAS",
        "delay_type": "Ground Stop Programs",
        "reason": "THUNDERSTORMS",
        "fetched_at": rec_ts,
        "stale": False,
    }])
    weather = pd.DataFrame([{
        "airport": "LAS",
        "visibility_m": 16093.0,
        "ceiling_m": 3000.0,
        "wind_kmh": 15.0,
        "wind_gust_kmh": None,
        "fetched_at": wx_ts,
        "stale": False,
    }])

    out = live_radar.prepare_live_feeds(
        weather=weather,
        faa=faa,
        traffic=pd.DataFrame(),
        source="Recorded live session",
        now_utc=now_utc,
    )
    assert out["is_recorded"] is True
    assert len(out["faa"]) == 1

    hubs = live_radar.build_hub_state(out, MINI_HUBS).set_index("airport")
    assert hubs.loc["LAS", "faa_severity"] == 3
    assert "Ground Stop Programs" in hubs.loc["LAS", "faa_programs"]
    assert "[stale]" not in hubs.loc["LAS", "faa_programs"]


def test_live_mode_preserves_stale_faa_alert_beyond_max_age_and_marks_stale():
    now_utc = datetime(2026, 10, 30, 22, 0, 0, tzinfo=timezone.utc)
    old_ts = (now_utc - timedelta(seconds=900)).isoformat()
    fresh_ts = (now_utc - timedelta(seconds=10)).isoformat()

    faa = pd.DataFrame([{
        "airport": "LAS",
        "delay_type": "Ground Stop Programs",
        "reason": "WX",
        "fetched_at": old_ts,
        "stale": True,
    }])
    weather = pd.DataFrame([{
        "airport": "LAS",
        "visibility_m": 16093.0,
        "ceiling_m": 4000.0,
        "wind_kmh": 10.0,
        "wind_gust_kmh": None,
        "fetched_at": fresh_ts,
        "stale": False,
    }])

    out = live_radar.prepare_live_feeds(
        weather=weather,
        faa=faa,
        traffic=pd.DataFrame(),
        source="Spark Gold (live)",
        now_utc=now_utc,
    )
    assert out["feed_stale"]["faa"] is True
    assert out["feed_stale"]["weather"] is False
    assert len(out["faa"]) == 1

    hubs = live_radar.build_hub_state(out, MINI_HUBS)
    by_code = hubs.set_index("airport")
    assert by_code.loc["LAS", "faa_severity"] == 3
    assert by_code.loc["LAS", "faa_programs"].endswith(" [stale]")

    label, color, subtitle = live_radar.compute_status_badge(hubs, out, now_utc=now_utc)
    assert label == "STALE"
    assert color == "#f97316"
    assert "stale feeds: faa" in subtitle


def test_live_mode_drops_unflagged_expired_faa_rows_and_flags_feed_stale():
    now_utc = datetime(2026, 10, 30, 22, 0, 0, tzinfo=timezone.utc)
    expired_ts = (now_utc - timedelta(seconds=900)).isoformat()

    faa = pd.DataFrame([{
        "airport": "LAS",
        "delay_type": "Ground Stop Programs",
        "reason": "WX",
        "fetched_at": expired_ts,
        "stale": False,
    }])
    out = live_radar.prepare_live_feeds(
        weather=pd.DataFrame(),
        faa=faa,
        traffic=pd.DataFrame(),
        source="Spark Gold (live)",
        now_utc=now_utc,
    )
    assert out["faa"].empty
    assert out["feed_stale"]["faa"] is True


def test_recording_fallback_prefers_live_recordings_then_sample_dir(tmp_path):
    rec_dir = tmp_path / "live_recordings"
    sample_dir = tmp_path / "sample"
    sample_dir.mkdir()
    sample_file = sample_dir / "live_session_20261030T213140Z.jsonl"
    sample_file.write_text(
        json.dumps({"topic": "airport_status", "value": {"airport": "LAS", "delay_type": "Ground Stop Programs"}})
        + "\n",
        encoding="utf-8",
    )

    buckets, label = live_radar._read_latest_recording(record_dir=rec_dir, sample_dir=sample_dir)
    assert label == "Sample recorded session"
    assert list(buckets["airport_status"]["airport"]) == ["LAS"]

    badge_label, badge_color, _ = live_radar.compute_status_badge(pd.DataFrame(), {"source": label})
    assert badge_label == "SAMPLE"
    assert badge_color == "#6366f1"

    rec_dir.mkdir()
    rec_file = rec_dir / "live_session_20261030T220000Z.jsonl"
    rec_file.write_text(
        json.dumps({"topic": "airport_status", "value": {"airport": "ATL", "delay_type": "Ground Delay Programs"}})
        + "\n",
        encoding="utf-8",
    )
    buckets2, label2 = live_radar._read_latest_recording(record_dir=rec_dir, sample_dir=sample_dir)
    assert label2 == "Recorded live session"
    assert list(buckets2["airport_status"]["airport"]) == ["ATL"]


def test_parse_retry_after_numeric_http_date_and_bounds():
    assert _common.parse_retry_after("120") == pytest.approx(120.0)
    assert _common.parse_retry_after("0") == pytest.approx(1.0)
    assert _common.parse_retry_after("-10") == pytest.approx(1.0)
    assert _common.parse_retry_after(None, default_s=75.0) == pytest.approx(75.0)
    assert _common.parse_retry_after("not-a-date", default_s=45.0) == pytest.approx(45.0)

    future_dt = datetime.now(timezone.utc) + timedelta(seconds=90)
    http_date = email.utils.format_datetime(future_dt, usegmt=True)
    parsed = _common.parse_retry_after(http_date)
    assert 80.0 <= parsed <= 95.0


def test_http_get_raises_rate_limit_error_immediately_on_long_429():
    url = "https://opensky-network.org/api/states/all"
    err = _http_429(url, retry_after="180")
    with patch("urllib.request.urlopen", side_effect=err) as m_open, patch("time.sleep") as m_sleep:
        with pytest.raises(_common.RateLimitError) as exc_info:
            _common.http_get(url, retries=2)
    assert exc_info.value.retry_after_s == pytest.approx(180.0)
    assert exc_info.value.url == url
    assert m_open.call_count == 1
    m_sleep.assert_not_called()


def test_http_get_retries_short_429_then_succeeds():
    url = "https://api.weather.gov/stations/KATL/observations/latest"

    class _OkResp:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return b'{"ok": true}'

    with patch("urllib.request.urlopen", side_effect=[_http_429(url, retry_after="4"), _OkResp()]) as m_open, patch(
        "time.sleep"
    ) as m_sleep:
        body = _common.http_get(url, retries=2)
    assert body == b'{"ok": true}'
    assert m_open.call_count == 2
    m_sleep.assert_called_once_with(4.0)


def test_source_poll_once_isolates_sink_failure_on_normal_and_failed_fetch():
    class _BrokenSink:
        def send(self, topic, rows, key_field):
            raise RuntimeError("Kafka broker down")

    src_ok = live_producer.Source(
        "faa",
        "airport_status",
        60,
        lambda: [{"airport": "ATL", "delay_type": "Ground Stop Programs"}],
        "airport",
    )
    src_ok.poll_once(_BrokenSink())
    assert src_ok.failures == 0
    assert src_ok.sink_failures == 1
    assert src_ok.sent == 0
    assert len(src_ok.last_rows) == 1

    def _fail_fetch():
        raise RuntimeError("FAA endpoint timeout")

    src_fail = live_producer.Source("faa", "airport_status", 60, _fail_fetch, "airport", republish_stale=True)
    src_fail.last_rows = [{"airport": "ATL", "delay_type": "Ground Stop Programs"}]
    src_fail.last_ok_epoch = time.time() - 65
    src_fail.poll_once(_BrokenSink())
    assert src_fail.failures == 1
    assert src_fail.sink_failures == 1
    assert src_fail.sent == 0


def test_source_poll_once_sets_cooldown_on_rate_limit_error():
    class _MemorySink:
        def __init__(self):
            self.batches = []

        def send(self, topic, rows, key_field):
            self.batches.append((topic, rows, key_field))

    def _rate_limited():
        raise _common.RateLimitError("HTTP 429", retry_after_s=180.0, url="https://opensky-network.org/api/states/all")

    src = live_producer.Source("opensky", "live_flights", 120, _rate_limited, "icao24", republish_stale=False)
    sink = _MemorySink()
    before = time.monotonic()
    src.poll_once(sink)
    assert src.failures == 1
    assert src.sink_failures == 0
    assert src.cooldown_until >= before + 179.0
    assert sink.batches == []


def test_risk_threshold_labels_and_summary_csv_benchmark_aggregation(dash):
    assert dash.format_risk_metric_label(30.0) == "Elevated risk (Δ>30m)"
    assert dash.format_risk_metric_label(5.0) == "Elevated risk (Δ>5m)"
    assert dash.format_risk_chart_title(30.0) == "Elevated-Risk Arrivals (Δ > 30 min)"
    assert dash.get_high_risk_threshold({"streaming": {"high_risk_threshold_min": 30.0}}) == pytest.approx(30.0)

    summary_df = pd.DataFrame([{
        "config": "c4_sp8_r5000_mo20000",
        "throughput_rps": 4448.0,
        "duration_ms": 3481.0,
        "p95_batch_ms": 3923.0,
        "p50_latency_ms": float("nan"),
        "p95_latency_ms": 8967.0,
    }])
    agg = dash.aggregate_benchmark(summary_df)
    assert agg.loc[0, "p95_batch_ms"] == pytest.approx(3923.0)
    assert agg.loc[0, "p50_batch_ms"] == pytest.approx(3481.0)


def test_bundled_sample_recording_loads_with_active_faa_programs(dash, tmp_path):
    if not list(live_radar.SAMPLE_DIR.glob("live_session_*.jsonl")):
        pytest.skip("No sample recording bundled in data/sample")
    rec, label = live_radar._read_latest_recording(record_dir=tmp_path / "no_live_recordings")
    assert label == "Sample recorded session"

    flights = live_radar._latest_snapshot(rec.get("live_flights", pd.DataFrame()))
    traffic = live_radar._traffic_from_flights(flights, dash.HUB_COORDS)
    future_now = datetime(2027, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
    prepared = live_radar.prepare_live_feeds(
        weather=rec.get("weather", pd.DataFrame()),
        faa=rec.get("airport_status", pd.DataFrame()),
        traffic=traffic,
        source=label,
        now_utc=future_now,
    )
    hubs = live_radar.build_hub_state(prepared, dash.HUB_COORDS).set_index("airport")
    assert hubs.loc["LAS", "faa_severity"] == 3
    assert hubs.loc["PHX", "faa_severity"] == 2


def test_canonical_benchmark_summary_and_config_forwarding(dash, tmp_path):
    loaded = dash.read_benchmark_results()
    agg = dash.aggregate_benchmark(loaded)
    assert len(agg) == 12
    assert agg["avg_throughput"].max() == pytest.approx(4448.0)

    from benchmark import run_benchmark as rb

    with patch.object(rb.subprocess, "Popen") as mock_popen:
        rb.start_producer("localhost:9092", "flights_stream", 5000, config_path="config/pipeline_config_colab.yaml")
        prod_cmd = mock_popen.call_args[0][0]
        assert prod_cmd[-2:] == ["--config", "config/pipeline_config_colab.yaml"]

    cfg = {"kafka": {"bootstrap_servers": "localhost:9092"}, "paths": {"benchmark_results": str(tmp_path)}}
    with patch.object(rb.subprocess, "run") as mock_run:
        rb.run_streaming_job(cfg, 4, 8, 20000, "c4_sp8_r5000_mo20000", config_path="config/pipeline_config_colab.yaml")
        stream_cmd = mock_run.call_args[0][0]
        assert stream_cmd[-2:] == ["--config", "config/pipeline_config_colab.yaml"]

