"""Config files stay consistent with the model feature sets and 40-hub registry."""
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
import yaml

from config.hubs import (
    AIRPORT_TZ,
    DEFAULT_ICAO_AIRPORTS,
    HUB_COORDS_LATLON,
    HUB_COORDS_NAMED,
    HUB_REGISTRY,
    HUBS,
)
from live import _common as live_common

ROOT = Path(__file__).resolve().parent.parent
CONFIGS = ["pipeline_config.yaml", "pipeline_config_colab.yaml"]
WEATHER = ["DestWindSpeed", "DestVisibility", "DestTemp", "DestCeiling",
           "OriginWindSpeed", "OriginVisibility", "OriginCeiling", "OriginTemp"]
KEY_EXPANDED_HUBS = ("MCI", "STL", "IND", "ONT", "IAD", "DCA", "SAN", "TPA")


@pytest.mark.parametrize("name", CONFIGS)
def test_weather_features_listed(name):
    cfg = yaml.safe_load((ROOT / "config" / name).read_text())
    numeric = cfg["features"]["numeric"]
    for col in WEATHER:
        assert col in numeric, f"{col} missing from {name}"
    assert len(numeric) == len(set(numeric)), "duplicate feature"


@pytest.mark.parametrize("name", CONFIGS)
def test_required_path_keys(name):
    paths = yaml.safe_load((ROOT / "config" / name).read_text())["paths"]
    for key in ("bronze", "silver", "gold_predictions", "gold_congestion",
                "models", "checkpoints", "benchmark_results"):
        assert key in paths


def test_stream_year_not_in_training_years():
    t = yaml.safe_load((ROOT / "config" / "pipeline_config.yaml").read_text())["training"]
    assert t["stream_year"] not in t["train_years"]
    assert t["val_year"] not in t["train_years"]


def test_hub_registry_invariants():
    assert len(HUB_REGISTRY) == 40
    assert len(HUBS) == 40
    assert len(set(DEFAULT_ICAO_AIRPORTS)) == 40
    for code, info in HUB_REGISTRY.items():
        assert info.iata == code
        assert info.icao == f"K{code}"
        ZoneInfo(info.tz)
        assert 24.0 <= info.lat <= 50.0, f"{code} latitude out of continental US bounds"
        assert -125.0 <= info.lon <= -66.0, f"{code} longitude out of continental US bounds"
        assert info.name.strip()


@pytest.mark.parametrize("name", CONFIGS)
def test_yaml_airports_match_hub_registry(name):
    cfg = yaml.safe_load((ROOT / "config" / name).read_text())
    assert cfg["airports"] == AIRPORT_TZ


def test_key_expanded_hubs_in_registry_dashboard_and_live(dash, stream_processor):
    import importlib.util
    import live_radar
    from live import fetch_aeroapi
    from scripts import download_noaa

    for hub in KEY_EXPANDED_HUBS:
        assert hub in HUB_REGISTRY
        assert hub in dash.HUB_COORDS
        assert hub in live_common.HUBS
    assert dash.HUB_COORDS == HUB_COORDS_NAMED
    assert live_radar.AIRPORT_TZ == AIRPORT_TZ
    assert live_common.HUBS == HUBS
    assert fetch_aeroapi.HUB_COORDS == HUB_COORDS_LATLON
    assert download_noaa.DEFAULT_AIRPORTS == DEFAULT_ICAO_AIRPORTS
    assert stream_processor.AIRPORT_TZ == AIRPORT_TZ

    spec = importlib.util.spec_from_file_location(
        "kafka_producer_hub_test", ROOT / "pipeline" / "02_kafka_producer.py"
    )
    kp = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(kp)
    assert kp.AIRPORT_TZ == AIRPORT_TZ
    event = kp.parse_bts_row({
        "FlightDate": "2024-06-15",
        "Reporting_Airline": "WN",
        "Flight_Number_Reporting_Airline": "402",
        "Tail_Number": "N880WN",
        "Origin": "MCI",
        "Dest": "IND",
        "CRSDepTime": "0830",
        "WheelsOff": "0840",
        "CRSArrTime": "1105",
        "DepDelay": "10.0",
        "ArrDelay": "5.0",
        "Distance": "451.0",
        "Cancelled": "0.0",
        "Diverted": "0.0",
    })
    assert event is not None
    assert event["origin"] == "MCI"
    assert event["dest"] == "IND"
    assert event["crs_dep_time_utc"] == "2024-06-15T13:30:00Z"
    assert event["crs_arr_time_utc"] == "2024-06-15T15:05:00Z"


def test_noaa_2024_csv_stations_match_default_icao_airports():
    noaa_2024 = ROOT / "data" / "raw" / "noaa" / "2024"
    if not noaa_2024.exists():
        pytest.skip("data/raw/noaa/2024 not present on disk")
    icao_prefixes = {p.name.split("_", 1)[0] for p in noaa_2024.glob("*.csv")}
    assert icao_prefixes == set(DEFAULT_ICAO_AIRPORTS)
