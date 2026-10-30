"""Config files stay consistent with the model feature sets."""
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
CONFIGS = ["pipeline_config.yaml", "pipeline_config_colab.yaml"]
WEATHER = ["DestWindSpeed", "DestVisibility", "DestTemp", "DestCeiling",
           "OriginWindSpeed", "OriginVisibility", "OriginCeiling", "OriginTemp"]


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
