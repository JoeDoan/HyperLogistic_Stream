"""The committed live-vs-historical comparison must stay consistent and honestly labelled."""
import json
from pathlib import Path

import pandas as pd

MODELS = Path(__file__).resolve().parent.parent / "models"


def test_live_vs_historical_table_is_consistent():
    t = pd.read_csv(MODELS / "live_vs_historical.csv")
    assert len(t) == 3
    no_change, hub_mean, estimator = t.iloc[0], t.iloc[1], t.iloc[2]
    # both history-based rows beat "no change" on live flights and on the 2024 validation year
    assert hub_mean["live_mae"] < no_change["live_mae"]
    assert estimator["live_mae"] < no_change["live_mae"]
    assert hub_mean["hist_mae"] < no_change["hist_mae"]
    assert estimator["hist_mae"] < hub_mean["hist_mae"]
    # the live row must never be labelled as the trained GBT
    assert "rule-based" in estimator["model"]


def test_live_vs_historical_summary_states_its_caveats():
    s = json.loads((MODELS / "live_vs_historical.json").read_text(encoding="utf-8"))
    assert s["landed_flights_scored"] > 0
    assert s["hist_validation_flights"] == 1665362
    text = " ".join(s["caveats"]).lower()
    assert "not the trained spark gbt" in text
