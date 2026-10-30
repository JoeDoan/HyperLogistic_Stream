"""Model Insights helpers: feature groups, per-hub gain and the error sentence."""
import pandas as pd
import pytest


def test_feature_groups_sum_to_total(dash):
    imps = {"DepDelay": 0.1, "DepHourUTC": 0.2, "ArrHourUTC": 0.1, "LaneMeanDelay": 0.2,
            "DestWindSpeed": 0.2, "OriginTemp": 0.2}
    grp = dash.feature_group_importance(imps).set_index("group")["importance"]
    assert grp["Schedule & time"] == pytest.approx(0.3)
    assert grp["Destination weather"] == pytest.approx(0.2)
    assert grp["Origin weather"] == pytest.approx(0.2)
    assert grp.sum() == pytest.approx(1.0)


def test_feature_groups_unknown_feature_goes_to_other(dash):
    grp = dash.feature_group_importance({"Mystery": 0.5}).set_index("group")["importance"]
    assert grp["Other"] == 0.5


def test_real_training_metrics_features_are_all_grouped(dash):
    import json
    m = json.loads((dash.MODELS_DIR / "training_metrics.json").read_text())
    grp = dash.feature_group_importance(m["gbt_feature_importances"])
    assert "Other" not in set(grp["group"])


def test_hub_error_gain(dash):
    hub = pd.DataFrame({"hub": ["ATL", "ORD"], "mae_gbt": [9.0, 12.0],
                        "mae_lane_mean": [10.0, 12.0], "mae_no_change": [11.0, 14.0]})
    out = dash.hub_error_gain(hub).set_index("hub")
    assert out.loc["ATL", "gain_pct"] == pytest.approx(10.0)
    assert out.loc["ORD", "gain_min"] == 0
    assert list(dash.hub_error_gain(hub)["hub"]) == ["ORD", "ATL"]  # worst first


def test_error_so_what_names_worst_hub_and_tail_bias(dash):
    hub = pd.DataFrame({"hub": ["ATL", "ORD"], "mae_gbt": [9.0, 12.0]})
    bucket = pd.DataFrame({"bucket": ["late >15 min", "within 5 min"], "bias_gbt": [-20.0, 1.0]})
    s = dash.error_so_what(hub, bucket)
    assert "ORD" in s and "12.0" in s and "ATL" in s
    assert "under-predicted by 20.0 min" in s


def test_error_so_what_empty(dash):
    assert dash.error_so_what(pd.DataFrame(), pd.DataFrame()) == "No error tables yet."
