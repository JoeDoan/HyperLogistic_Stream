"""
live/eval_live_vs_historical.py
Score landed FlightAware AeroAPI flights from the recorded live sessions and compare the
error with the 2024 historical validation numbers.

Writes (committed, so the poster and EVALUATION.md are reproducible without the raw data):
    models/live_vs_historical.csv   one row per model: live MAE/RMSE, 2024 MAE/RMSE, gain vs "no change"
    models/live_vs_historical.json  sample size, date range and the caveats

Live rows are scored by the rule-based estimator in live/aeroapi_predictor.py, not by the
trained Spark GBT. The 2024 rows come from the real GBT (models/training_metrics.json).

Usage:
    python live/eval_live_vs_historical.py
"""

import glob
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "live"))

from aeroapi_predictor import enrich_and_score_live_flights, evaluate_live_vs_historical  # noqa: E402


def load_recordings(pattern: str):
    flights, weather = [], []
    files = sorted(glob.glob(pattern))
    for f in files:
        with open(f, encoding="utf-8") as fh:
            for line in fh:
                if '"aeroapi_flights"' in line:
                    flights.append(json.loads(line)["value"])
                elif '"topic": "weather"' in line:
                    weather.append(json.loads(line)["value"])
    return files, pd.DataFrame(flights), pd.DataFrame(weather)


def main():
    files, flights, weather = load_recordings(str(ROOT / "data" / "live_recordings" / "*.jsonl"))
    if flights.empty:
        sys.exit("No aeroapi_flights rows in data/live_recordings. Run live_producer.py with AeroAPI enabled.")

    flights = flights.sort_values("fetched_at").drop_duplicates("flight_id", keep="last")
    scored = enrich_and_score_live_flights(flights, weather, 15.0)
    ev = evaluate_live_vs_historical(scored)
    if not ev.get("has_live"):
        sys.exit("No landed flights with a measured delay yet.")

    table = ev["comparison_table"].rename(columns={
        "Model": "model", "Live Today MAE (min)": "live_mae", "2024 Historical MAE (min)": "hist_mae",
        "Live Today RMSE (min)": "live_rmse", "2024 Historical RMSE (min)": "hist_rmse",
        "Live vs. No-Change": "live_gain_vs_no_change", "2024 vs. No-Change": "hist_gain_vs_no_change",
    })
    out_csv = ROOT / "models" / "live_vs_historical.csv"
    table.to_csv(out_csv, index=False)

    metrics_path = ROOT / "models" / "training_metrics.json"
    hist_val_flights = (
        int(json.loads(metrics_path.read_text(encoding="utf-8"))["val_count"])
        if metrics_path.exists()
        else int(ev.get("hist_val_count", 3041312))
    )
    arrived = ev["arrived_df"]
    summary = {
        "landed_flights_scored": int(ev["arrived_count"]),
        "first_fetch_utc": str(flights["fetched_at"].min()),
        "last_fetch_utc": str(flights["fetched_at"].max()),
        "recorded_sessions": len(files),
        "share_arriving_early_pct": ev["early_share_pct"],
        "mean_actual_delta_min": round(float(arrived["actual_delta_delay"].mean()), 2),
        "late_over_15_n": int((arrived["actual_delta_delay"] > 15).sum()),
        "hist_validation_flights": hist_val_flights,
        "caveats": [
            "Live rows use a rule-based estimator, not the trained Spark GBT.",
            "Live baseline 2 uses 2024 hub averages (not true lane means).",
            "Landed flights from one evening (about 5 hours of fetches); small sample, one time of day.",
        ],
    }
    (ROOT / "models" / "live_vs_historical.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(table.to_string(index=False))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
