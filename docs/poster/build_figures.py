"""Build the poster figures from the repo's measured results (no hand-typed numbers).

    python docs/poster/build_figures.py [--url https://github.com/<owner>/<repo>]

Reads models/ablation.csv, models/error_by_*.csv and benchmark/results/*.jsonl; writes PNGs to
docs/poster/figures/. Then render poster.html to PDF (see docs/poster/README.md).
"""
import argparse
import glob
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent.parent
OUT = Path(__file__).resolve().parent / "figures"
BLUE, DARK, LIGHT, ORANGE, GREY, RED = "#0b5cad", "#12344d", "#9fc9d8", "#e8912d", "#9aa5ae", "#c8372d"

plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 11, "axes.spines.top": False,
    "axes.spines.right": False, "axes.edgecolor": "#55616b", "axes.titleweight": "bold",
    "axes.titlesize": 12.5,
})


def save(fig, name):
    fig.savefig(OUT / name, dpi=220, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print("wrote", name)


def ablation():
    a = pd.read_csv(ROOT / "models" / "ablation.csv")
    labels = {"baseline_no_change": "No\nchange", "baseline_lane_mean": "Lane\nmean",
              "gbt_no_weather": "GBT\nno weather", "gbt_dest_wind_vis_temp": "+ dest.\nwx v1",
              "gbt_dest_plus_ceiling": "+ dest.\nceiling", "gbt_full_weather": "GBT full\nweather"}
    a["label"] = a["model"].map(labels)
    colors = [GREY, GREY, BLUE, BLUE, BLUE, ORANGE]
    fig, ax = plt.subplots(figsize=(4.6, 2.4))
    bars = ax.bar(a["label"], a["val_mae"], color=colors)
    ax.set_ylim(9.5, 13.2)
    ax.set_ylabel("Validation MAE (min)")
    ax.set_title("Model vs. baselines and weather ablation", loc="left")
    for b, v in zip(bars, a["val_mae"]):
        ax.text(b.get_x() + b.get_width() / 2, v + 0.05, f"{v:.2f}", ha="center", fontsize=10, fontweight="bold")
    ax.tick_params(axis="x", labelsize=8.5)
    save(fig, "ablation.png")


def outcome():
    b = pd.read_csv(ROOT / "models" / "error_by_bucket.csv")
    order = ["early >15 min", "early 5-15 min", "within 5 min", "late 5-15 min", "late >15 min"]
    b = b.set_index("bucket").loc[order].reset_index()
    x = range(len(b))
    fig, ax = plt.subplots(figsize=(4.6, 2.4))
    ax.bar([i - 0.2 for i in x], b["mae_gbt"], 0.4, color=BLUE, label="GBT")
    ax.bar([i + 0.2 for i in x], b["mae_no_change"], 0.4, color=GREY, label="No change")
    ax.set_xticks(list(x))
    ax.set_xticklabels(["early\n>15 min", "early\n5-15", "on time\n(within 5)", "late\n5-15", "late\n>15 min"],
                       fontsize=8.5)
    ax.set_ylabel("MAE (min)")
    ax.set_title("Error by what actually happened", loc="left")
    ax.legend(frameon=False, loc="upper center", ncol=2, fontsize=9)
    ax.set_ylim(0, 41)
    save(fig, "outcome.png")


def hubs():
    h = pd.read_csv(ROOT / "models" / "error_by_hub.csv").sort_values("mae_gbt", ascending=False)
    fig, ax = plt.subplots(figsize=(4.6, 3.3))
    ax.bar(h["hub"], h["mae_gbt"], color=BLUE, label="GBT")
    ax.plot(h["hub"], h["mae_lane_mean"], "o", color=ORANGE, markersize=4, label="Lane mean")
    ax.set_ylabel("MAE (min)")
    ax.set_title("Error by origin hub (24 hubs)", loc="left")
    ax.tick_params(axis="x", labelrotation=90, labelsize=7.5)
    ax.legend(frameon=False, fontsize=9)
    save(fig, "hubs.png")


def benchmark():
    rows = []
    for f in glob.glob(str(ROOT / "benchmark" / "results" / "metrics_*.jsonl")):
        rows += [json.loads(line) for line in open(f, encoding="utf-8") if line.strip()]
    df = pd.DataFrame(rows)
    agg = df.groupby("config")["throughput_rps"].mean().sort_values()
    colors = [ORANGE if c == agg.index[-1] else (BLUE if c.startswith("c4") else LIGHT) for c in agg.index]
    fig, ax = plt.subplots(figsize=(4.6, 2.7))
    ax.barh(agg.index, agg.values, color=colors)
    ax.set_xlabel("Mean throughput (records/s)")
    ax.set_title("Throughput, 12 Spark/Kafka configs", loc="left")
    ax.tick_params(axis="y", labelsize=8)
    for i, v in enumerate(agg.values):
        ax.text(v + 40, i, f"{v:,.0f}", va="center", fontsize=8)
    ax.set_xlim(0, agg.max() * 1.15)
    save(fig, "benchmark.png")


def live_vs_hist():
    t = pd.read_csv(ROOT / "models" / "live_vs_historical.csv")
    n = json.loads((ROOT / "models" / "live_vs_historical.json").read_text(encoding="utf-8"))["landed_flights_scored"]
    labels = ["Assume the delay\nstays the same", "Historical\nhub average", "Estimator (live)\nSpark GBT (2024)"]
    x = range(len(t))
    fig, ax = plt.subplots(figsize=(6.2, 3.0))
    b1 = ax.bar([i - 0.2 for i in x], t["live_mae"], 0.4, color=ORANGE, label=f"Live, {n} landed flights")
    b2 = ax.bar([i + 0.2 for i in x], t["hist_mae"], 0.4, color=BLUE, label="2024 history, 1.67M flights")
    for bars in (b1, b2):
        for b in bars:
            ax.text(b.get_x() + b.get_width() / 2, b.get_height() + 0.15, f"{b.get_height():.1f}",
                    ha="center", fontsize=10, fontweight="bold")
    ax.set_xticks(list(x)); ax.set_xticklabels(labels, fontsize=9)
    ax.set_ylabel("Arrival error, MAE (min)")
    ax.set_ylim(0, 15.5)
    ax.set_title("Live vs. history: lower is better", loc="left")
    ax.legend(frameon=False, fontsize=9, loc="upper right", bbox_to_anchor=(1.0, 1.04))
    save(fig, "live_vs_hist.png")


def qr(url):
    import qrcode
    img = qrcode.make(url, box_size=10, border=1)
    img.save(OUT / "qr_repo.png")
    print("wrote qr_repo.png ->", url)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="https://github.com/mosomo82/HyperLogistic_Stream")
    args = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    ablation(); outcome(); hubs(); benchmark(); live_vs_hist(); qr(args.url)
