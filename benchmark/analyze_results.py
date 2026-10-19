#!/usr/bin/env python3
"""
benchmark/analyze_results.py
RQ2 Post-Hoc Analysis: Generate publication figures from benchmark results.

Produces:
  - fig_rq2_throughput_heatmap.png   — Throughput (rps) across (cores, shuffle_partitions)
  - fig_rq2_latency_boxplot.png      — p50/p95 batch time distribution per config
  - fig_rq2_scalability_curve.png    — Throughput vs. cores (speedup curve)
  - fig_rq2_kafka_rate_impact.png    — Throughput vs. ingestion rate

Usage:
    python benchmark/analyze_results.py
"""

import json
from pathlib import Path

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import seaborn as sns

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = PROJECT_ROOT / "benchmark" / "results"
FIGS_DIR = PROJECT_ROOT / "reports" / "figures"

plt.style.use("seaborn-v0_8-whitegrid" if "seaborn-v0_8-whitegrid" in plt.style.available else "default")
plt.rcParams.update({"font.size": 11, "axes.labelweight": "bold"})


def load_results() -> pd.DataFrame:
    fpath = RESULTS_DIR / "benchmark_full_results.json"
    if not fpath.exists():
        raise FileNotFoundError(f"Run run_benchmark.py first. Expected: {fpath}")
    with open(fpath) as f:
        data = json.load(f)
    df = pd.DataFrame([r for r in data if "error" not in r])
    return df


def fig_throughput_heatmap(df: pd.DataFrame):
    """Heatmap: avg throughput across (cores × shuffle_partitions), averaged over rates."""
    pivot = (
        df.groupby(["cores", "shuffle_partitions"])["avg_throughput_rps"]
        .mean()
        .reset_index()
        .pivot(index="cores", columns="shuffle_partitions", values="avg_throughput_rps")
    )

    fig, ax = plt.subplots(figsize=(7, 4))
    sns.heatmap(
        pivot, annot=True, fmt=".0f", cmap="YlOrRd",
        linewidths=0.5, ax=ax, cbar_kws={"label": "Throughput (rps)"}
    )
    ax.set_title("RQ2: Avg Throughput by CPU Cores × Shuffle Partitions", fontweight="bold")
    ax.set_xlabel("Shuffle Partitions")
    ax.set_ylabel("CPU Cores")
    plt.tight_layout()
    outpath = FIGS_DIR / "fig_rq2_throughput_heatmap.png"
    plt.savefig(outpath, dpi=300)
    plt.close()
    print(f"Saved {outpath.name}")


def fig_latency_boxplot(df: pd.DataFrame):
    """Box plot of batch processing time distribution across selected configs."""
    # Load per-batch jsonl for richer latency data
    per_batch_records = []
    for fpath in RESULTS_DIR.glob("metrics_*.jsonl"):
        label = fpath.stem.replace("metrics_", "")
        with open(fpath) as f:
            for line in f:
                try:
                    r = json.loads(line.strip())
                    r["config"] = label
                    per_batch_records.append(r)
                except Exception:
                    pass

    if not per_batch_records:
        print("No per-batch records found for latency boxplot.")
        return

    bdf = pd.DataFrame(per_batch_records)
    # Take up to 10 representative configs sorted by avg throughput
    top_configs = (
        bdf.groupby("config")["throughput_rps"]
        .mean()
        .nlargest(10)
        .index.tolist()
    )
    plot_df = bdf[bdf["config"].isin(top_configs)]

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    sns.boxplot(
        x="config", y="duration_ms", data=plot_df, ax=axes[0],
        palette="Set2", showfliers=False
    )
    axes[0].set_title("Batch Processing Time Distribution (Top 10 Configs)", fontweight="bold")
    axes[0].set_xlabel("Configuration")
    axes[0].set_ylabel("Batch Duration (ms)")
    axes[0].tick_params(axis="x", rotation=45)

    p95_df = plot_df.dropna(subset=["p95_latency_ms"])
    if not p95_df.empty:
        sns.boxplot(
            x="config", y="p95_latency_ms", data=p95_df, ax=axes[1],
            palette="Set2", showfliers=False
        )
        axes[1].set_title("p95 End-to-End Latency Distribution", fontweight="bold")
        axes[1].set_xlabel("Configuration")
        axes[1].set_ylabel("p95 Latency (ms)")
        axes[1].tick_params(axis="x", rotation=45)

    plt.tight_layout()
    outpath = FIGS_DIR / "fig_rq2_latency_boxplot.png"
    plt.savefig(outpath, dpi=300)
    plt.close()
    print(f"Saved {outpath.name}")


def fig_scalability_curve(df: pd.DataFrame):
    """Speedup curve: throughput vs. CPU cores at fixed shuffle=8."""
    sub = df[df["shuffle_partitions"] == 8].copy()
    if sub.empty:
        sub = df.copy()

    agg = (
        sub.groupby("cores")["avg_throughput_rps"]
        .mean()
        .reset_index()
        .sort_values("cores")
    )

    if agg.empty or len(agg) < 2:
        print("Insufficient data for scalability curve.")
        return

    baseline = agg["avg_throughput_rps"].iloc[0]
    agg["speedup"] = agg["avg_throughput_rps"] / baseline
    ideal_speedup = agg["cores"] / agg["cores"].iloc[0]

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    axes[0].plot(agg["cores"], agg["avg_throughput_rps"], "o-", color="#2563eb", linewidth=2.5, label="Measured")
    axes[0].set_title("Throughput Scaling vs. CPU Cores", fontweight="bold")
    axes[0].set_xlabel("CPU Cores")
    axes[0].set_ylabel("Avg Throughput (rps)")
    axes[0].set_xticks(agg["cores"])
    axes[0].legend()

    axes[1].plot(agg["cores"], agg["speedup"], "o-", color="#059669", linewidth=2.5, label="Measured Speedup")
    axes[1].plot(agg["cores"], ideal_speedup, "--", color="#dc2626", linewidth=1.5, label="Ideal Linear Speedup")
    axes[1].set_title("Parallel Speedup (Amdahl's Law Comparison)", fontweight="bold")
    axes[1].set_xlabel("CPU Cores")
    axes[1].set_ylabel("Speedup (relative to 2 cores)")
    axes[1].set_xticks(agg["cores"])
    axes[1].legend()

    plt.tight_layout()
    outpath = FIGS_DIR / "fig_rq2_scalability_curve.png"
    plt.savefig(outpath, dpi=300)
    plt.close()
    print(f"Saved {outpath.name}")


def fig_kafka_rate_impact(df: pd.DataFrame):
    """Throughput and p95 batch time vs. Kafka ingestion rate."""
    if "ingestion_rate" not in df.columns:
        print("ingestion_rate column not found; skipping kafka rate impact figure.")
        return

    agg = (
        df.groupby("ingestion_rate")
        .agg(
            avg_throughput=("avg_throughput_rps", "mean"),
            p95_batch_ms=("p95_batch_ms", "mean"),
        )
        .reset_index()
        .sort_values("ingestion_rate")
    )

    fig, ax1 = plt.subplots(figsize=(8, 5))
    color1 = "#2563eb"
    ax1.plot(agg["ingestion_rate"], agg["avg_throughput"], "o-", color=color1, linewidth=2.5)
    ax1.set_xlabel("Kafka Ingestion Rate (events/sec)")
    ax1.set_ylabel("Avg Throughput (rps)", color=color1, fontweight="bold")
    ax1.tick_params(axis="y", labelcolor=color1)
    ax1.set_xscale("log")
    ax1.set_xticks(agg["ingestion_rate"])
    ax1.get_xaxis().set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))

    ax2 = ax1.twinx()
    color2 = "#dc2626"
    ax2.plot(agg["ingestion_rate"], agg["p95_batch_ms"], "s--", color=color2, linewidth=2, alpha=0.9)
    ax2.set_ylabel("p95 Batch Time (ms)", color=color2, fontweight="bold")
    ax2.tick_params(axis="y", labelcolor=color2)

    plt.title("RQ2: Kafka Rate Impact on Throughput & Batch Time", fontweight="bold")
    fig.tight_layout()
    outpath = FIGS_DIR / "fig_rq2_kafka_rate_impact.png"
    plt.savefig(outpath, dpi=300)
    plt.close()
    print(f"Saved {outpath.name}")


def main():
    FIGS_DIR.mkdir(parents=True, exist_ok=True)
    print("=== RQ2 Benchmark Analysis ===")

    try:
        df = load_results()
        print(f"Loaded {len(df)} config results.")
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        return

    fig_throughput_heatmap(df)
    fig_latency_boxplot(df)
    fig_scalability_curve(df)
    fig_kafka_rate_impact(df)

    print(f"\nAll figures saved to {FIGS_DIR}")


if __name__ == "__main__":
    main()
