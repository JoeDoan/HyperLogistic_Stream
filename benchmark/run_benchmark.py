#!/usr/bin/env python3
"""
benchmark/run_benchmark.py
RQ2 Benchmark Runner: 3×3×3×3 Factorial Experiment Grid.

Sweeps:
  - CPU Cores: 2, 4, 8
  - Shuffle Partitions: 8, 64, 200
  - Kafka Replay Rates: 1k, 5k, 20k eps
  - maxOffsetsPerTrigger: 5k, 20k, 100k

For each configuration:
  1. Starts the Kafka producer at the specified rate (subprocess)
  2. Runs the Spark Structured Streaming job for warmup + measurement batches
  3. Collects throughput, batch duration, p50/p95 latency
  4. Writes results to benchmark/results/metrics_<label>.jsonl

Usage:
    python benchmark/run_benchmark.py --quick     # reduced 2x2x2x2 grid
    python benchmark/run_benchmark.py --cores 4   # single-dimension sweep
    python benchmark/run_benchmark.py             # full 3x3x3x3 grid
"""

import os
import sys
import json
import time
import subprocess
import threading
import argparse
from pathlib import Path
from datetime import datetime
from itertools import product
from typing import Iterator

import yaml
import pandas as pd
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
CONFIG_PATH = Path(os.environ.get("HYPERLOGISTICS_CONFIG", PROJECT_ROOT / "config" / "pipeline_config.yaml"))
RESULTS_DIR = PROJECT_ROOT / "benchmark" / "results"


def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def grid_configs(cfg: dict, quick: bool = False) -> list[dict]:
    """Generate all benchmark configurations."""
    if quick:
        cores_list = [2, 4]
        shuffle_list = [8, 64]
        rates_list = [1000, 5000]
        offsets_list = [5000, 20000]
    else:
        cores_list = cfg["benchmark"]["cores"]
        shuffle_list = cfg["benchmark"]["shuffle_partitions"]
        rates_list = cfg["benchmark"]["ingestion_rates"]
        offsets_list = cfg["benchmark"]["max_offsets"]

    configs = []
    for cores, shuffle, rate, offsets in product(cores_list, shuffle_list, rates_list, offsets_list):
        label = f"c{cores}_sp{shuffle}_r{rate}_mo{offsets}"
        configs.append({
            "label": label,
            "cores": cores,
            "shuffle_partitions": shuffle,
            "ingestion_rate": rate,
            "max_offsets": offsets,
        })
    return configs


def start_producer(bootstrap: str, topic: str, rate: int, limit: int = 50000) -> subprocess.Popen:
    """Start Kafka producer subprocess."""
    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "pipeline" / "02_kafka_producer.py"),
        "--rate", str(rate),
        "--limit", str(limit),
        "--bootstrap", bootstrap,
        "--topic", topic,
    ]
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1
    )
    return proc


def drain_proc_output(proc: subprocess.Popen, prefix: str = "[PRODUCER]"):
    """Read subprocess output in a background thread."""
    def _drain():
        for line in proc.stdout:
            print(f"{prefix} {line.rstrip()}")
    t = threading.Thread(target=_drain, daemon=True)
    t.start()
    return t


def run_streaming_job(
    cfg: dict,
    cores: int,
    shuffle_partitions: int,
    max_offsets: int,
    label: str,
    warmup_batches: int = 3,
    measure_batches: int = 10,
    bootstrap: str = None,
) -> dict:
    """Run the Spark streaming job as subprocess, parse metrics output."""
    bootstrap = bootstrap or cfg["kafka"]["bootstrap_servers"]

    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "pipeline" / "03_stream_processor.py"),
        "--cores", str(cores),
        "--shuffle-partitions", str(shuffle_partitions),
        "--max-offsets", str(max_offsets),
        "--benchmark-label", label,
        "--test",  # runs for 30s then exits
        "--bootstrap", bootstrap,
    ]

    print(f"\n[BENCH] Launching Spark streaming: {label}")
    print(f"  Cmd: {' '.join(cmd)}")

    t_start = time.time()
    proc = subprocess.run(
        cmd, capture_output=True, text=True, timeout=120
    )
    elapsed = time.time() - t_start

    # Parse metrics from the jsonl file written by the streaming job
    metrics_file = RESULTS_DIR / f"metrics_{label}.jsonl"
    if not metrics_file.exists():
        print(f"  [BENCH] No metrics file found for {label}")
        return {"label": label, "error": "no metrics file", "elapsed": elapsed}

    records = []
    with open(metrics_file) as f:
        for line in f:
            try:
                records.append(json.loads(line.strip()))
            except Exception:
                pass

    if not records:
        return {"label": label, "error": "empty metrics", "elapsed": elapsed}

    df = pd.DataFrame(records)

    # Skip warmup batches
    if len(df) > warmup_batches:
        df = df.iloc[warmup_batches:]

    return {
        "label": label,
        "cores": cores,
        "shuffle_partitions": shuffle_partitions,
        "max_offsets": max_offsets,
        "batches_measured": len(df),
        "total_rows": int(df["num_rows"].sum()),
        "avg_throughput_rps": float(df["throughput_rps"].mean()),
        "p50_batch_ms": float(df["duration_ms"].quantile(0.50)),
        "p95_batch_ms": float(df["duration_ms"].quantile(0.95)),
        "median_p50_latency_ms": float(df["p50_latency_ms"].dropna().median()) if df["p50_latency_ms"].notna().any() else None,
        "median_p95_latency_ms": float(df["p95_latency_ms"].dropna().median()) if df["p95_latency_ms"].notna().any() else None,
        "wall_elapsed_sec": elapsed,
    }


def summarize_results(results: list[dict]) -> pd.DataFrame:
    """Pivot results into a readable comparison table."""
    df = pd.DataFrame([r for r in results if "error" not in r])
    if df.empty:
        return df

    print("\n" + "=" * 80)
    print("RQ2 BENCHMARK SUMMARY")
    print("=" * 80)

    display_cols = [
        "label", "cores", "shuffle_partitions", "max_offsets",
        "avg_throughput_rps", "p50_batch_ms", "p95_batch_ms",
        "median_p50_latency_ms", "median_p95_latency_ms",
    ]
    available = [c for c in display_cols if c in df.columns]
    print(df[available].to_string(index=False, float_format="{:.1f}".format))
    print("=" * 80)
    return df


def generate_report(df: pd.DataFrame, output_path: Path):
    """Save consolidated report CSV and a markdown insights table."""
    df.to_csv(output_path / "benchmark_summary.csv", index=False)
    print(f"[BENCH] Summary CSV → {output_path / 'benchmark_summary.csv'}")

    # Best config by throughput
    if "avg_throughput_rps" in df.columns and not df.empty:
        best = df.loc[df["avg_throughput_rps"].idxmax()]
        worst = df.loc[df["avg_throughput_rps"].idxmin()]
        insights = [
            "# RQ2 Benchmark Insights\n",
            f"**Best throughput config:** `{best['label']}` → {best['avg_throughput_rps']:.0f} rps",
            f"**Worst throughput config:** `{worst['label']}` → {worst['avg_throughput_rps']:.0f} rps",
            "",
            "## Top 5 Configurations by Throughput",
            df.nlargest(5, "avg_throughput_rps")[
                ["label", "cores", "shuffle_partitions", "max_offsets", "avg_throughput_rps", "p50_batch_ms"]
            ].to_markdown(index=False),
            "",
            "## Latency Rankings (p95 Batch Time)",
            df.nsmallest(5, "p95_batch_ms")[
                ["label", "cores", "shuffle_partitions", "avg_throughput_rps", "p95_batch_ms"]
            ].to_markdown(index=False),
        ]
        with open(output_path / "benchmark_insights.md", "w") as f:
            f.write("\n".join(insights))
        print(f"[BENCH] Insights → {output_path / 'benchmark_insights.md'}")


def main():
    parser = argparse.ArgumentParser(description="HyperLogistics RQ2 Benchmark Runner")
    parser.add_argument("--quick", action="store_true", help="Run 2×2×2×2 reduced grid")
    parser.add_argument("--cores", type=int, nargs="+", default=None, help="Override core counts")
    parser.add_argument("--shuffle", type=int, nargs="+", default=None, help="Override shuffle partitions")
    parser.add_argument("--rates", type=int, nargs="+", default=None, help="Override Kafka rates")
    parser.add_argument("--offsets", type=int, nargs="+", default=None, help="Override maxOffsets values")
    parser.add_argument("--bootstrap", type=str, default=None, help="Kafka bootstrap servers")
    args = parser.parse_args()

    cfg = load_config()
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    # Build config grid
    configs = grid_configs(cfg, quick=args.quick)

    # Apply CLI overrides
    if args.cores or args.shuffle or args.rates or args.offsets:
        cores_list = args.cores or cfg["benchmark"]["cores"]
        shuffle_list = args.shuffle or cfg["benchmark"]["shuffle_partitions"]
        rates_list = args.rates or cfg["benchmark"]["ingestion_rates"]
        offsets_list = args.offsets or cfg["benchmark"]["max_offsets"]
        configs = []
        for cores, shuffle, rate, offsets in product(cores_list, shuffle_list, rates_list, offsets_list):
            configs.append({
                "label": f"c{cores}_sp{shuffle}_r{rate}_mo{offsets}",
                "cores": cores, "shuffle_partitions": shuffle,
                "ingestion_rate": rate, "max_offsets": offsets,
            })

    bootstrap = args.bootstrap or cfg["kafka"]["bootstrap_servers"]
    topic = cfg["kafka"]["topic"]

    print("=" * 60)
    print(f"HyperLogistics-Stream: RQ2 Benchmark Runner")
    print(f"Grid size: {len(configs)} configurations")
    print(f"Bootstrap: {bootstrap}  Topic: {topic}")
    print("=" * 60)

    all_results = []
    total_t = time.time()

    for i, config in enumerate(configs, 1):
        print(f"\n[BENCH] === Config {i}/{len(configs)}: {config['label']} ===")
        print(f"  Cores={config['cores']}  ShufflePartitions={config['shuffle_partitions']}")
        print(f"  IngestionRate={config['ingestion_rate']} eps  MaxOffsets={config['max_offsets']}")

        # 1. Start producer at configured rate
        print(f"  Starting Kafka producer at {config['ingestion_rate']} eps...")
        producer_proc = start_producer(
            bootstrap=bootstrap,
            topic=topic,
            rate=config["ingestion_rate"],
            limit=200000,
        )
        drain_thread = drain_proc_output(producer_proc, prefix=f"  [P-{config['label']}]")

        # Give producer 3 seconds to fill the topic before Spark starts
        time.sleep(3)

        # 2. Run streaming job
        result = run_streaming_job(
            cfg=cfg,
            cores=config["cores"],
            shuffle_partitions=config["shuffle_partitions"],
            max_offsets=config["max_offsets"],
            label=config["label"],
            warmup_batches=cfg["benchmark"]["warmup_batches"],
            measure_batches=cfg["benchmark"]["measure_batches"],
            bootstrap=bootstrap,
        )
        result["ingestion_rate"] = config["ingestion_rate"]
        all_results.append(result)

        # 3. Terminate producer
        producer_proc.terminate()
        try:
            producer_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            producer_proc.kill()

        print(f"  Result: throughput={result.get('avg_throughput_rps', 'N/A'):.0f} rps  "
              f"p50_batch={result.get('p50_batch_ms', 'N/A'):.0f} ms  "
              f"p95_batch={result.get('p95_batch_ms', 'N/A'):.0f} ms")

        # Small gap between runs
        time.sleep(5)

    total_elapsed = time.time() - total_t
    print(f"\n[BENCH] All {len(configs)} configs completed in {total_elapsed:.0f}s")

    # Summarize and save
    summary_df = summarize_results(all_results)
    if not summary_df.empty:
        generate_report(summary_df, RESULTS_DIR)

    # Save full results as JSON
    full_results_path = RESULTS_DIR / "benchmark_full_results.json"
    with open(full_results_path, "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"[BENCH] Full results → {full_results_path}")


if __name__ == "__main__":
    main()
