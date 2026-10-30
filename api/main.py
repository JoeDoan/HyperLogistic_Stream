"""
api/main.py
Phase 5: FastAPI REST Microservice for HyperLogistics-Stream.

Endpoints:
  GET /api/v1/flights/active          — Current in-flight predictions from Gold table
  GET /api/v1/hubs/{hub_code}/congestion — Hub inbound pressure from Gold aggregations
  GET /api/v1/system/metrics           — Streaming pipeline health metrics
  GET /api/v1/benchmark/results        — Latest benchmark summary

Usage:
    uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
"""

import os
import sys
import json
import glob
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional

import yaml
import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "pipeline_config.yaml"


def load_config():
    cfg_path = os.environ.get("HYPERLOGISTICS_CONFIG", str(CONFIG_PATH))
    with open(cfg_path) as f:
        return yaml.safe_load(f)


def resolve_path(base_path: str) -> Path:
    return Path(base_path) if os.path.isabs(base_path) else (PROJECT_ROOT / base_path)


cfg = load_config()

app = FastAPI(
    title="HyperLogistics-Stream API",
    description="Real-Time Hub-to-Hub ETA Prediction — COMP-SCI 5540 UMKC",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# --- Pydantic response models ---

class FlightPrediction(BaseModel):
    flight_id: str
    carrier: str
    origin: str
    dest: str
    dep_delay: float
    predicted_delta_delay: float
    predicted_eta_utc: Optional[str]
    is_high_risk: bool
    dest_wind_speed: Optional[float]
    dest_visibility: Optional[float]
    dest_temp: Optional[float]


class HubCongestion(BaseModel):
    hub: str
    window_start: str
    window_end: str
    inbound_count: int
    avg_predicted_delta: float
    high_risk_count: int


class SystemMetrics(BaseModel):
    status: str
    gold_records_last_10min: int
    latest_batch_ts: Optional[str]
    benchmark_configs_run: int


# --- Data access helpers ---

def read_latest_parquet(base_path: str, max_rows: int = 1000):
    """Read the most-recently-written partition from a Parquet directory."""
    path = resolve_path(base_path)
    if not path.exists():
        return pd.DataFrame()

    # Find leaf parquet files, sorted by mtime descending
    pq_files = sorted(
        path.rglob("*.parquet"),
        key=lambda p: p.stat().st_mtime,
        reverse=True
    )
    if not pq_files:
        return pd.DataFrame()

    frames = []
    total = 0
    for pf in pq_files:
        df = pd.read_parquet(pf)
        frames.append(df)
        total += len(df)
        if total >= max_rows:
            break

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    return combined.head(max_rows)


# --- Routes ---

@app.get("/")
async def root():
    return {
        "service": "HyperLogistics-Stream API",
        "version": "1.0.0",
        "docs": "/docs",
    }


@app.get("/api/v1/flights/active", response_model=list[FlightPrediction])
async def get_active_flights(
    limit: int = Query(default=100, le=500, description="Max flights to return"),
    hub: Optional[str] = Query(default=None, description="Filter by destination hub"),
    high_risk_only: bool = Query(default=False, description="Return only high-risk flights"),
):
    """Return current in-flight ETA predictions from the Gold table."""
    gold_path = cfg["paths"]["gold_predictions"]
    df = read_latest_parquet(gold_path, max_rows=5000)

    if df.empty:
        return []

    # Filter to recent partition dates relative to the latest batch date
    if "gold_date" in df.columns and not df.empty:
        dates = pd.to_datetime(df["gold_date"]).dt.date
        anchor_date = dates.max()
        df = df[dates >= (anchor_date - timedelta(days=1))]

    if hub:
        df = df[df["dest"].str.upper() == hub.upper()]

    if high_risk_only:
        df = df[df["is_high_risk"] == True]

    df = df.head(limit)

    results = []
    for _, row in df.iterrows():
        try:
            results.append(FlightPrediction(
                flight_id=str(row.get("flight_id", "")),
                carrier=str(row.get("carrier", "")),
                origin=str(row.get("origin", "")),
                dest=str(row.get("dest", "")),
                dep_delay=float(row.get("dep_delay", 0)),
                predicted_delta_delay=float(row.get("predicted_delta_delay", 0)),
                predicted_eta_utc=str(row.get("predicted_eta_utc", "")),
                is_high_risk=bool(row.get("is_high_risk", False)),
                dest_wind_speed=float(row["DestWindSpeed"]) if pd.notna(row.get("DestWindSpeed")) else None,
                dest_visibility=float(row["DestVisibility"]) if pd.notna(row.get("DestVisibility")) else None,
                dest_temp=float(row["DestTemp"]) if pd.notna(row.get("DestTemp")) else None,
            ))
        except Exception:
            continue

    return results


@app.get("/api/v1/hubs/{hub_code}/congestion", response_model=list[HubCongestion])
async def get_hub_congestion(
    hub_code: str,
    hours_ahead: int = Query(default=4, le=12, description="Hours of arrival window to show"),
):
    """Return predicted inbound arrival pressure for a specific hub."""
    agg_path = cfg["paths"]["gold_congestion"]
    df = read_latest_parquet(agg_path, max_rows=10000)

    if df.empty:
        raise HTTPException(status_code=404, detail=f"No congestion data found for hub {hub_code}")

    hub_code = hub_code.upper()
    df = df[df["dest"].str.upper() == hub_code].copy()

    if df.empty:
        raise HTTPException(status_code=404, detail=f"No data for hub {hub_code}")

    # Filter to relevant time windows anchored to the latest replay watermark
    if "window_start" in df.columns and not df.empty:
        df["window_start"] = pd.to_datetime(df["window_start"])
        df["window_end"] = pd.to_datetime(df["window_end"])
        anchor = df["window_start"].max()
        cutoff_start = anchor - timedelta(hours=1)
        cutoff_end = anchor + timedelta(hours=hours_ahead)
        df = df[(df["window_end"] >= cutoff_start) & (df["window_start"] <= cutoff_end)]

    df = df.sort_values("window_start").head(50)

    results = []
    for _, row in df.iterrows():
        try:
            results.append(HubCongestion(
                hub=hub_code,
                window_start=str(row.get("window_start", "")),
                window_end=str(row.get("window_end", "")),
                inbound_count=int(row.get("inbound_count", 0)),
                avg_predicted_delta=float(row.get("avg_predicted_delta", 0)),
                high_risk_count=int(row.get("high_risk_count", 0)),
            ))
        except Exception:
            continue

    if not results:
        raise HTTPException(status_code=404, detail=f"No upcoming congestion windows for hub {hub_code}")

    return results


@app.get("/api/v1/system/metrics", response_model=SystemMetrics)
async def get_system_metrics():
    """Return real-time Spark streaming health metrics."""
    gold_path = resolve_path(cfg["paths"]["gold_predictions"])
    benchmark_dir = resolve_path(cfg["paths"]["benchmark_results"])

    # Count Gold records from last 10 minutes
    recent_count = 0
    latest_batch_ts = None
    pq_files = list(gold_path.rglob("*.parquet")) if gold_path.exists() else []
    if pq_files:
        recent_files = [
            p for p in pq_files
            if (time.time() - p.stat().st_mtime) < 600  # last 10 min
        ]
        for pf in recent_files:
            try:
                df = pd.read_parquet(pf, columns=["flight_id"])
                recent_count += len(df)
            except Exception:
                pass
        if pq_files:
            latest_file = max(pq_files, key=lambda p: p.stat().st_mtime)
            latest_batch_ts = datetime.utcfromtimestamp(
                latest_file.stat().st_mtime
            ).isoformat() + "Z"

    # Count benchmark result files
    bench_count = len(list(benchmark_dir.glob("metrics_*.jsonl"))) if benchmark_dir.exists() else 0

    status = "active" if recent_count > 0 else "idle"

    return SystemMetrics(
        status=status,
        gold_records_last_10min=recent_count,
        latest_batch_ts=latest_batch_ts,
        benchmark_configs_run=bench_count,
    )


@app.get("/api/v1/benchmark/results")
async def get_benchmark_results(
    config: Optional[str] = Query(default=None, description="Filter by config label"),
    summary: bool = Query(default=True, description="Return aggregated summary"),
):
    """Return RQ2 benchmark metrics."""
    bench_dir = resolve_path(cfg["paths"]["benchmark_results"])
    if not bench_dir.exists():
        return {"results": [], "summary": {}}

    pattern = f"metrics_{config}.jsonl" if config else "metrics_*.jsonl"
    files = list(bench_dir.glob(pattern))

    all_records = []
    for fpath in files:
        with open(fpath) as f:
            for line in f:
                try:
                    all_records.append(json.loads(line.strip()))
                except json.JSONDecodeError:
                    pass

    if not all_records:
        return {"results": [], "summary": {}}

    if not summary:
        return {"results": all_records}

    # Aggregate per config
    df = pd.DataFrame(all_records)
    agg = (
        df.groupby("config")
        .agg(
            batches=("batch_id", "count"),
            total_rows=("num_rows", "sum"),
            avg_throughput_rps=("throughput_rps", "mean"),
            p50_batch_ms=("duration_ms", lambda x: float(x.quantile(0.50))),
            p95_batch_ms=("duration_ms", lambda x: float(x.quantile(0.95))),
            median_p50_latency_ms=("p50_latency_ms", "median"),
            median_p95_latency_ms=("p95_latency_ms", "median"),
        )
        .reset_index()
        .to_dict(orient="records")
    )

    return {"configs_run": len(files), "summary": agg, "total_records": len(all_records)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True)
