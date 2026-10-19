"""
dashboard/app.py
Phase 5: Streamlit Live Operational Dashboard for HyperLogistics-Stream.

Panels:
  1. Live Fleet Radar Map  — PyDeck arc + scatterplot, color-coded by delay risk
  2. Hub Pressure Monitor  — Inbound arrival counts for ORD, DFW, ATL, MEM, SDF, CVG
  3. System Health Tiles   — Kafka rate, Spark batch time, p50/p95 latency gauges
  4. Benchmark Comparison  — RQ2 heatmap of throughput/latency across config grid

Usage:
    streamlit run dashboard/app.py
"""

import os
import sys
import json
import glob
import time
from pathlib import Path
from datetime import datetime, timedelta

import yaml
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "pipeline_config.yaml"

HUB_COORDS = {
    "ORD": (41.9742, -87.9073, "Chicago O'Hare"),
    "DFW": (32.8998, -97.0403, "Dallas/Fort Worth"),
    "ATL": (33.6407, -84.4277, "Atlanta Hartsfield"),
    "MEM": (35.0424, -89.9767, "Memphis (FedEx Hub)"),
    "SDF": (38.1744, -85.7360, "Louisville (UPS Hub)"),
    "CVG": (39.0489, -84.6678, "Cincinnati (Amazon Air)"),
    "LAX": (33.9425, -118.4081, "Los Angeles"),
    "JFK": (40.6413, -73.7781, "New York JFK"),
    "ORD": (41.9742, -87.9073, "Chicago O'Hare"),
    "DEN": (39.8561, -104.6737, "Denver"),
    "SEA": (47.4502, -122.3088, "Seattle-Tacoma"),
    "BOS": (42.3656, -71.0096, "Boston Logan"),
    "MIA": (25.7959, -80.2870, "Miami"),
    "IAH": (29.9844, -95.3414, "Houston"),
    "MSP": (44.8848, -93.2223, "Minneapolis"),
}


def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


@st.cache_data(ttl=10)  # refresh every 10 seconds
def read_gold_predictions(max_rows: int = 2000) -> pd.DataFrame:
    cfg = load_config()
    gold_path = PROJECT_ROOT / cfg["paths"]["gold_predictions"]
    if not gold_path.exists():
        return pd.DataFrame()

    pq_files = sorted(gold_path.rglob("*.parquet"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not pq_files:
        return pd.DataFrame()

    frames = []
    total = 0
    for pf in pq_files[:10]:
        try:
            df = pd.read_parquet(pf)
            frames.append(df)
            total += len(df)
            if total >= max_rows:
                break
        except Exception:
            pass

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).head(max_rows)


@st.cache_data(ttl=10)
def read_hub_congestion() -> pd.DataFrame:
    cfg = load_config()
    agg_path = PROJECT_ROOT / cfg["paths"]["gold_congestion"]
    if not agg_path.exists():
        return pd.DataFrame()

    pq_files = sorted(agg_path.rglob("*.parquet"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not pq_files:
        return pd.DataFrame()

    frames = []
    for pf in pq_files[:5]:
        try:
            frames.append(pd.read_parquet(pf))
        except Exception:
            pass

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


@st.cache_data(ttl=10)
def read_benchmark_results() -> pd.DataFrame:
    cfg = load_config()
    bench_dir = PROJECT_ROOT / cfg["paths"]["benchmark_results"]
    if not bench_dir.exists():
        return pd.DataFrame()

    all_records = []
    for fpath in bench_dir.glob("metrics_*.jsonl"):
        with open(fpath) as f:
            for line in f:
                try:
                    all_records.append(json.loads(line.strip()))
                except Exception:
                    pass

    if not all_records:
        return pd.DataFrame()
    return pd.DataFrame(all_records)


def risk_color(delta: float) -> list:
    """Map predicted_delta_delay to RGB [R, G, B, A]."""
    if delta <= 0:
        return [16, 185, 129, 200]   # green: on time / recovering
    elif delta <= 15:
        return [251, 191, 36, 200]   # yellow: minor delay
    elif delta <= 30:
        return [249, 115, 22, 220]   # orange: moderate
    else:
        return [220, 38, 38, 240]    # red: high risk


def demo_flights_df() -> pd.DataFrame:
    """Generate synthetic flight data for demo when pipeline hasn't run yet."""
    rng = np.random.default_rng(int(time.time()) % 100)
    hubs = list(HUB_COORDS.keys())
    rows = []
    for _ in range(150):
        origin = rng.choice(hubs)
        dest = rng.choice([h for h in hubs if h != origin])
        dep_delay = float(rng.normal(10, 20))
        predicted_delta = float(rng.normal(0, 12))
        o_lat, o_lon, _ = HUB_COORDS[origin]
        d_lat, d_lon, _ = HUB_COORDS[dest]
        rows.append({
            "flight_id": f"DEMO_{origin}_{dest}_{rng.integers(1000,9999)}",
            "carrier": rng.choice(["AA", "UA", "DL", "FX", "UPS"]),
            "origin": origin, "dest": dest,
            "origin_lat": o_lat, "origin_lon": o_lon,
            "dest_lat": d_lat, "dest_lon": d_lon,
            "dep_delay": dep_delay,
            "predicted_delta_delay": predicted_delta,
            "is_high_risk": predicted_delta > 30,
        })
    return pd.DataFrame(rows)


def render_fleet_map(flights_df: pd.DataFrame):
    """Render the PyDeck arc map using fallback plotly scatter if pydeck unavailable."""
    st.subheader("Live Fleet Radar Map")

    if flights_df.empty:
        st.info("No live flight data — showing synthetic demo data.")
        flights_df = demo_flights_df()

    # Add coordinates
    if "origin_lat" not in flights_df.columns:
        for col_prefix, col_name in [("origin", "origin"), ("dest", "dest")]:
            lat_col, lon_col = f"{col_prefix}_lat", f"{col_prefix}_lon"
            flights_df[lat_col] = flights_df[col_name].map(
                lambda c: HUB_COORDS[c][0] if c in HUB_COORDS else np.nan
            )
            flights_df[lon_col] = flights_df[col_name].map(
                lambda c: HUB_COORDS[c][1] if c in HUB_COORDS else np.nan
            )

    flights_df = flights_df.dropna(subset=["origin_lat", "dest_lat"])
    flights_df["risk_score"] = flights_df["predicted_delta_delay"].clip(-20, 120)

    try:
        import pydeck as pdk

        arc_layer = pdk.Layer(
            "ArcLayer",
            data=flights_df,
            get_source_position=["origin_lon", "origin_lat"],
            get_target_position=["dest_lon", "dest_lat"],
            get_source_color=[[16, 185, 129, 180]],
            get_target_color="[is_high_risk ? 220 : 16, is_high_risk ? 38 : 185, is_high_risk ? 38 : 129, 200]",
            get_width=1.5,
            auto_highlight=True,
            pickable=True,
        )
        scatter_layer = pdk.Layer(
            "ScatterplotLayer",
            data=pd.DataFrame([
                {"name": v[2], "lat": v[0], "lon": v[1]} for v in HUB_COORDS.values()
            ]),
            get_position=["lon", "lat"],
            get_color=[59, 130, 246, 255],
            get_radius=25000,
            pickable=True,
        )
        deck = pdk.Deck(
            layers=[arc_layer, scatter_layer],
            initial_view_state=pdk.ViewState(
                latitude=38.5, longitude=-96.0, zoom=3.5, pitch=30
            ),
            tooltip={"text": "{flight_id}\n{origin}→{dest}\nDepDelay: {dep_delay}m\nPredicted Δ: {predicted_delta_delay:.1f}m"},
            map_style="mapbox://styles/mapbox/dark-v9",
        )
        st.pydeck_chart(deck)
    except ImportError:
        # Fallback to plotly scatter
        fig = go.Figure()
        for _, row in flights_df.iterrows():
            color = "red" if row["is_high_risk"] else ("orange" if row["predicted_delta_delay"] > 15 else "green")
            fig.add_trace(go.Scattergeo(
                lon=[row["origin_lon"], row["dest_lon"]],
                lat=[row["origin_lat"], row["dest_lat"]],
                mode="lines",
                line=dict(width=1, color=color),
                opacity=0.5,
                showlegend=False,
            ))
        hub_df = pd.DataFrame([
            {"name": v[2], "lat": v[0], "lon": v[1]} for v in HUB_COORDS.values()
        ])
        fig.add_trace(go.Scattergeo(
            lon=hub_df["lon"], lat=hub_df["lat"],
            mode="markers+text",
            marker=dict(size=8, color="royalblue"),
            text=hub_df["name"],
            textposition="top right",
        ))
        fig.update_layout(
            geo=dict(scope="usa", showland=True, landcolor="#1a1a2e", bgcolor="#0d1117"),
            paper_bgcolor="#0d1117", font_color="white",
            height=450, margin=dict(l=0, r=0, t=0, b=0),
        )
        st.plotly_chart(fig, use_container_width=True)

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Active Flights", len(flights_df))
    with col2:
        high_risk = int(flights_df["is_high_risk"].sum())
        st.metric("High-Risk (Δ>30m)", high_risk, delta=None)
    with col3:
        avg_delta = float(flights_df["predicted_delta_delay"].mean())
        st.metric("Avg Predicted Δ Delay", f"{avg_delta:.1f} min")


def render_hub_pressure(congestion_df: pd.DataFrame):
    """Bar chart of inbound arrival counts per hub."""
    st.subheader("Hub Inbound Pressure (Next 4 Hours)")

    top_hubs = ["ORD", "DFW", "ATL", "MEM", "SDF", "CVG"]

    if congestion_df.empty:
        # Generate demo congestion
        rng = np.random.default_rng(int(time.time()) % 50)
        rows = []
        for hub in top_hubs:
            rows.append({
                "dest": hub,
                "inbound_count": int(rng.integers(20, 150)),
                "avg_predicted_delta": float(rng.normal(5, 10)),
                "high_risk_count": int(rng.integers(0, 20)),
            })
        congestion_df = pd.DataFrame(rows)
        st.info("No live congestion data — showing synthetic demo data.")
    else:
        if "window_start" in congestion_df.columns:
            congestion_df["window_start"] = pd.to_datetime(congestion_df["window_start"])
            cutoff = datetime.utcnow() + timedelta(hours=4)
            congestion_df = congestion_df[congestion_df["window_start"] <= cutoff]

        congestion_df = (
            congestion_df[congestion_df["dest"].isin(top_hubs)]
            .groupby("dest", as_index=False)
            .agg(
                inbound_count=("inbound_count", "sum"),
                avg_predicted_delta=("avg_predicted_delta", "mean"),
                high_risk_count=("high_risk_count", "sum"),
            )
        )

    if congestion_df.empty:
        st.warning("No hub data available.")
        return

    congestion_df = congestion_df.reindex(
        [i for i in range(len(congestion_df)) if congestion_df.iloc[i]["dest"] in top_hubs]
    ).dropna()

    col1, col2 = st.columns(2)
    with col1:
        fig = px.bar(
            congestion_df, x="dest", y="inbound_count",
            color="avg_predicted_delta",
            color_continuous_scale="RdYlGn_r",
            labels={"dest": "Hub", "inbound_count": "Inbound Flights", "avg_predicted_delta": "Avg Δ Delay (min)"},
            title="Inbound Flight Volume",
        )
        fig.update_layout(height=350, paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig2 = px.bar(
            congestion_df, x="dest", y="high_risk_count",
            color_discrete_sequence=["#dc2626"],
            labels={"dest": "Hub", "high_risk_count": "High-Risk Arrivals"},
            title="High-Risk Arrivals (Δ > 30 min)",
        )
        fig2.update_layout(height=350, paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig2, use_container_width=True)


def render_system_health(benchmark_df: pd.DataFrame):
    """System metrics tiles."""
    st.subheader("System Health")

    cfg = load_config()
    gold_path = PROJECT_ROOT / cfg["paths"]["gold_predictions"]

    # Count recent gold records
    recent_count = 0
    pq_files = list(gold_path.rglob("*.parquet")) if gold_path.exists() else []
    for pf in pq_files:
        if (time.time() - pf.stat().st_mtime) < 60:
            try:
                df = pd.read_parquet(pf, columns=["flight_id"])
                recent_count += len(df)
            except Exception:
                pass

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Gold Records / min", recent_count, help="Records written to Gold table in last 60s")
    with col2:
        if not benchmark_df.empty and "throughput_rps" in benchmark_df.columns:
            avg_tput = benchmark_df["throughput_rps"].mean()
            st.metric("Avg Throughput", f"{avg_tput:.0f} rps")
        else:
            st.metric("Avg Throughput", "—")
    with col3:
        if not benchmark_df.empty and "duration_ms" in benchmark_df.columns:
            p50_ms = benchmark_df["duration_ms"].quantile(0.50)
            st.metric("p50 Batch Time", f"{p50_ms:.0f} ms")
        else:
            st.metric("p50 Batch Time", "—")
    with col4:
        if not benchmark_df.empty and "p95_latency_ms" in benchmark_df.columns:
            p95_lat = benchmark_df["p95_latency_ms"].dropna().quantile(0.95)
            st.metric("p95 E2E Latency", f"{p95_lat:.0f} ms")
        else:
            st.metric("p95 E2E Latency", "—")


def render_benchmark(benchmark_df: pd.DataFrame):
    """RQ2 heatmap — throughput across (cores × shuffle_partitions)."""
    st.subheader("RQ2 Benchmark Results")

    if benchmark_df.empty:
        st.info("No benchmark results yet. Run: `python benchmark/run_benchmark.py`")
        return

    # Aggregate per config
    agg = (
        benchmark_df.groupby("config")
        .agg(
            avg_throughput=("throughput_rps", "mean"),
            p50_batch_ms=("duration_ms", lambda x: x.quantile(0.50)),
            p95_batch_ms=("duration_ms", lambda x: x.quantile(0.95)),
            p50_latency_ms=("p50_latency_ms", "median"),
            p95_latency_ms=("p95_latency_ms", "median"),
        )
        .reset_index()
    )

    col1, col2 = st.columns(2)
    with col1:
        fig = px.bar(
            agg, x="config", y="avg_throughput",
            color="avg_throughput", color_continuous_scale="Blues",
            labels={"config": "Configuration", "avg_throughput": "Throughput (rps)"},
            title="Average Throughput by Config",
        )
        fig.update_xaxes(tickangle=45)
        fig.update_layout(height=400, paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig2 = px.scatter(
            agg, x="p50_batch_ms", y="p95_batch_ms",
            text="config",
            labels={"p50_batch_ms": "p50 Batch Time (ms)", "p95_batch_ms": "p95 Batch Time (ms)"},
            title="Batch Time: p50 vs p95 per Config",
            color="avg_throughput", color_continuous_scale="Viridis",
        )
        fig2.update_traces(textposition="top center")
        fig2.update_layout(height=400, paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig2, use_container_width=True)

    with st.expander("Raw Benchmark Table"):
        st.dataframe(agg.round(1))


def main():
    st.set_page_config(
        page_title="HyperLogistics-Stream Dashboard",
        page_icon="✈",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    # Custom dark theme header
    st.markdown(
        """
        <style>
        .main { background-color: #0d1117; }
        h1 { color: #60a5fa; font-family: 'Inter', sans-serif; }
        h2 { color: #93c5fd; }
        h3 { color: #bfdbfe; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.title("HyperLogistics-Stream — Real-Time Hub-to-Hub ETA Dashboard")
    st.caption(
        f"COMP-SCI 5540 · UMKC Fall 2026 · Tony Nguyen & Joe Doan  |  "
        f"Last refresh: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"
    )

    # Auto-refresh toggle
    with st.sidebar:
        st.header("Controls")
        auto_refresh = st.checkbox("Auto-Refresh (10s)", value=True)
        refresh_interval = st.slider("Refresh Interval (s)", 5, 60, 10)
        st.markdown("---")
        st.markdown("**API:** `http://localhost:8000/docs`")
        st.markdown("**Kafka:** `localhost:9094`")
        st.markdown("**Spark UI:** `localhost:4040`")

    # Load data
    flights_df = read_gold_predictions()
    congestion_df = read_hub_congestion()
    benchmark_df = read_benchmark_results()

    # Render panels
    render_fleet_map(flights_df)
    st.divider()
    render_hub_pressure(congestion_df)
    st.divider()
    render_system_health(benchmark_df)
    st.divider()
    render_benchmark(benchmark_df)

    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()


if __name__ == "__main__":
    main()
