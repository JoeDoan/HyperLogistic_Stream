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

from live_radar import render_live_radar

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "pipeline_config.yaml"

HUB_COORDS = {
    "ATL": (33.6407, -84.4277, "Atlanta Hartsfield"),
    "ORD": (41.9742, -87.9073, "Chicago O'Hare"),
    "DFW": (32.8998, -97.0403, "Dallas/Fort Worth"),
    "DEN": (39.8561, -104.6737, "Denver"),
    "LAX": (33.9425, -118.4081, "Los Angeles"),
    "JFK": (40.6413, -73.7781, "New York JFK"),
    "EWR": (40.6895, -74.1745, "Newark Liberty"),
    "SFO": (37.6213, -122.3790, "San Francisco"),
    "MEM": (35.0424, -89.9767, "Memphis (FedEx Hub)"),
    "SDF": (38.1744, -85.7360, "Louisville (UPS Hub)"),
    "CVG": (39.0489, -84.6678, "Cincinnati (Amazon Air)"),
    "CLT": (35.2140, -80.9431, "Charlotte Douglas"),
    "LAS": (36.0840, -115.1537, "Las Vegas"),
    "PHX": (33.4352, -112.0101, "Phoenix Sky Harbor"),
    "MIA": (25.7959, -80.2870, "Miami"),
    "IAH": (29.9844, -95.3414, "Houston"),
    "BOS": (42.3656, -71.0096, "Boston Logan"),
    "SEA": (47.4502, -122.3088, "Seattle-Tacoma"),
    "MSP": (44.8848, -93.2223, "Minneapolis"),
    "DTW": (42.2162, -83.3554, "Detroit Metro"),
    "PHL": (39.8744, -75.2424, "Philadelphia"),
    "BWI": (39.1774, -76.6684, "Baltimore/Washington"),
    "SLC": (40.7899, -111.9791, "Salt Lake City"),
    "MCO": (28.4312, -81.3081, "Orlando"),
}


def load_config():
    cfg_path = os.environ.get("HYPERLOGISTICS_CONFIG", str(CONFIG_PATH))
    with open(cfg_path) as f:
        return yaml.safe_load(f)

# Add list_parquet_files function for list parquet files
def list_parquet_files(base: Path) -> list[Path]:
    """Committed Parquet files, newest first. Skips Spark temp/hidden files and files that vanish mid-scan."""
    files = []
    for p in base.rglob("*.parquet"):
        if any(part.startswith(("_", ".")) for part in p.relative_to(base).parts):
            continue  # _temporary, _spark_metadata, .crc files
        try:
            files.append((p.stat().st_mtime, p))
        except FileNotFoundError:
            continue  # moved by Spark between listing and stat
    return [p for _, p in sorted(files, key=lambda x: x[0], reverse=True)]

def resolve_path(base_path: str) -> Path:
    return Path(base_path) if os.path.isabs(base_path) else (PROJECT_ROOT / base_path)


@st.cache_data(ttl=10)  # refresh every 10 seconds
def read_gold_predictions(max_rows: int = 2000) -> pd.DataFrame:
    cfg = load_config()
    gold_path = resolve_path(cfg["paths"]["gold_predictions"])
    if not gold_path.exists():
        return pd.DataFrame()

    pq_files = list_parquet_files(gold_path)
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
    agg_path = resolve_path(cfg["paths"]["gold_congestion"])
    if not agg_path.exists():
        return pd.DataFrame()

    pq_files = list_parquet_files(agg_path)
    if not pq_files:
        return pd.DataFrame()

    frames = []
    for pf in pq_files[:300]:
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
    bench_dir = resolve_path(cfg["paths"]["benchmark_results"])
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
            "is_high_risk": predicted_delta > 5,
        })
    return pd.DataFrame(rows)


def render_fleet_map(flights_df: pd.DataFrame):
    """Render the PyDeck arc map using fallback plotly scatter if pydeck unavailable."""
    st.subheader("Live Fleet Radar Map")

    if flights_df.empty:
        st.info("No live flight data — showing synthetic demo data.")
        flights_df = demo_flights_df()
        # Remove duplicate flights and add live data to the dashboard 
        flights_df = flights_df.drop_duplicates("flight_id", keep="last") 

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
    flights_df["predicted_delta_delay"] = flights_df["predicted_delta_delay"].round(1) # Add the roundup 

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
            tooltip={"text": "{flight_id}\n{origin}→{dest}\nDepDelay: {dep_delay}m\nPredicted Δ: {predicted_delta_delay}m"},
            map_style="mapbox://styles/mapbox/dark-v9",
        )
        st.pydeck_chart(deck)
    except ImportError:
        # Fallback to plotly scatter
        fig = go.Figure()
        for _, row in flights_df.iterrows():
            color = "red" if row["is_high_risk"] else ("orange" if row["predicted_delta_delay"] > 2 else "green")
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
        st.plotly_chart(fig, width='stretch')

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Flights shown (latest)", f"{flights_df['flight_id'].nunique():,}") # Total unique flights
    with col2:
        high_risk = int(flights_df["is_high_risk"].sum())
        st.metric("Elevated risk (Δ>5m)", high_risk, delta=None)
    with col3:
        avg_delta = float(flights_df["predicted_delta_delay"].mean())
        st.metric("Avg Predicted Δ Delay", f"{avg_delta:.1f} min")


def render_hub_pressure(congestion_df: pd.DataFrame):
    """Bar chart of inbound arrival counts per hub."""
    st.subheader("Hub Inbound Pressure (selected arrival hour)")

    top_hubs = ["ATL", "DEN", "ORD", "DFW", "LAX", "PHX", "LAS", "MCO"] # List of top hubs

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
        congestion_df = congestion_df[congestion_df["dest"].isin(top_hubs)].copy()
        if not congestion_df.empty:
            congestion_df["window_start"] = pd.to_datetime(congestion_df["window_start"])
            congestion_df["wsum"] = congestion_df["avg_predicted_delta"] * congestion_df["inbound_count"]

            # each micro-batch writes partial counts for the same window, so sum them
            agg = (congestion_df.groupby(["window_start", "dest"], as_index=False)
                   .agg(inbound_count=("inbound_count", "sum"),
                        wsum=("wsum", "sum"),
                        high_risk_count=("high_risk_count", "sum")))
            agg["avg_predicted_delta"] = agg["wsum"] / agg["inbound_count"]

            # latest windows in which every hub has flights
            hubs_per = agg.groupby("window_start")["dest"].nunique()
            good = hubs_per[hubs_per >= len(top_hubs)].index.sort_values(ascending=False)
            windows = good.tolist()[:48] or agg["window_start"].sort_values(ascending=False).unique().tolist()[:48]
            chosen = st.selectbox("Arrival hour (UTC, replay time)", windows,
                                  format_func=lambda t: pd.Timestamp(t).strftime("%b %d %H:%M"))
            congestion_df = agg[agg["window_start"] == chosen][
                ["dest", "inbound_count", "avg_predicted_delta", "high_risk_count"]]

    if congestion_df.empty:
        st.warning("No hub data available.")
        return

    col1, col2 = st.columns(2)
    with col1:
        fig = px.bar(
            congestion_df, x="dest", y="inbound_count",
            color="avg_predicted_delta",
            color_continuous_scale="RdYlGn_r",
            range_color=[-30, 30],
            color_continuous_midpoint=0,
            labels={"dest": "Hub", "inbound_count": "Inbound Flights", "avg_predicted_delta": "Avg Δ Delay (min)"},
            title="Inbound Flight Volume",
        )
        fig.update_layout(height=350, paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, width='stretch')

    with col2:
        fig2 = px.bar(
            congestion_df, x="dest", y="high_risk_count",
            color_discrete_sequence=["#dc2626"],
            labels={"dest": "Hub", "high_risk_count": "High-Risk Arrivals"},
            title="Elevated-Risk Arrivals (Δ > 5 min)",
        )
        fig2.update_layout(height=350, paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig2, use_container_width=True)


ANALYTICS_COLS = [
    "flight_id", "origin", "dest", "predicted_delta_delay", "is_high_risk",
    "DestWindSpeed", "DestVisibility", "DestCeiling",
]

# (factor, column, bin edges, labels); the last bin is the "clear" reference.
WEATHER_FACTORS = [
    ("Visibility", "DestVisibility", [0, 1609.34, 4828.03, 8046.72, float("inf")],
     ["<1 mi", "1–3 mi", "3–5 mi", ">5 mi"]),
    ("Wind", "DestWindSpeed", [-1, 3, 6, 9, float("inf")],
     ["<3 m/s", "3–6 m/s", "6–9 m/s", "≥9 m/s"]),
    ("Ceiling", "DestCeiling", [0, 152.4, 304.8, 914.4, float("inf")],
     ["<500 ft", "500–1k ft", "1k–3k ft", ">3k ft"]),
]
MIN_BUCKET_N = 30
MIN_LANE_N = 20


@st.cache_data(ttl=60)
def read_gold_analytics(max_files: int = 400) -> pd.DataFrame:
    """Wider slice of Gold predictions for the story charts (de-duplicated by flight_id)."""
    import pyarrow.parquet as pq

    cfg = load_config()
    gold_path = resolve_path(cfg["paths"]["gold_predictions"])
    if not gold_path.exists():
        return pd.DataFrame()
    frames = []
    for pf in list_parquet_files(gold_path)[:max_files]:
        try:
            names = set(pq.read_schema(pf).names)
            cols = [c for c in ANALYTICS_COLS if c in names]
            frames.append(pd.read_parquet(pf, columns=cols))
        except Exception:
            pass
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    if "flight_id" in df.columns:
        df = df.drop_duplicates("flight_id")
    return df


def weather_impact_table(df: pd.DataFrame) -> pd.DataFrame:
    """Avg predicted delta and high-risk share per weather bucket (long format)."""
    rows = []
    for factor, col, edges, labels in WEATHER_FACTORS:
        if col not in df.columns:
            continue
        vals = pd.to_numeric(df[col], errors="coerce")
        bucket = pd.cut(vals, bins=edges, labels=labels, right=False)
        g = (df.assign(_b=bucket).dropna(subset=["_b"])
             .groupby("_b", observed=True)
             .agg(n=("predicted_delta_delay", "size"),
                  avg_delta=("predicted_delta_delay", "mean"),
                  high_risk_pct=("is_high_risk", lambda s: 100 * s.astype(float).mean()))
             .reset_index().rename(columns={"_b": "bucket"}))
        g["factor"] = factor
        g["bucket"] = g["bucket"].astype(str)
        rows.append(g)
    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True)
    return out[out["n"] >= MIN_BUCKET_N]


def weather_so_what(tbl: pd.DataFrame) -> str:
    """One sentence: the weather factor with the biggest predicted-delay gap vs clear conditions."""
    best = None
    for factor, _col, _edges, labels in WEATHER_FACTORS:
        sub = tbl[tbl["factor"] == factor].set_index("bucket")
        if labels[-1] not in sub.index or len(sub) < 2:
            continue
        ref = sub.loc[labels[-1], "avg_delta"]
        worst_label = (sub["avg_delta"] - ref).abs().idxmax()
        gap = sub.loc[worst_label, "avg_delta"] - ref
        if best is None or abs(gap) > abs(best[2]):
            best = (factor, worst_label, gap, int(sub.loc[worst_label, "n"]))
    if best is None:
        return "Not enough flights per weather bucket yet to compare."
    factor, label, gap, n = best
    if abs(gap) < 0.5:
        return (f"So what: weather barely moves the model's predicted delay in this slice "
                f"(largest gap {gap:+.1f} min, {factor.lower()} {label}).")
    direction = "later" if gap > 0 else "earlier"
    return (f"So what: flights arriving with {factor.lower()} {label} are predicted "
            f"{abs(gap):.1f} min {direction} than in clear conditions (n={n:,}).")


def render_weather_impact(df: pd.DataFrame):
    """Predicted delay by destination weather bucket."""
    st.subheader("Weather Impact on Predicted Delay")
    if df.empty or "predicted_delta_delay" not in df.columns:
        st.info("No Gold predictions yet. Run the stream, then reload.")
        return
    tbl = weather_impact_table(df)
    if tbl.empty:
        st.info("Gold has no weather columns yet (or too few flights per bucket).")
        return
    order = {f: l for f, _c, _e, l in WEATHER_FACTORS}
    fig = px.bar(
        tbl, x="bucket", y="avg_delta", facet_col="factor", color="avg_delta",
        color_continuous_scale="RdYlGn_r", color_continuous_midpoint=0,
        hover_data={"n": ":,", "high_risk_pct": ":.1f", "avg_delta": ":.2f"},
        labels={"bucket": "Destination weather at scheduled arrival",
                "avg_delta": "Avg predicted Δ delay (min)"},
        category_orders={"factor": [f for f, *_ in WEATHER_FACTORS if f in set(tbl["factor"])]},
    )
    fig.update_xaxes(matches=None, showticklabels=True)
    for ann in fig.layout.annotations:
        ann.text = ann.text.split("=")[-1]
    fig.update_layout(height=380, paper_bgcolor="rgba(0,0,0,0)", coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)
    st.caption(weather_so_what(tbl) +
               "  Values are model predictions (arrival delay minus departure delay), not observed delays.")


def lane_matrix(df: pd.DataFrame, top_n: int = 12):
    """Origin x destination matrices of avg predicted delta and flight count."""
    vol = pd.concat([df["origin"], df["dest"]]).value_counts()
    hubs = vol.index[:top_n].tolist()
    sub = df[df["origin"].isin(hubs) & df["dest"].isin(hubs)]
    g = (sub.groupby(["origin", "dest"])
         .agg(avg_delta=("predicted_delta_delay", "mean"), n=("predicted_delta_delay", "size"))
         .reset_index())
    g = g[g["n"] >= MIN_LANE_N]
    z = g.pivot(index="origin", columns="dest", values="avg_delta").reindex(index=hubs, columns=hubs)
    n = g.pivot(index="origin", columns="dest", values="n").reindex(index=hubs, columns=hubs)
    return z, n, g


def render_worst_lanes(df: pd.DataFrame):
    """Heatmap of avg predicted delay by lane (origin x destination)."""
    st.subheader("Worst Lanes")
    if df.empty or not {"origin", "dest", "predicted_delta_delay"} <= set(df.columns):
        st.info("No Gold predictions yet. Run the stream, then reload.")
        return
    z, n, g = lane_matrix(df)
    if g.empty:
        st.info(f"No lane has {MIN_LANE_N}+ flights yet.")
        return
    fig = go.Figure(go.Heatmap(
        z=z.values, x=z.columns, y=z.index, customdata=n.values,
        colorscale="RdYlGn_r", zmid=0,
        colorbar=dict(title="Avg Δ (min)"),
        hovertemplate="%{y} → %{x}<br>Avg predicted Δ: %{z:.1f} min<br>Flights: %{customdata:.0f}<extra></extra>",
    ))
    fig.update_layout(height=480, paper_bgcolor="rgba(0,0,0,0)",
                      xaxis_title="Destination", yaxis_title="Origin", yaxis_autorange="reversed")
    st.plotly_chart(fig, use_container_width=True)
    worst = g.sort_values("avg_delta", ascending=False).iloc[0]
    if worst["avg_delta"] < 0:
        effect = (f"is the lane predicted to make up the least time "
                  f"({abs(worst['avg_delta']):.1f} min better than departure delay)")
    else:
        effect = f"is the worst lane, predicted {worst['avg_delta']:.1f} min worse than departure delay"
    st.caption(
        f"So what: {worst['origin']} → {worst['dest']} {effect} over {int(worst['n']):,} flights. "
        f"Lanes with fewer than {MIN_LANE_N} flights are left blank. Predictions, not observed delays."
    )
    with st.expander("Top 5 worst lanes"):
        top = g.sort_values("avg_delta", ascending=False).head(5).round({"avg_delta": 1})
        st.dataframe(top.rename(columns={"avg_delta": "avg_predicted_delta_min", "n": "flights"}),
                     hide_index=True)


def benchmark_headline(agg: pd.DataFrame) -> str:
    """One sentence built only from measured numbers in the benchmark table."""
    best = agg.loc[agg["avg_throughput"].idxmax()]
    parts = [
        f"Across {len(agg)} configurations, the best ({best['config']}) sustained "
        f"{best['avg_throughput']:,.0f} records/s with p95 batch time {best['p95_batch_ms']:,.0f} ms"
    ]
    if pd.notna(best.get("p95_latency_ms")):
        parts.append(f"and median p95 end-to-end latency {best['p95_latency_ms']:,.0f} ms")
    parts.append(f"(mean across configs: {agg['avg_throughput'].mean():,.0f} records/s).")
    return " ".join(parts)


MODELS_DIR = PROJECT_ROOT / "models"
FEATURE_GROUPS = {
    "Departure delay": ["DepDelay"],
    "Schedule & time": ["DepHourUTC", "ArrHourUTC", "DayOfWeek"],
    "Lane & distance": ["LaneMeanDelay", "Distance"],
    "Destination weather": ["DestWindSpeed", "DestVisibility", "DestTemp", "DestCeiling"],
    "Origin weather": ["OriginWindSpeed", "OriginVisibility", "OriginCeiling", "OriginTemp"],
}


def feature_group_importance(importances: dict) -> pd.DataFrame:
    """Sum GBT feature importances into readable groups (unknown features go to 'Other')."""
    known = {f: g for g, fs in FEATURE_GROUPS.items() for f in fs}
    out = {}
    for feat, val in importances.items():
        g = known.get(feat, "Other")
        out[g] = out.get(g, 0.0) + float(val)
    return (pd.DataFrame({"group": list(out), "importance": list(out.values())})
            .sort_values("importance", ascending=False, ignore_index=True))


def hub_error_gain(hub_df: pd.DataFrame) -> pd.DataFrame:
    """Per hub: GBT MAE improvement over the lane-mean baseline, in minutes and percent."""
    d = hub_df.copy()
    d["gain_min"] = d["mae_lane_mean"] - d["mae_gbt"]
    d["gain_pct"] = 100 * d["gain_min"] / d["mae_lane_mean"]
    return d.sort_values("mae_gbt", ascending=False, ignore_index=True)


def error_so_what(hub_df: pd.DataFrame, bucket_df: pd.DataFrame) -> str:
    """One sentence on where the model is weakest, from the error tables."""
    parts = []
    if not hub_df.empty:
        worst = hub_df.loc[hub_df["mae_gbt"].idxmax()]
        best = hub_df.loc[hub_df["mae_gbt"].idxmin()]
        parts.append(f"Largest error is at {worst['hub']} (MAE {worst['mae_gbt']:.1f} min); "
                     f"smallest at {best['hub']} ({best['mae_gbt']:.1f} min).")
    if not bucket_df.empty:
        tail = bucket_df[bucket_df["bucket"] == "late >15 min"]
        if not tail.empty and tail["bias_gbt"].iloc[0] < 0:
            parts.append(f"Flights that turn out >15 min late are under-predicted by "
                         f"{abs(tail['bias_gbt'].iloc[0]):.1f} min on average, so big delays are underestimated.")
    return " ".join(parts) or "No error tables yet."


def _read_model_csv(name: str) -> pd.DataFrame:
    path = MODELS_DIR / name
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def render_model_insights():
    """Feature importance and error analysis from the batch-training outputs."""
    st.subheader("Model Insights")
    metrics_path = MODELS_DIR / "training_metrics.json"
    if metrics_path.exists():
        m = json.loads(metrics_path.read_text())
        grp = feature_group_importance(m.get("gbt_feature_importances", {}))
        fig = px.bar(grp, x="importance", y="group", orientation="h",
                     labels={"importance": "Share of GBT split importance", "group": ""})
        fig.update_layout(height=280, paper_bgcolor="rgba(0,0,0,0)", yaxis_autorange="reversed")
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Split-based importance favours continuous features, so it overstates weather. "
                   "The ablation in EVALUATION.md (weather adds about 1% MAE gain) is the reliable measure.")
    else:
        st.info("No training_metrics.json yet. Run `pipeline/01_batch_train.py`.")

    hub, hour = _read_model_csv("error_by_hub.csv"), _read_model_csv("error_by_hour.csv")
    month, bucket = _read_model_csv("error_by_month_hub.csv"), _read_model_csv("error_by_bucket.csv")
    if hub.empty:
        st.info("No error tables yet. Run `python pipeline/04_error_analysis.py` (Spark).")
        return

    st.markdown("**Where the model is wrong**")
    st.caption(error_so_what(hub, bucket))
    g = hub_error_gain(hub).melt(id_vars="hub", value_vars=["mae_gbt", "mae_lane_mean", "mae_no_change"],
                                 var_name="model", value_name="mae")
    fig = px.bar(g, x="hub", y="mae", color="model", barmode="group",
                 labels={"mae": "MAE (min)", "hub": "Origin hub"})
    fig.update_layout(height=340, paper_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig, use_container_width=True)

    if not hour.empty:
        fig = px.line(hour, x="DepHourUTC", y=["mae_gbt", "mae_lane_mean"], markers=True,
                      labels={"value": "MAE (min)", "DepHourUTC": "Wheels-off hour (UTC)"})
        fig.update_layout(height=300, paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, use_container_width=True)

    if not month.empty:
        z = month.pivot(index="hub", columns="Month", values="mean_actual")
        fig = go.Figure(go.Heatmap(z=z.values, x=z.columns, y=z.index, colorscale="RdYlGn_r", zmid=0,
                                   colorbar=dict(title="Avg actual Δ (min)")))
        fig.update_layout(height=460, paper_bgcolor="rgba(0,0,0,0)", xaxis_title="Month",
                          yaxis_title="Origin hub", yaxis_autorange="reversed")
        st.markdown("**Seasonality: average actual delta delay by hub and month (validation year)**")
        st.plotly_chart(fig, use_container_width=True)

    if not bucket.empty:
        with st.expander("Error by actual delay bucket"):
            st.dataframe(bucket.round(2), hide_index=True)


def render_system_health(benchmark_df: pd.DataFrame):
    """System metrics tiles."""
    st.subheader("System Health")

    cfg = load_config()
    gold_path = resolve_path(cfg["paths"]["gold_predictions"])

    # Count recent gold records
    recent_count = 0
    pq_files = list_parquet_files(gold_path) if gold_path.exists() else []
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

    st.caption(benchmark_headline(agg))

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
        st.plotly_chart(fig, width='stretch')

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
    tab_live, tab_replay, tab_model = st.tabs(["Live Radar", "Replay & Benchmark", "Model Insights"])

    with tab_live:
        render_live_radar(HUB_COORDS)

    with tab_replay:
        render_fleet_map(flights_df)
        st.divider()
        render_hub_pressure(congestion_df)
        st.divider()
        analytics_df = read_gold_analytics()
        render_weather_impact(analytics_df)
        st.divider()
        render_worst_lanes(analytics_df)
        st.divider()
        render_system_health(benchmark_df)
        st.divider()
        render_benchmark(benchmark_df)

    with tab_model:
        render_model_insights()

    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()


if __name__ == "__main__":
    main()
