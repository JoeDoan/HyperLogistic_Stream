"""
dashboard/live_radar.py
"Live Radar" tab for the HyperLogistics-Stream dashboard (Plan V2).

Shows which US hubs are under stress RIGHT NOW by combining three live feeds:
  * NWS weather      -> flight category (VFR / MVFR / IFR / LIFR) + weather severity (0-3)
  * FAA NAS status   -> ground stops / ground delays / delays (severity 0-3)
  * OpenSky aircraft -> airborne aircraft within ~100 km of each hub

Data source priority (so the tab works with or without Kafka/Spark):
  1. Spark Gold live tables  data/gold/live/{weather,airport_status,hub_traffic}
  2. Latest recorded session data/live_recordings/*.jsonl  (live_producer.py --record)
  3. Bundled sample session  data/sample/live_session_*.jsonl

Honest limits (also shown in the UI): OpenSky has no delay minutes, so "stress" is
FAA severity + weather severity, not a measured delay. Flights are a public proxy
for hub-to-hub freight.
"""

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent.parent
GOLD_LIVE = PROJECT_ROOT / "data" / "gold" / "live"
RECORD_DIR = PROJECT_ROOT / "data" / "live_recordings"
SAMPLE_DIR = PROJECT_ROOT / "data" / "sample"
BASELINE_CSV = PROJECT_ROOT / "models" / "baseline_hub_hour_dow.csv"  # written by 01_batch_train.py

AIRPORT_TZ = {
    'ATL': 'America/New_York', 'ORD': 'America/Chicago', 'DFW': 'America/Chicago',
    'DEN': 'America/Denver', 'LAX': 'America/Los_Angeles', 'JFK': 'America/New_York',
    'EWR': 'America/New_York', 'SFO': 'America/Los_Angeles', 'MEM': 'America/Chicago',
    'SDF': 'America/New_York', 'CVG': 'America/New_York', 'CLT': 'America/New_York',
    'LAS': 'America/Los_Angeles', 'PHX': 'America/Phoenix', 'MIA': 'America/New_York',
    'IAH': 'America/Chicago', 'BOS': 'America/New_York', 'SEA': 'America/Los_Angeles',
    'MSP': 'America/Chicago', 'DTW': 'America/New_York', 'PHL': 'America/New_York',
    'BWI': 'America/New_York', 'SLC': 'America/Denver', 'MCO': 'America/New_York',
}

RADIUS_KM = 100.0
STALE_AFTER_S = 180          # data older than this is shown as STALE
FAA_MAX_AGE_S = 600          # ignore FAA rows older than this

STRESS_LABEL = {0: "Calm", 1: "Calm", 2: "Watch", 3: "Strained", 4: "Strained", 5: "Severe", 6: "Severe"}
STRESS_COLOR = {0: "#10b981", 1: "#10b981", 2: "#fbbf24", 3: "#f97316", 4: "#f97316", 5: "#dc2626", 6: "#dc2626"}


# ----------------------------- enrichment (pandas) -----------------------------

def _enrich_weather(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "flight_category" in df.columns:
        return df
    df = df.copy()
    vis_mi = pd.to_numeric(df["visibility_m"], errors="coerce") / 1609.34
    ceil_ft = pd.to_numeric(df["ceiling_m"], errors="coerce") * 3.281
    gust = pd.to_numeric(df.get("wind_gust_kmh"), errors="coerce").fillna(
        pd.to_numeric(df.get("wind_kmh"), errors="coerce")).fillna(0.0)

    cat = np.select(
        [(vis_mi < 1) | (ceil_ft < 500), (vis_mi < 3) | (ceil_ft < 1000),
         (vis_mi <= 5) | (ceil_ft <= 3000)],
        ["LIFR", "IFR", "MVFR"], default="VFR")
    df["flight_category"] = cat
    base = pd.Series(cat).map({"LIFR": 3, "IFR": 2, "MVFR": 1, "VFR": 0}).to_numpy()
    df["weather_severity"] = np.minimum(3, base + (gust.to_numpy() >= 50).astype(int))
    return df


def _enrich_faa(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "delay_severity" in df.columns:
        return df
    df = df.copy()
    t = df["delay_type"].fillna("").str.lower()
    sev = np.select(
        [t.str.contains("closure"), t.str.contains("ground stop"), t.str.contains("ground delay"),
         t.str.contains("arrival") | t.str.contains("departure") | t.str.contains("delay")],
        [0, 3, 2, 1], default=0)
    df["delay_severity"] = sev
    avg = df["avg"] if "avg" in df.columns else pd.Series([""] * len(df), index=df.index)
    df["avg_delay_min"] = pd.to_numeric(avg.astype(str).str.extract(r"(\d+)")[0], errors="coerce")
    return df


def _traffic_from_flights(df: pd.DataFrame, hub_coords: dict) -> pd.DataFrame:
    """Aircraft within RADIUS_KM of each hub (vectorized haversine)."""
    if df.empty:
        return pd.DataFrame()
    ac = df[(df["on_ground"] == False) & df["lat"].notna() & df["lon"].notna()]  # noqa: E712
    if ac.empty:
        return pd.DataFrame()
    lat = np.radians(ac["lat"].to_numpy(dtype=float))[:, None]
    lon = np.radians(ac["lon"].to_numpy(dtype=float))[:, None]
    alt = pd.to_numeric(ac["baro_altitude"], errors="coerce").to_numpy()
    vr = pd.to_numeric(ac["vertical_rate"], errors="coerce").to_numpy()

    rows = []
    for code, (hlat, hlon, *_rest) in hub_coords.items():
        dlat = np.radians(hlat) - lat[:, 0]
        dlon = np.radians(hlon) - lon[:, 0]
        a = np.sin(dlat / 2) ** 2 + np.cos(lat[:, 0]) * np.cos(np.radians(hlat)) * np.sin(dlon / 2) ** 2
        dist = 2 * 6371.0 * np.arcsin(np.sqrt(a))
        m = dist <= RADIUS_KM
        rows.append({
            "airport": code,
            "aircraft_nearby": int(m.sum()),
            "low_altitude_count": int(((alt < 3000) & m).sum()),
            "descending_count": int(((vr < -1.0) & m).sum()),
        })
    out = pd.DataFrame(rows)
    out["stale"] = bool(ac["stale"].any()) if "stale" in ac.columns else False
    out["fetched_at"] = ac["fetched_at"].max() if "fetched_at" in ac.columns else None
    return out


# ------------------------------- loading --------------------------------------

def _parquet_files(base: Path) -> list:
    files = []
    if not base.exists():
        return files
    for p in base.rglob("*.parquet"):
        if any(part.startswith(("_", ".")) for part in p.relative_to(base).parts):
            continue
        try:
            files.append((p.stat().st_mtime, p))
        except FileNotFoundError:
            continue
    return [p for _, p in sorted(files, key=lambda x: x[0], reverse=True)]


def _read_gold(name: str, max_files: int = 30) -> pd.DataFrame:
    frames = []
    for pf in _parquet_files(GOLD_LIVE / name)[:max_files]:
        try:
            frames.append(pd.read_parquet(pf))
        except Exception:
            pass
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _find_recording_file(
    record_dir: Path | None = None,
    sample_dir: Path | None = None,
) -> tuple[Path | None, str]:
    rdir = RECORD_DIR if record_dir is None else record_dir
    sdir = SAMPLE_DIR if sample_dir is None else sample_dir
    for folder, label in ((rdir, "Recorded live session"), (sdir, "Sample recorded session")):
        if folder.exists():
            files = sorted(folder.glob("live_session_*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)
            if files:
                return files[0], label
    return None, "none"


def _read_latest_recording(
    record_dir: Path | None = None,
    sample_dir: Path | None = None,
) -> tuple[dict, str]:
    """Return ({topic: DataFrame}, source_label) from the newest recorded or sample session."""
    fpath, label = _find_recording_file(record_dir=record_dir, sample_dir=sample_dir)
    if fpath is None:
        return {}, "none"
    buckets: dict = {}
    with open(fpath, encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
                buckets.setdefault(rec["topic"], []).append(rec["value"])
            except Exception:
                continue
    if not buckets:
        return {}, "none"
    return {t: pd.DataFrame(v) for t, v in buckets.items()}, label


def _latest_by(df: pd.DataFrame, key: str, ts: str = "fetched_at") -> pd.DataFrame:
    if df.empty or key not in df.columns:
        return df
    if ts not in df.columns:
        return df.drop_duplicates(key, keep="last")
    return df.sort_values(ts).drop_duplicates(key, keep="last")


def _latest_snapshot(df: pd.DataFrame, ts: str = "fetched_at") -> pd.DataFrame:
    """Rows from the most recent fetch only."""
    if df.empty or ts not in df.columns:
        return df
    return df[df[ts] == df[ts].max()]


def _max_fetched_utc(*dfs: pd.DataFrame) -> datetime | None:
    best: datetime | None = None
    for df in dfs:
        if df is None or df.empty or "fetched_at" not in df.columns:
            continue
        ts = pd.to_datetime(df["fetched_at"], utc=True, errors="coerce").max()
        if pd.isna(ts):
            continue
        dt = ts.to_pydatetime().astimezone(timezone.utc)
        if best is None or dt > best:
            best = dt
    return best


def _any_truthy(series: pd.Series) -> bool:
    return any(bool(v) for v in series if pd.notna(v))


def prepare_live_feeds(
    weather: pd.DataFrame,
    faa: pd.DataFrame,
    traffic: pd.DataFrame,
    source: str,
    now_utc: datetime | None = None,
) -> dict:
    now_utc = now_utc or datetime.now(timezone.utc)
    is_recorded = source in ("Recorded live session", "Sample recorded session")
    ref_utc = (_max_fetched_utc(weather, faa, traffic) or now_utc) if is_recorded else now_utc

    weather = _latest_by(_enrich_weather(weather), "airport")
    faa = _enrich_faa(faa)
    traffic = _latest_by(traffic, "airport", "fetched_at") if not traffic.empty else traffic

    feed_stale = {"weather": False, "faa": False, "traffic": False}
    if not is_recorded:
        for feed_name, feed_df in (("weather", weather), ("traffic", traffic)):
            if feed_df.empty:
                continue
            flagged = _any_truthy(feed_df["stale"]) if "stale" in feed_df.columns else False
            feed_ts = _max_fetched_utc(feed_df)
            aged_out = (ref_utc - feed_ts).total_seconds() > STALE_AFTER_S if feed_ts is not None else False
            feed_stale[feed_name] = flagged or aged_out

    faa_latest = _latest_snapshot(faa)
    if not faa_latest.empty and "fetched_at" in faa_latest.columns:
        faa_ts = _max_fetched_utc(faa_latest)
        faa_age = (ref_utc - faa_ts).total_seconds() if faa_ts is not None else 0.0
        faa_flagged_stale = _any_truthy(faa_latest["stale"]) if "stale" in faa_latest.columns else False
        if is_recorded:
            age_vs_ref = (
                ref_utc - pd.to_datetime(faa_latest["fetched_at"], utc=True, errors="coerce")
            ).dt.total_seconds()
            faa_latest = faa_latest[age_vs_ref <= FAA_MAX_AGE_S]
        else:
            if faa_flagged_stale:
                feed_stale["faa"] = True
            elif faa_age > FAA_MAX_AGE_S:
                feed_stale["faa"] = True
                faa_latest = faa_latest.iloc[0:0]
    faa = faa_latest

    return {
        "source": source,
        "weather": weather,
        "faa": faa,
        "traffic": traffic,
        "reference_utc": ref_utc,
        "feed_stale": feed_stale,
        "is_recorded": is_recorded,
    }


@st.cache_data(ttl=10)
def load_live(hub_coords: dict) -> dict:
    weather = _read_gold("weather")
    faa = _read_gold("airport_status")
    traffic = _read_gold("hub_traffic")
    source = "Spark Gold (live)"

    if weather.empty and faa.empty and traffic.empty:
        rec, rec_source = _read_latest_recording()
        if not rec:
            return {
                "source": "none",
                "weather": pd.DataFrame(),
                "faa": pd.DataFrame(),
                "traffic": pd.DataFrame(),
                "reference_utc": None,
                "feed_stale": {"weather": False, "faa": False, "traffic": False},
                "is_recorded": False,
            }
        weather = rec.get("weather", pd.DataFrame())
        faa = rec.get("airport_status", pd.DataFrame())
        flights = _latest_snapshot(rec.get("live_flights", pd.DataFrame()))
        traffic = _traffic_from_flights(flights, hub_coords)
        source = rec_source

    return prepare_live_feeds(weather, faa, traffic, source)


# ------------------------------- hub state -------------------------------------

@st.cache_data(ttl=300)
def _load_baseline_csv() -> pd.DataFrame:
    if not BASELINE_CSV.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(BASELINE_CSV)
    except Exception:
        return pd.DataFrame()


def _merge_baseline_csv(hubs: pd.DataFrame, reference_utc: datetime | None = None) -> pd.DataFrame:
    """Attach typical delay for each hub's local hour and weekday (Spark dow: Sun=1)."""
    base = _load_baseline_csv()
    if base.empty:
        return hubs
    now_utc = reference_utc or datetime.now(timezone.utc)
    keys = []
    for code in hubs["airport"]:
        local = now_utc.astimezone(ZoneInfo(AIRPORT_TZ.get(code, "UTC")))
        keys.append({"airport": code, "hour_local": local.hour,
                     "day_of_week": (local.weekday() + 1) % 7 + 1})
    k = pd.DataFrame(keys).merge(
        base.rename(columns={"hub": "airport"}),
        on=["airport", "hour_local", "day_of_week"], how="left")
    dep = pd.to_numeric(k.get("typical_dep_delay"), errors="coerce")
    arr = pd.to_numeric(k.get("typical_arr_delay"), errors="coerce")
    k["typical_delay_min"] = pd.concat([dep, arr], axis=1).mean(axis=1)
    hubs = hubs.drop(columns=[c for c in ["typical_delay_min"] if c in hubs.columns])
    return hubs.merge(k[["airport", "typical_delay_min"]], on="airport", how="left")


def build_hub_state(data: dict, hub_coords: dict) -> pd.DataFrame:
    hubs = pd.DataFrame([{"airport": k, "lat": v[0], "lon": v[1], "name": v[2]} for k, v in hub_coords.items()])
    w, f, t = data["weather"], data["faa"], data["traffic"]

    if not w.empty:
        wcols = [c for c in ["airport", "flight_category", "weather_severity", "description", "wind_kmh",
                             "wind_gust_kmh", "visibility_m", "ceiling_m", "stale", "fetched_at",
                             "typical_delay_min"] if c in w.columns]
        hubs = hubs.merge(w[wcols], on="airport", how="left")
    if not f.empty:
        act = f[f["delay_severity"] > 0].copy()
        if not act.empty:
            act["_prog"] = act["delay_type"].fillna("") + np.where(
                act["reason"].notna(), " (" + act["reason"].astype(str) + ")", "")
            g = act.groupby("airport").agg(
                faa_severity=("delay_severity", "max"),
                faa_programs=("_prog", lambda s: "; ".join(sorted(set(s)))),
                faa_avg_delay_min=("avg_delay_min", "max"),
            ).reset_index()
            hubs = hubs.merge(g, on="airport", how="left")
    if not t.empty:
        tcols = [c for c in ["airport", "aircraft_nearby", "low_altitude_count", "descending_count"] if c in t.columns]
        hubs = hubs.merge(t[tcols], on="airport", how="left")

    for col, default in [("weather_severity", 0), ("faa_severity", 0), ("aircraft_nearby", 0),
                         ("low_altitude_count", 0), ("descending_count", 0)]:
        if col not in hubs.columns:
            hubs[col] = default
        hubs[col] = pd.to_numeric(hubs[col], errors="coerce").fillna(default)
    for col in ["flight_category", "faa_programs", "description"]:
        if col not in hubs.columns:
            hubs[col] = None

    if data.get("feed_stale", {}).get("faa") and "faa_programs" in hubs.columns:
        mask = hubs["faa_programs"].notna() & (hubs["faa_programs"].astype(str).str.len() > 0)
        hubs.loc[mask, "faa_programs"] = hubs.loc[mask, "faa_programs"].astype(str).map(
            lambda s: s if s.endswith(" [stale]") else f"{s} [stale]"
        )

    # Historical baseline (typical delay for this hub, local hour and weekday) from 01_batch_train.py.
    if "typical_delay_min" not in hubs.columns or hubs["typical_delay_min"].isna().all():
        hubs = _merge_baseline_csv(hubs, reference_utc=data.get("reference_utc"))
    if "typical_delay_min" not in hubs.columns:
        hubs["typical_delay_min"] = np.nan

    hubs["stress"] = (hubs["weather_severity"] + hubs["faa_severity"]).clip(0, 6).astype(int)
    hubs["stress_label"] = hubs["stress"].map(STRESS_LABEL)
    hubs["color"] = hubs["stress"].map(STRESS_COLOR)
    return hubs.sort_values(["stress", "aircraft_nearby"], ascending=False).reset_index(drop=True)


def _story_line(r: pd.Series) -> str:
    parts = []
    if isinstance(r["faa_programs"], str) and r["faa_programs"]:
        parts.append(f"FAA: {r['faa_programs']}")
    if isinstance(r["flight_category"], str) and r["flight_category"] != "VFR":
        bits = [r["flight_category"]]
        if pd.notna(r.get("ceiling_m")):
            bits.append(f"ceiling {r['ceiling_m'] * 3.281:,.0f} ft")
        if pd.notna(r.get("visibility_m")):
            bits.append(f"visibility {r['visibility_m'] / 1609.34:.1f} mi")
        parts.append("weather " + ", ".join(bits))
    elif pd.notna(r.get("wind_gust_kmh")) and r["wind_gust_kmh"] >= 50:
        parts.append(f"gusts {r['wind_gust_kmh']:.0f} km/h")
    if r["aircraft_nearby"]:
        parts.append(f"{int(r['aircraft_nearby'])} aircraft within {int(RADIUS_KM)} km")
    if pd.notna(r.get("typical_delay_min")):
        typ = f"typical delay now ≈ {r['typical_delay_min']:.0f} min"
        faa_avg = r.get("faa_avg_delay_min")
        if pd.notna(faa_avg) and faa_avg > 0:
            typ += f" · FAA reports ~{faa_avg:.0f} min ({faa_avg - r['typical_delay_min']:+.0f} vs typical)"
        parts.append(typ)
    return " · ".join(parts) if parts else "No active issues."


# --------------------------------- UI ------------------------------------------

def compute_status_badge(
    hubs: pd.DataFrame,
    data: dict | str,
    now_utc: datetime | None = None,
) -> tuple[str, str, str]:
    if isinstance(data, str):
        data = {"source": data}
    source = data.get("source", "Spark Gold (live)")
    feed_stale = data.get("feed_stale") or {}
    stale_feeds = [name for name in ("weather", "faa", "traffic") if feed_stale.get(name)]

    stale_flag = _any_truthy(hubs["stale"]) if "stale" in hubs.columns else False
    newest = _max_fetched_utc(hubs, data.get("weather"), data.get("faa"), data.get("traffic"))
    now_val = now_utc or datetime.now(timezone.utc)
    age = (now_val - newest).total_seconds() if newest is not None else None
    is_stale = bool(stale_feeds) or stale_flag or (age is not None and age > STALE_AFTER_S)

    if source == "Sample recorded session":
        label, color = "SAMPLE", "#6366f1"
    elif source == "Recorded live session":
        label, color = "RECORDED", "#6366f1"
    elif is_stale:
        label, color = "STALE", "#f97316"
    else:
        label, color = "LIVE", "#10b981"

    if source in ("Recorded live session", "Sample recorded session"):
        age_txt = f"last data {newest:%b %d %H:%M} UTC" if newest is not None else "no timestamp"
    else:
        age_txt = f"updated {int(age)}s ago" if age is not None and age < 3600 else (
            f"last data {newest:%b %d %H:%M} UTC" if newest is not None else "no timestamp")
        if stale_feeds:
            age_txt = f"{age_txt} · stale feeds: {', '.join(stale_feeds)}"
    return label, color, f"{source} · {age_txt}"


def _status_badge(hubs: pd.DataFrame, data: dict):
    label, color, subtitle = compute_status_badge(hubs, data)
    st.markdown(
        f"<span style='background:{color};color:white;padding:4px 12px;border-radius:12px;"
        f"font-weight:700;letter-spacing:1px'>{label}</span> "
        f"<span style='color:#9ca3af;margin-left:8px'>{subtitle}</span>",
        unsafe_allow_html=True)


def _render_map(hubs: pd.DataFrame):
    size = 10 + 30 * (hubs["aircraft_nearby"] / max(1, hubs["aircraft_nearby"].max()))
    hover = hubs.apply(lambda r: f"<b>{r['airport']}</b> · {r['name']}<br>{r['stress_label']} "
                                 f"(stress {r['stress']}/6)<br>{_story_line(r)}", axis=1)
    fig = go.Figure(go.Scattergeo(
        lon=hubs["lon"], lat=hubs["lat"], mode="markers+text",
        text=hubs["airport"], textposition="top center", textfont=dict(color="white", size=10),
        marker=dict(size=size, color=hubs["color"], line=dict(width=1, color="white"), opacity=0.9),
        hovertext=hover, hoverinfo="text",
    ))
    fig.update_layout(
        geo=dict(scope="usa", showland=True, landcolor="#1a1a2e", bgcolor="#0d1117",
                 showlakes=False, subunitcolor="#374151"),
        paper_bgcolor="#0d1117", font_color="white", height=470,
        margin=dict(l=0, r=0, t=0, b=0),
    )
    st.plotly_chart(fig, use_container_width=True)
    st.caption("Circle color = hub stress (weather severity + FAA severity). "
               "Circle size = aircraft within 100 km right now.")


def render_live_radar(hub_coords: dict):
    st.subheader("Live Radar — which hubs are under stress right now?")

    data = load_live(hub_coords)
    if data["source"] == "none":
        st.info(
            "No live data yet. Start the feed:\n\n"
            "`python live/live_producer.py --record`\n\n"
            "Then (optional, needs Kafka + Spark) `python pipeline/04_live_processor.py`. "
            "The tab also works from a recorded session without Kafka."
        )
        return

    hubs = build_hub_state(data, hub_coords)
    _status_badge(hubs, data)
    st.write("")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Hubs under stress (≥3)", int((hubs["stress"] >= 3).sum()))
    c2.metric("Active FAA programs", int((hubs["faa_severity"] > 0).sum()),
              help="Ground stops, ground delays and arrival/departure delays at tracked hubs")
    c3.metric("Non-VFR weather hubs", int((hubs["flight_category"].isin(["MVFR", "IFR", "LIFR"])).sum()))
    c4.metric("Aircraft near hubs", f"{int(hubs['aircraft_nearby'].sum()):,}")

    _render_map(hubs)

    st.markdown("#### Today's story")
    worst = hubs[hubs["stress"] > 0].head(5)
    if worst.empty:
        st.success("All tracked hubs look calm right now. No FAA programs and no weather restrictions.")
    else:
        for _, r in worst.iterrows():
            icon = "🔴" if r["stress"] >= 5 else ("🟠" if r["stress"] >= 3 else "🟡")
            st.markdown(f"{icon} **{r['airport']}** — {r['name']} · *{r['stress_label']}*  \n"
                        f"&nbsp;&nbsp;&nbsp;&nbsp;{_story_line(r)}")
        st.caption("So what: hubs at the top are the most likely to cause knock-on delays for "
                   "connecting flights and freight handoffs in the next few hours.")

    if hubs["typical_delay_min"].notna().any():
        st.caption("Baseline loaded: typical delay for each hub's local hour and weekday (training years only).")
    else:
        st.caption("Baseline not loaded yet: stress uses absolute severity, not the typical day. "
                   "Run `pipeline/01_batch_train.py` to build `models/baseline_hub_hour_dow.csv`.")

    with st.expander("All hubs"):
        cols = [c for c in ["airport", "name", "stress", "stress_label", "flight_category", "weather_severity",
                            "faa_severity", "faa_programs", "aircraft_nearby", "low_altitude_count",
                            "descending_count"] if c in hubs.columns]
        st.dataframe(hubs[cols], use_container_width=True, hide_index=True)

    st.caption("Limits: OpenSky reports positions, not delay minutes, so stress is FAA + weather severity, "
               "not a measured delay. Flights are a public proxy for hub-to-hub freight.")
