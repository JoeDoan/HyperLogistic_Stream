# Plan options (flight data only, built around storytelling)

The professor's advice changes the focus. The pipeline stays, but the product becomes **"data → insight → story."** Each path below keeps the Kafka + Spark engine.

## Path comparison

| | **Path 1: Delay Story Dashboard** | **Path 2: Live Airport Weather and Delay Radar** | **Path 3: Storyteller Agent** |
|---|---|---|---|
| **Pitch** | "Why do flights get delayed, and what will happen next?" | "A live map of US hubs under stress right now" | "Ask the data a question and get a plain-English story" |
| **Data** | Existing BTS flight data (static replay) | Live feeds (OpenSky, FAA, NWS weather) plus the BTS baseline | Existing data plus the agent from your plan |
| **Data Science depth** | Medium: ablations, error analysis, lane and hub insights | High: weather and live-vs-history features, drift checks | Medium-high: tool-using LLM with guardrails |
| **Wow factor** | Medium | **High** | High |
| **Risk** | Low | Medium (API limits and outages) | Medium (hallucination) |
| **Effort** | ~35 hrs | ~50 hrs | ~45 hrs |
| **Best for** | The safest finish | The best demo | An AI-track edge |

**My recommendation:** do **Path 1 as the base** (it is mostly done), then add **Path 2's live feed** as the highlight. Add Path 3 only if time remains.

## What each path delivers

### Path 1: Delay Story Dashboard
1. **Insight cards** in plain English, for example "ORD delays are 2.3× worse on Friday evenings."
2. **Story tabs:** worst lanes, best and worst hours, a seasonal heatmap, a hub ranking.
3. **Model explainability:** which features drive delay (SHAP or feature importance).
4. **Error analysis:** where the model fails, and why.
5. **A "so what" line** under each chart.

### Path 2: Live Radar (Path 1 + live feed)
1. Everything in Path 1.
2. A live flight feed into Kafka.
3. A weather join (METAR/NWS) as a model feature.
4. Live alerts, such as "DFW is trending 18 min worse than normal."
5. A **historical vs. live** comparison, which is the strongest story: "Today vs. a typical day."

### Path 3: Storyteller Agent
1. Everything in Path 1.
2. Agent tools: `hub_status`, `lane_delay`, `compare_to_history`.
3. A daily auto-generated "Delay Briefing" for viewers.
4. An evaluation table for tool-choice accuracy and refusals.

---

# Can the data go live? Yes.

Your data is static, but you can make it live in two ways.

## Option A: True live data (recommended for the wow factor)

**Free sources:**

| Source | Gives you | Notes |
|---|---|---|
| **OpenSky Network API** | Live aircraft positions and states | Free. Anonymous access is rate-limited (~10 s polls); a free account gives more |
| **FAA NAS Status** (`nasstatus.faa.gov`) | Real-time airport delays and ground stops | Free, very good for stories |
| **NWS API** (`api.weather.gov`) | Live weather and METAR per airport | Free, no key needed |
| AviationStack / AeroAPI | Scheduled vs. actual flight times | Free tiers are tiny. Use only as a backup |

> [!WARNING]
> OpenSky gives **positions, not delay minutes**. The delay story comes from combining it with the FAA status, schedules and your model. Check the current rate limits before you build, because they change.

**Steps:**
1. **Prototype the fetch** (2–3 hrs): a Python script that polls OpenSky for US airspace and prints rows.
2. **Create a Kafka producer** (3–4 hrs): `live_producer.py` polls every 15–30 s and publishes to a `live_flights` topic. Use the same JSON schema as your replay producer where possible.
3. **Add an FAA + weather poller** (3 hrs): publish airport status and METAR to `airport_status` and `weather` topics.
4. **Update Spark** (4–6 hrs): add a stream reading `live_flights`, join it with weather and the lane baseline, and write to Gold.
5. **Handle missing data** (2 hrs): when an API fails, fall back to the last known value and show a "stale data" badge.
6. **Update the dashboard** (3–4 hrs): add a "LIVE" indicator, a last-updated time, and a toggle between **Live** and **Replay**.
7. **Add a safety net** (1 hr): cache a recorded live session so you can replay it if the API fails during the demo.

## Option B: Simulated live (the safe fallback)
Your replay already behaves as a live stream. Make it more convincing:
- Replay the **same calendar day from the past**, in real time.
- Show a "Replaying: Dec 23, 2023 (winter storm day)" label.
- Pick **famous disruption days** (storms, the Southwest meltdown) and tell the story.

This is honest, reliable and demo-proof. Label it as replay, not live.

## Recommended combination
**Option A with Option B as the fallback.** Record live data while you build, so that if the API fails on demo day, you replay your own recorded live session.
