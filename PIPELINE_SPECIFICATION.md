# HyperLogistics-Stream: End-to-End System Pipeline & Technical Specification

**Project:** Real-Time Hub-to-Hub Logistics ETA Prediction with Kafka and Spark Structured Streaming  
**Course:** COMP-SCI 5540 Principles of Big Data Management, Fall 2026, UMKC  
**Authors:** Tony Nguyen (`tmnc62@umsystem.edu`) & Joe Doan (`jdbc6@umsystem.edu`)  
**Instructor:** Dr. Md Yusuf Sarwar Uddin  

---

## 1. System Architecture Diagram

```
[ 1. OFFLINE BATCH TRAINING (pipeline/01_batch_train.py) ]
  BTS (2022-2023) + NOAA ISD Weather ──► Data Cleaning & UTC Alignment ──► Feature Vector ──► Spark MLlib Train ──► Serialized GBT / RF Model
                                                                                                                   │
                                                                                                                   │ (Loaded into Driver & Executors)
[ 2. DUAL INGESTION LAYER ]                                                                                        ▼
  (A) Historical Replay (pipeline/02_kafka_producer.py) ──► Kafka Topic: flights_stream (1k/5k/20k eps) ──┐
  (B) Live Multi-Source Feeds (live/live_producer.py)                                                     │
      ├── OpenSky Network (Aircraft Positions)  ────────► Kafka Topic: live_flights (30s)                │
      ├── FAA NAS Status (Ground Stops & Delays) ────────► Kafka Topic: airport_status (60s)              │
      └── NWS API (METAR Surface Weather)        ────────► Kafka Topic: weather (300s)                   ├──► Apache Kafka
                                                                                                          │    (Port 9092 / 9094)
                                                                                                          │
[ 3. STREAM PROCESSING ENGINES (PySpark Structured Streaming) ]                                           │
  (A) Replay Engine (pipeline/03_stream_processor.py) ◄───────────────────────────────────────────────────┤
      Broadcast Join with Static NOAA Table ──► VectorAssembler ──► MLlib .transform() ──► Gold Output    │
  (B) Live Processor (pipeline/04_live_processor.py) ◄────────────────────────────────────────────────────┘
      Spatial Radius Join (Haversine km) ──► Multi-Topic Fusion ──► Live Hub Stress Table
                                    │
                                    ▼
[ 4. MEDALLION STORAGE LAYER (Snappy Parquet) ]
  ├── Bronze: Raw Kafka event stream dumps with checkpointing
  ├── Silver: Cleaned & weather-joined records
  └── Gold:
      ├── Scored dynamic predictions: data/gold/predictions/
      ├── Rolling window hub congestion: data/gold/hub_congestion/
      └── Real-time live hub status: data/gold/live_hub_status/
                                    │
                                    ▼
[ 5. SERVING, OBSERVABILITY & BENCHMARKS ]
  ├── FastAPI Microservice (api/main.py) ───────► REST Endpoints (/flights/active, /hubs/{hub}/congestion)
  ├── Streamlit Dashboard (dashboard/app.py) ──► Tabs: Live Radar (live_radar.py) | Replay & Benchmark | Story
  └── Benchmark Suite (benchmark/run_benchmark.py, analyze_results.py) ──► RQ2 Factorial Matrix & Plots
```

---

## 2. Phase 1: Offline Batch Pipeline (Data Preparation & Model Training)

Implemented in [`pipeline/01_batch_train.py`](pipeline/01_batch_train.py).

### 2.1. Raw Data Extraction (~11 GB Total)
* **BTS Reporting Carrier On-Time Performance (2022–2023, ~6.5 GB):**
  * Historical flight departure and arrival records for training.
  * Year 2024 (~3.3 GB) is strictly held out as the streaming replay evaluation partition.
* **NOAA Integrated Surface Database (ISD) (2022–2024, ~911 MB):**
  * Hourly weather readings for destination airports (temperature, visibility, wind speed).

### 2.2. Cleaning & Standardization
* Filter cancelled and diverted flights (`Cancelled == 0`, `Diverted == 0`).
* Convert local wall-clock times (`FlightDate`, `CRSDepTime`, `WheelsOff`, `CRSArrTime`) to universal UTC timestamps using airport IANA timezone mappings.
* Clean and extract numeric metrics from NOAA ISD compound strings:
  * `WND`: Wind direction (degrees) and speed (converted to m/s).
  * `VIS`: Visibility distance in meters.
  * `TMP`: Air temperature in degrees Celsius.

### 2.3. Feature Engineering & Formulation
* **Target Label Formulation:**
  $$\Delta\text{delay} = \text{ArrDelay} - \text{DepDelay}$$
  *Rationale:* Predicting delay change forces the model to learn airborne transit dynamics rather than trivial identity mappings.
* **Dynamic ETA Formulation:**
  $$\text{Predicted\_ETA} = \text{CRSArrTime} + \text{DepDelay} + \text{Predicted\_}\Delta\text{delay}$$
* **Feature Vector ($X$):**
  * `DepDelay`: Known departure delay at wheels-off.
  * `Distance`: Great-circle distance between hub pairs.
  * `DepHourUTC`, `ArrHourUTC`: Hour-of-day cyclics in UTC.
  * `DayOfWeek`: Integer (1 to 7).
  * `DestWindSpeed`, `DestVisibility`, `DestTemp`: Destination surface weather at scheduled arrival hour.
  * `LaneMeanDelay`: 30-day historical rolling mean delay for the specific Origin-Destination lane.

### 2.4. Offline Model Training & Serialization
* **Algorithms:** Spark MLlib `GBTRegressor` (primary) and `RandomForestRegressor` (baseline).
* **Pipeline:** `StringIndexer` $\rightarrow$ `OneHotEncoder` $\rightarrow$ `VectorAssembler` $\rightarrow$ `GBTRegressor`.
* **Evaluation:** Hold-out evaluation measuring MAE and RMSE against:
  1. *Baseline 1:* Delay unchanged ($\Delta\text{delay} = 0$).
  2. *Baseline 2:* Historical lane mean.
* **Model Artifact:** Serialized `PipelineModel` stored in `models/gbt_pipeline_v1` (with metadata in `models/training_summary.json`).

---

## 3. Phase 2: Ingestion Layer (Kafka Dual Ingestion Engine)

### 3.1. Mode A: Historical Replay Producer
Implemented in [`pipeline/02_kafka_producer.py`](pipeline/02_kafka_producer.py).
* **Input Source:** Chronologically sorted BTS 2024 flight departure records.
* **Replay Mechanics:** Emits events strictly aligned with historical `WheelsOff` milestone order.
* **Throughput Throttling:** Configurable emission rates (1,000, 5,000, 20,000 events/second) via high-precision token bucket pacing.
* **Target Topic:** `flights_stream` (8 partitions, key: `origin`).

### 3.2. Mode B: Live Multi-Source Feed Producer (Plan V2)
Implemented in [`live/live_producer.py`](live/live_producer.py).
* Coordinates three real-time asynchronous fetchers:
  1. **OpenSky Network** ([`live/fetch_opensky.py`](live/fetch_opensky.py)): Polled every 30 s for live transponder positions, altitudes, and headings across US airspace (`live_flights` topic). Includes exponential backoff for public rate-limit handling.
  2. **FAA NAS Status** ([`live/fetch_faa_nas.py`](live/fetch_faa_nas.py)): Polled every 60 s for official airport ground stops, ground delay programs, and arrival closures (`airport_status` topic).
  3. **NWS API** ([`live/fetch_nws_weather.py`](live/fetch_nws_weather.py)): Polled every 5 min for METAR surface observations (wind speed/gusts, visibility, cloud ceiling, temperature) for key hubs (`weather` topic).
* **Resilience Safeguards:**
  * **Stale-Data Fallback:** Automatically caches last-known values with freshness timestamps and flags stale feeds on the dashboard.
  * **Recorded Session:** Supports `--record` flag to save streaming sessions to JSONL for offline demo replay.

### 3.3. Kafka Broker Configuration
* Defined in [`docker-compose.yml`](docker-compose.yml):
  * Confluent Kafka 7.6.1 + Zookeeper.
  * Internal broker port: `kafka:9092`.
  * External host listener port: `localhost:9094`.
  * Topic auto-creation enabled with `NUM_PARTITIONS=8`.

---

## 4. Phase 3: Stream Processing & Feature Fusion (Spark Structured Streaming)

### 4.1. Replay Stream Processor
Implemented in [`pipeline/03_stream_processor.py`](pipeline/03_stream_processor.py).
* **Stream Ingestion:** Ingests from `flights_stream` with static schema enforcement.
* **Stream-to-Static Join:** Broadcast joins streaming flights with preloaded NOAA ISD destination weather table on `(dest, scheduled_arr_hour_utc)`.
* **ML Inference:** Driver loads `gbt_pipeline_v1` and applies distributed `.transform()` scoring in micro-batches.
* **Sliding Window Aggregation:** Computes 1-hour tumbling/sliding arrival pressure windows per destination hub:
  * `inbound_count = count(flight_id)`
  * `avg_predicted_delta = avg(predicted_delta_delay)`
  * `high_risk_count = count_if(predicted_delta_delay > 5.0)`
* **Execution Flags:** Supports `--reset` (cleans checkpoints and outputs for deterministic reruns) and `--benchmark-label`.

### 4.2. Live Stream Processor
Implemented in [`pipeline/04_live_processor.py`](pipeline/04_live_processor.py).
* **Multi-Topic Consumer:** Ingests `live_flights`, `airport_status`, and `weather` streams concurrently.
* **Spatial Hub Attribution:** Uses Haversine distance calculations to associate airborne transponder positions within a configurable radius (e.g., 100 km) of major hubs.
* **Live Feature Enrichment:** Fuses current FAA delay statuses with real-time METAR weather to compute active hub congestion scores.

---

## 5. Phase 4: Storage Layer (Parquet Medallion Architecture)

Stream sinks write to Parquet format with Snappy compression and structured checkpointing:

| Layer | Directory Path | Purpose | Update Strategy |
|:---|:---|:---|:---|
| **Bronze** | `data/bronze/flights_raw/` | Raw ingested Kafka payload dump with checkpointing | Append-only partitioned by date |
| **Silver** | `data/silver/flights_enriched/` | Cleaned records joined with weather and lane baselines | Partitioned by date and origin hub |
| **Gold Predictions** | `data/gold/predictions/` | Dynamic ETA predictions for active airborne flights | Fast micro-batch append |
| **Gold Congestion** | `data/gold/hub_congestion/` | 1-hour windowed arrival pressure metrics per hub | Windowed rollup partitions |
| **Gold Live Status** | `data/gold/live_hub_status/` | Real-time live hub stress metrics (Plan V2) | Micro-batch overwrite |

---

## 6. Phase 5: Serving & Observability Layer

### 6.1. FastAPI REST Microservice
Implemented in [`api/main.py`](api/main.py).
* High-performance asynchronous endpoints:
  * `GET /api/v1/flights/active`: List current in-flight trips with real-time predicted delay deltas.
  * `GET /api/v1/hubs/{hub_code}/congestion`: Inbound arrival volume and delay risk distribution for a given hub.
  * `GET /api/v1/system/metrics`: Streaming health indicators (batch duration, input rate, processing rate).

### 6.2. Streamlit Operational Dashboard
Implemented in [`dashboard/app.py`](dashboard/app.py) and [`dashboard/live_radar.py`](dashboard/live_radar.py).
* **Live Radar Panel:** Interactive PyDeck / Plotly map tracking live aircraft positions around US logistics hubs, styled with real FAA ground-stop alerts, live weather conditions, and STALE data badges.
* **Replay & Benchmark Panel:**
  * Fleet radar map showing delay risk color codes (Green: on time, Yellow: minor delay, Red: high risk).
  * Hub inbound pressure bar charts for key freight/logistics hubs (ORD, DFW, ATL, MEM, SDF, CVG).
  * System health tiles (Gold records/min, Throughput rps, p50 batch time ms, p95 latency ms).
  * RQ2 benchmark configuration charts and raw metric tables.
* **Story Tabs:** Operational storytelling charts ("Why delays happen", worst lanes heatmap, weather impact curves, and "today vs. typical" baseline deltas).

---

## 7. Performance Benchmark Matrix (RQ2 Methodology)

Implemented in [`benchmark/run_benchmark.py`](benchmark/run_benchmark.py) and analyzed via [`benchmark/analyze_results.py`](benchmark/analyze_results.py).

Evaluates Spark Structured Streaming scalability across a $3 \times 3 \times 3 \times 3$ factorial matrix:

| Factor | Tested Levels | Engineering Rationale |
|:---|:---|:---|
| **Spark CPU Cores** | 2, 4, 8 | Quantifies multi-core scaling efficiency and driver/executor speedup. |
| **Shuffle Partitions** | 8, 64, 200 | Compares low-partition micro-batch execution (8–64) against default 200 overhead. |
| **Kafka Replay Rate** | 1,000, 5,000, 20,000 eps | Stresses system behavior under normal, busy, and extreme burst ingestion. |
| **Batch Cap (`maxOffsetsPerTrigger`)** | 5,000, 20,000, 100,000 | Enforces micro-batch backpressure to prevent JVM heap exhaustion. |

### Evaluated Metrics:
* **Throughput:** Processed records per second ($\text{recs/sec}$).
* **Batch Duration:** Spark micro-batch processing time ($\text{ms}$).
* **End-to-End Latency:** Time elapsed between simulated event timestamp and Gold table write ($p_{50}$ median and $p_{95}$ tail latency).
* **Summary Headline:** Generates concise benchmark headline strings (e.g., `p95 = 45 ms, throughput = 2,340 rps`).

---

## 8. Technical Risks & Engineering Safeguards

```
┌──────────────────────────────────────┬────────────────────────────────────────────┬────────────────────────────────────────────────────────┐
│ Identified Failure Mode / Bottleneck │ Root Cause                                 │ Mitigation & Engineering Safeguard                     │
├──────────────────────────────────────┼────────────────────────────────────────────┼────────────────────────────────────────────────────────┤
│ 1. Memory Exhaustion at 20k eps      │ Unbounded micro-batch size causes JVM OOM  │ Enable Backpressure and enforce strict                 │
│                                      │ on executors during ingestion bursts.      │ `maxOffsetsPerTrigger` thresholds.                     │
├──────────────────────────────────────┼────────────────────────────────────────────┼────────────────────────────────────────────────────────┤
│ 2. Task Scheduling Overhead          │ Default `shuffle.partitions = 200` creates │ Tune shuffle partitions down to 8 or 16 to match       │
│                                      │ excessive small tasks per micro-batch.     │ available local CPU cores.                             │
├──────────────────────────────────────┼────────────────────────────────────────────┼────────────────────────────────────────────────────────┤
│ 3. External API Rate Limits          │ OpenSky and public weather API rate caps   │ Implement exponential backoff, poll rate throttling,   │
│                                      │ causing request drops or 429 errors.       │ and cached stale-data fallbacks with STALE badges.     │
├──────────────────────────────────────┼────────────────────────────────────────────┼────────────────────────────────────────────────────────┤
│ 4. Deterministic Replay Integrity    │ Repeated replay runs duplicating records   │ Support `--reset` flag in stream processor to clean    │
│                                      │ in downstream Gold tables.                 │ checkpoints and output directories before execution.   │
├──────────────────────────────────────┼────────────────────────────────────────────┼────────────────────────────────────────────────────────┤
│ 5. Parquet Small-File Fragmentation  │ Frequent micro-batch triggers generating   │ Enforce batch trigger window (`trigger-interval >= 2s`)│
│                                      │ thousands of sub-megabyte files on disk.   │ and write aggregated partition layouts.                │
└──────────────────────────────────────┴────────────────────────────────────────────┴────────────────────────────────────────────────────────┘
```
