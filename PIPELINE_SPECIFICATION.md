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
      ├── OpenSky Network (Aircraft Positions)  ────────► Kafka Topic: live_flights (120s)               │
      ├── FAA NAS Status (Ground Stops & Delays) ────────► Kafka Topic: airport_status (60s)              │
      └── NWS API (METAR Surface Weather)        ────────► Kafka Topic: weather (300s)                   ├──► Apache Kafka
                                                                                                          │    (Port 9092 / 9094)
                                                                                                          │
[ 3. STREAM PROCESSING ENGINES (PySpark Structured Streaming) ]                                           │
  (A) Replay Engine (pipeline/03_stream_processor.py) ◄───────────────────────────────────────────────────┤
      Broadcast Join with Static NOAA Table ──► VectorAssembler ──► MLlib .transform() ──► Gold Output    │
  (B) Live Processor (pipeline/04_live_processor.py) ◄────────────────────────────────────────────────────┘
      Latest-Snapshot Haversine Join (km) ──► Multi-Topic Enrichment ──► Live Gold Tables
                                    │
                                    ▼
[ 4. MEDALLION STORAGE LAYER (Snappy Parquet) ]
  ├── Bronze: Raw Kafka event stream dumps with checkpointing
  ├── Silver: Cleaned & weather-joined records
  └── Gold:
      ├── Scored dynamic predictions: data/gold/predictions/
      ├── Rolling window hub congestion: data/gold/hub_congestion/
      └── Real-time live hub feeds: data/gold/live/{weather,airport_status,hub_traffic}/
                                    │
                                    ▼
[ 5. SERVING, OBSERVABILITY & BENCHMARKS ]
  ├── FastAPI Microservice (api/main.py) ───────► REST Endpoints (/flights/active, /hubs/{hub}/congestion)
  ├── Streamlit Dashboard (dashboard/app.py) ──► Tabs: Live Radar (live_radar.py) | Replay & Benchmark | Model Insights
  └── Benchmark Suite (benchmark/run_benchmark.py, analyze_results.py) ──► RQ2 Factorial Matrix & Plots
```

---

## 2. Phase 1: Offline Batch Pipeline (Data Preparation & Model Training)

Implemented in [`pipeline/01_batch_train.py`](pipeline/01_batch_train.py) and evaluated per slice in [`pipeline/04_error_analysis.py`](pipeline/04_error_analysis.py).

### 2.1. Raw Data Extraction (20.55 GB Full Colab Corpus; ~11 GB Local 2022 to 2024 Slice)
* **BTS Reporting Carrier On-Time Performance (2018, 2019, 2021, 2022, 2023 train; 2024 holdout, 18.63 GB across 72 monthly CSVs on Colab):**
  * Historical flight departure and arrival records for 24 US hubs (`7,996,449` training flights; year 2020 excluded due to COVID traffic collapse).
  * Year 2024 (`1,665,362` flights, `3.20 GB`) is held out for model evaluation and Kafka streaming replay.
* **NOAA Integrated Surface Database (ISD) (2018 to 2024 excluding 2020, 1.90 GB across 240 station-year CSVs):**
  * Hourly surface readings for all 24 tracked hubs (wind speed, visibility, air temperature, and cloud ceiling).

### 2.2. Cleaning & Standardization
* Pin Spark SQL session timezone to `UTC` (`spark.sql.session.timeZone = UTC`) across batch, streaming, and live jobs so timestamp truncation and hour features remain identical on Linux, macOS, and Windows.
* Filter cancelled and diverted flights (`Cancelled == 0`, `Diverted == 0`).
* Convert local wall-clock times (`FlightDate`, `CRSDepTime`, `WheelsOff`, `CRSArrTime`) to universal UTC timestamps using airport IANA timezone mappings while computing `DayOfWeek` consistently from local `FlightDate`.
* Clean and extract numeric metrics from NOAA ISD compound strings:
  * `WND`: Wind speed converted to m/s (`>= 9999` treated as missing).
  * `VIS`: Visibility distance in meters (`>= 999999` treated as missing).
  * `TMP`: Air temperature in degrees Celsius (`abs >= 9999` treated as missing).
  * `CIG`: Cloud ceiling height in meters (`>= 99999` mapped to `22000.0` m clear sky).

### 2.3. Feature Engineering & Formulation (14 Numeric Features)
* **Target Label Formulation:**
  $$\Delta\text{delay} = \text{ArrDelay} - \text{DepDelay}$$
  *Rationale:* Predicting delay change forces the model to learn airborne transit and turnaround recovery dynamics rather than copying departure delay.
* **Dynamic ETA Formulation:**
  $$\text{Predicted\_ETA} = \text{CRSArrTime} + \text{DepDelay} + \text{Predicted\_}\Delta\text{delay}$$
* **Feature Vector ($X$, 14 numeric columns):**
  * Schedule and lane (6): `DepDelay`, `Distance`, `DepHourUTC`, `ArrHourUTC`, `DayOfWeek` (from local `FlightDate`), and `LaneMeanDelay` (historical Origin-Destination mean computed on training years only).
  * Destination weather at scheduled arrival UTC hour (4): `DestWindSpeed`, `DestVisibility`, `DestTemp`, `DestCeiling` (joined on observed NOAA ISD at `sched_arr_hour_utc`, acting as an oracle forecast upper bound for arrival weather).
  * Origin weather at wheels-off UTC hour (4): `OriginWindSpeed`, `OriginVisibility`, `OriginCeiling`, `OriginTemp`.

### 2.4. Offline Model Training, Ablation & Serialization
* **Algorithms:** Spark MLlib `GBTRegressor` (primary) and `RandomForestRegressor` (comparison).
* **Pipeline:** `VectorAssembler(handleInvalid="keep")` $\rightarrow$ `GBTRegressor` (or `RandomForestRegressor`).
* **Evaluation:** Hold-out 2024 evaluation measuring MAE and RMSE across a 4-step weather ablation (`models/ablation.csv`) and two baselines:
  1. *Baseline 1:* Delay unchanged ($\Delta\text{delay} = 0$, MAE `12.749` min).
  2. *Baseline 2:* Historical training-years lane mean (MAE `10.799` min).
* **Artifacts:** Serialized `PipelineModel` in `models/gbt_pipeline_v1`, metrics in `models/training_metrics.json`, ablation table in `models/ablation.csv`, hub-hour-weekday reference in `models/baseline_hub_hour_dow.csv`, and slice error tables in `models/error_by_*.csv`.

---

## 3. Phase 2: Ingestion Layer (Kafka Dual Ingestion Engine)

### 3.1. Mode A: Historical Replay Producer
Implemented in [`pipeline/02_kafka_producer.py`](pipeline/02_kafka_producer.py).
* **Input Source:** Chronologically sorted BTS 2024 flight departure records (`flight_id`, `flight_date`, `origin`, `dest`, UTC timestamps, `dep_delay`, `distance`, `emit_epoch_ms`).
* **Replay Mechanics:** Emits events strictly aligned with historical `WheelsOff` milestone order.
* **Throughput Throttling:** Configurable emission rates (1,000, 5,000, 20,000 events/second) via high-precision token bucket pacing.
* **Target Topic:** `flights_stream` (8 partitions, key: `origin`).

### 3.2. Mode B: Live Multi-Source Feed Producer (Plan V2)
Implemented in [`live/live_producer.py`](live/live_producer.py).
* Coordinates three real-time asynchronous fetchers:
  1. **OpenSky Network** ([`live/fetch_opensky.py`](live/fetch_opensky.py)): Polled every 120 s by default for live transponder positions, altitudes, and vertical rates across US airspace (`live_flights` topic), honoring HTTP `429 Retry-After` headers to stay within anonymous daily credit budgets.
  2. **FAA NAS Status** ([`live/fetch_faa_nas.py`](live/fetch_faa_nas.py)): Polled every 60 s for official airport ground stops, ground delay programs, and arrival or departure delays (`airport_status` topic).
  3. **NWS API** ([`live/fetch_nws_weather.py`](live/fetch_nws_weather.py)): Polled every 5 min for METAR surface observations (wind speed and gusts, visibility, cloud ceiling, temperature) across all 24 hubs (`weather` topic).
* **Resilience Safeguards:**
  * **Stale-Data Fallback & Sink Isolation:** Decouples HTTP fetch errors from Kafka publish errors, re-publishing last-known FAA and weather rows with `stale=true` so feed outages trigger a `STALE` badge without terminating producer threads.
  * **Recorded Session & Sample Fallback:** Supports `--record` to save sessions to `data/live_recordings/*.jsonl`, with a committed snapshot in `data/sample/` so fresh clones render the Live Radar tab without Kafka or Spark.

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
* **Stream Ingestion:** Ingests from `flights_stream` with static schema enforcement (`FLIGHT_SCHEMA`, including `flight_date`).
* **Stream-to-Static Joins:** Broadcast joins streaming flights twice against preloaded 2024 NOAA ISD weather (`(dest_icao, sched_arr_hour_utc)` and `(origin_icao, wheels_off_hour_utc)`) and once against `models/lane_mean.parquet`.
* **ML Inference Guard:** Loads `models/gbt_pipeline_v1` and fails fast if absent unless `--allow-dummy-model` is explicitly passed (tagging Gold rows with `is_fallback_prediction`).
* **Repeat-Safe Deduplication:** `dedupe_batch` drops duplicate `flight_id` records within each micro-batch and anti-joins against existing Gold Parquet files.
* **Sliding Window Aggregation:** Computes 1-hour sliding arrival pressure windows (15-minute slide) per destination hub using the configured `high_risk_threshold_min` (`5.0` min on Colab, `30.0` min local).
* **Execution Flags:** Supports `--reset` (validated against `PROJECT_ROOT` and configured Medallion or `--output-root` paths), `--output-root`, `--no-dedupe`, and `--benchmark-label`.

### 4.2. Live Stream Processor
Implemented in [`pipeline/04_live_processor.py`](pipeline/04_live_processor.py).
* **Multi-Topic Consumer:** Ingests `live_flights`, `airport_status`, and `weather` streams concurrently without invoking the GBT model.
* **Latest-Snapshot Spatial Hub Attribution:** Filters each `live_flights` micro-batch to the latest `fetched_at` poll, deduplicates `icao24`, and left-joins Haversine proximity counts (`<= 100` km) across all 24 hubs so multi-poll batches never double-count aircraft and zero-traffic hubs refresh cleanly.
* **Live Feature Enrichment:** Classifies METAR flight categories (`VFR`, `MVFR`, `IFR`, `LIFR`), scores FAA delay severity (`0` to `3`), and joins historical hub-hour-weekday baselines (`models/baseline_hub_hour_dow.parquet` or `.csv`).

---

## 5. Phase 4: Storage Layer (Parquet Medallion Architecture)

Stream sinks write to Parquet format with Snappy compression and structured checkpointing:

| Layer | Directory Path | Purpose | Update Strategy |
|:---|:---|:---|:---|
| **Bronze** | `data/bronze/flights_raw/` | Raw ingested Kafka payload dump with checkpointing | Append-only partitioned by `ingest_date` |
| **Silver** | `data/silver/flights_enriched/` | Cleaned records joined with origin/destination weather and lane baselines | Append-only partitioned by `enrich_date` |
| **Gold Predictions** | `data/gold/predictions/` | Repeat-safe ETA predictions for replayed 2024 flights | Deduplicated micro-batch append |
| **Gold Congestion** | `data/gold/hub_congestion/` | 1-hour windowed arrival pressure metrics per hub | Windowed micro-batch append |
| **Gold Live Feeds** | `data/gold/live/{weather,airport_status,hub_traffic}/` | Real-time live hub weather, FAA status, and nearby aircraft counts | Partitioned by `gold_date` |

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
