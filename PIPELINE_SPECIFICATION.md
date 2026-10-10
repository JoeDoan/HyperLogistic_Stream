# HyperLogistics-Stream: End-to-End System Pipeline & Technical Specification

**Project:** Real-Time Hub-to-Hub Logistics ETA Prediction with Kafka and Spark Structured Streaming  
**Course:** COMP-SCI 5540 Principles of Big Data Management, Fall 2026, UMKC  
**Authors:** Tony Nguyen & Joe Doan  
**Instructor:** Dr. Md Yusuf Sarwar Uddin  

---

## 1. System Architecture Diagram

```
[ OFFLINE BATCH PIPELINE ]
  BTS (2018-2023) + NOAA Weather ──► Data Cleaning & UTC Alignment ──► Feature Store ──► Spark MLlib Train ──► Serialized Pipeline Model (GBT / RF)
                                                                                                                   │
                                                                                                                   │ (Loaded into Memory)
[ ONLINE STREAMING PIPELINE ]                                                                                      ▼
  BTS 2024 (Replay File) ──► Kafka Producer ──► Kafka Topic (flights_stream) ──► Spark Structured Streaming ──► MLlib .transform()
                                  (1k / 5k / 20k eps)                                    │
                                                                              Broadcast Join with
                                                                           Static NOAA Weather Table
                                                                                         │
                                      ┌──────────────────────────────────────────────────┴──────────────────────────────────────────────────┐
                                      ▼                                                  ▼                                                  ▼
                                 Bronze Layer                                       Silver Layer                                       Gold Layer
                              (Raw Kafka Dump)                                (Joined Clean Features)                           (Predictions + Aggregates)
                                      │                                                  │                                                  │
                                      └──────────────────────────────────────────────────┼──────────────────────────────────────────────────┘
                                                                                         ▼
                                                                            FastAPI Serving & Metrics
                                                                                         │
                                                                            ┌────────────┴────────────┐
                                                                            ▼                         ▼
                                                                    Streamlit Dashboard         Benchmark Logger
                                                                     (Live Map & Tiles)      (Throughput, p50/p95)
```

---

## 2. Phase 1: Offline Batch Pipeline (Data Preparation & Model Training)

### 2.1. Raw Data Extraction
* **BTS Reporting Carrier On-Time Performance (2018–2023):**
  * Tens of millions of historical flight records.
  * Year 2020 is explicitly excluded due to pandemic flight volume anomalies.
* **NOAA Integrated Surface Database (ISD):**
  * Hourly weather readings for all destination airports (temperature, visibility, wind speed, precipitation).

### 2.2. Cleaning & Standardization
* Filter out non-operational records (`Cancelled == 1` or `Diverted == 1`).
* Align local wall-clock departure times (`FlightDate`, `CRSDepTime`, `WheelsOff`) to universal UTC timestamps using airport IANA timezone mappings (`America/New_York`, `America/Chicago`, `America/Denver`, `America/Los_Angeles`).
* Clean and extract numeric metrics from NOAA ISD compound strings:
  * `WND`: Wind direction (degrees) and speed (converted from tenth m/s to m/s).
  * `VIS`: Visibility distance in meters.
  * `TMP`: Air temperature in degrees Celsius.

### 2.3. Feature Engineering & Formulation
* **Target Label Formulation:**
  $$\Delta\text{delay} = \text{ArrDelay} - \text{DepDelay}$$
  *Rationale:* Formulating the target as delay change prevents the model from acting as a trivial identity function ($Y \approx \text{DepDelay}$) and forces it to learn airborne dynamics.
* **Dynamic ETA Formulation:**
  $$\text{Predicted\_ETA} = \text{CRSArrTime} + \text{DepDelay} + \text{Predicted\_}\Delta\text{delay}$$
* **Feature Set ($X$):**
  * `DepDelay`: Departure delay in minutes (known at wheels-off).
  * `Distance`: Great circle flight distance in miles.
  * `DepHourUTC`, `ArrHourUTC`: Hour of day in UTC.
  * `DayOfWeek`: 1 to 7.
  * `DestWindSpeed`, `DestVisibility`, `DestTemp`: Arrival weather condition at scheduled arrival hour.
  * `LaneMeanDelay`: Rolling 30-day historical mean delay for the specific Origin-Destination lane.

### 2.4. Offline Model Training & Serialization
* **Algorithms:** Spark MLlib `GBTRegressor` (primary) and `RandomForestRegressor` (baseline).
* **Pipeline:** `StringIndexer` $\rightarrow$ `OneHotEncoder` $\rightarrow$ `VectorAssembler` $\rightarrow$ `GBTRegressor`.
* **Evaluation:** Hold-out evaluation on 2023 data measuring MAE and RMSE against:
  1. Baseline 1: Unchanged delay assumption ($\Delta\text{delay} = 0$).
  2. Baseline 2: Historical lane mean.
* **Artifact Output:** Serialized `PipelineModel` exported to disk (`models/gbt_pipeline_v1`).

---

## 3. Phase 2: Ingestion Layer (Kafka Time-Replay Engine)

### 3.1. Kafka Producer Mechanics
* **Input Source:** Chronologically sorted BTS 2024 flight departure records.
* **Replay Simulation:** Replays events based on actual `WheelsOff` milestone order.
* **Throughput Rate Throttling:**
  * Configurable emission rates: **1,000**, **5,000**, and **20,000 events/second** (for load saturation benchmarks).
* **Message Payload (JSON):**
  ```json
  {
    "flight_id": "AA1234_20240108_ORD_DFW",
    "tail_number": "N485PX",
    "carrier": "AA",
    "origin": "ORD",
    "dest": "DFW",
    "crs_dep_time_utc": "2024-01-08T14:30:00Z",
    "wheels_off_utc": "2024-01-08T14:45:00Z",
    "crs_arr_time_utc": "2024-01-08T17:15:00Z",
    "dep_delay": 15.0,
    "distance": 802.0
  }
  ```

### 3.2. Kafka Broker Configuration
* **Topic Name:** `flights_stream`
* **Partitions:** 8 partitions (aligned with maximum worker CPU core configurations).
* **Replication Factor:** 1 (single-node local testing) / 3 (cluster mode).

---

## 4. Phase 3: Stream Processing & Feature Fusion (Spark Structured Streaming)

### 4.1. Stream Reading & Micro-Batching
* Ingest from Kafka via `spark.readStream`.
* Static schema parsing using `from_json` to avoid dynamic schema inference overhead.
* Rate limiting per trigger configured via `maxOffsetsPerTrigger` (5,000, 20,000, 100,000).

### 4.2. Stream-to-Static Broadcast Join
* Preload the 2024 NOAA ISD hourly weather table as a static DataFrame.
* Broadcast join stream with static weather on `(dest, scheduled_arr_hour_utc)`.
* *Design invariant:* Avoid stateful stream-to-stream joins to eliminate watermarking overhead and unbounded state-store growth.

### 4.3. Real-Time ML Scoring
* Load pre-trained `PipelineModel` into the Spark driver context.
* Apply vectorized model scoring: `scored_stream = model.transform(enriched_stream)`.
* Compute dynamic arrival time and delay alert flags (`is_high_risk = predicted_delta > 30.0`).

### 4.4. Sliding Window Hub Aggregation
* Compute arrival pressure for destination hubs using sliding event-time windows:
  * Window duration: 1 hour, Slide interval: 15 minutes.
  * Aggregations: `count(flight_id)`, `avg(predicted_delay)`, `count_if(is_high_risk)`.

---

## 5. Phase 4: Storage Layer (Parquet Medallion Architecture)

The system writes stream outputs to a multi-tiered Parquet store with Snappy compression:

| Layer | Directory | Purpose | Retention / Format |
|:---|:---|:---|:---|
| **Bronze** | `data/bronze/flights_raw/` | Raw ingested Kafka payload dump with checkpointing | Append-only, partitioned by date |
| **Silver** | `data/silver/flights_enriched/` | Cleaned records joined with destination weather & lane history | Parquet, partitioned by date and origin hub |
| **Gold** | `data/gold/predictions/` | Real-time scored predictions for active airborne flights | Overwritten/Appended, fast lookup |
| **Gold Aggs** | `data/gold/hub_congestion/` | 1-hour rolling window arrival counts and hub delay metrics | Materialized summary table |

---

## 6. Phase 5: Serving & Observability Layer

### 6.1. FastAPI REST Microservice
* Exposes asynchronous non-blocking query endpoints:
  * `GET /api/v1/flights/active`: List current in-flight trips and predicted delay changes.
  * `GET /api/v1/hubs/{hub_code}/congestion`: Get inbound arrival count and delay distribution for a specific hub.
  * `GET /api/v1/system/metrics`: Expose real-time Spark streaming metrics (last batch duration, input rate, processing rate).

### 6.2. Streamlit Operational Dashboard
* **Live Fleet Radar Map:** Real-time nationwide map (Deck.gl / PyDeck) displaying active flights color-coded by delay risk (Green: on-time, Yellow: minor delay, Red: delay > 30 mins).
* **Hub Pressure Monitoring:** Bar charts showing predicted arrivals over the next 1–4 hours for top cargo/logistics hubs (ORD, DFW, ATL, MEM, SDF, CVG).
* **System Health Tiles:** Real-time gauges for Kafka ingestion rate, Spark micro-batch processing time, and p50/p95 end-to-end latency.

---

## 7. Performance Benchmark Matrix (RQ2 Methodology)

To identify Spark scalability and saturation limits under heavy load, the pipeline executes a controlled $3 \times 3 \times 3 \times 3$ factorial benchmark:

| Variable | Values Tested | Rationale & Expected Behavior |
|:---|:---|:---|
| **CPU Cores** | 2, 4, 8 cores | Measure multi-core CPU scaling efficiency and speedup. |
| **Shuffle Partitions** | 8, 64, 200 | Default 200 causes high scheduling lag on micro-batches; 8–64 matches cores. |
| **Kafka Replay Rate** | 1,000, 5,000, 20,000 events/sec | Evaluate driver/executor behavior under light, medium, and extreme burst ingestion. |
| **Batch Size Cap (`maxOffsetsPerTrigger`)** | 5,000, 20,000, 100,000 | Acts as a safety backpressure ceiling to prevent Out-Of-Memory (OOM) crashes. |

### Measured System Metrics:
* **Throughput:** Processed records per second ($\text{recs/sec}$).
* **Batch Processing Time:** Duration of each micro-batch execution ($\text{ms}$).
* **End-to-End Latency:** Time elapsed from simulated departure event emission to Gold table write completion ($p_{50}$ median and $p_{95}$ tail latency).

---

## 8. Technical Risks, Bottlenecks & Mitigation Strategies

```
┌───────────────────────────────────────┬───────────────────────────────────────────┬────────────────────────────────────────────────────────┐
│ Identified Failure Mode / Bottleneck  │ Root Cause                                │ Mitigation & Engineering Safeguard                     │
├───────────────────────────────────────┼───────────────────────────────────────────┼────────────────────────────────────────────────────────┤
│ 1. Out-of-Memory (OOM) at 20k eps     │ Unbounded micro-batch size causes JVM heap│ Enable Backpressure (`spark.streaming.backpressure`)  │
│                                       │ exhaustion on executors during bursts.    │ and enforce tight `maxOffsetsPerTrigger`.              │
├───────────────────────────────────────┼───────────────────────────────────────────┼────────────────────────────────────────────────────────┤
│ 2. Task Scheduling Lag                │ Default `shuffle.partitions = 200` creates│ Tune partitions down to 8 or 16 to match available     │
│                                       │ hundreds of empty/tiny tasks per trigger. │ worker CPU cores.                                      │
├───────────────────────────────────────┼───────────────────────────────────────────┼────────────────────────────────────────────────────────┤
│ 3. UTC Timestamp Drift                │ Local departure times misaligned with     │ Build strict UTC conversion using authoritative        │
│                                       │ universal NOAA hourly weather timestamps. │ airport IANA timezone database before join.            │
├───────────────────────────────────────┼───────────────────────────────────────────┼────────────────────────────────────────────────────────┤
│ 4. Parquet Small Files Problem        │ Fast micro-batch triggers create thousands│ Enforce batch trigger window (`Trigger.ProcessingTime` │
│                                       │ of sub-megabyte files on disk.            │ of 2-5 seconds) and run periodic compaction.           │
└───────────────────────────────────────┴───────────────────────────────────────────┴────────────────────────────────────────────────────────┘
```
