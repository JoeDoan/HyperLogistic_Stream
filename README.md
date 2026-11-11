# HyperLogistics-Stream

**Real-Time Hub-to-Hub Logistics Arrival Delay Prediction with Kafka and Spark Structured Streaming**

[![CI](https://github.com/JoeDoan/HyperLogistic_Stream/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/JoeDoan/HyperLogistic_Stream/actions/workflows/ci.yml)

| | |
|---|---|
| **Course** | COMP-SCI 5540 Principles of Big Data Management (Fall 2026) |
| **Institution** | School of Science and Engineering, University of Missouri-Kansas City (UMKC) |
| **Instructor** | Dr. Md Yusuf Sarwar Uddin |
| **Authors** | Tony Nguyen (`tmnc62@umsystem.edu`) & Joe Doan (`jdbc6@umsystem.edu`) |

## Contents

1. [Project overview](#1-project-overview)
2. [Quick start](#2-quick-start)
3. [Data setup](#3-data-setup-required-for-batch-retraining-and-replay)
4. [Pipeline script cheat sheet](#4-pipeline-script-cheat-sheet)
5. [Repository structure](#5-repository-structure)
6. [Exploratory data analysis](#6-exploratory-data-analysis)
7. [Research questions and evaluation matrix](#7-research-questions-and-evaluation-matrix)
8. [Results](#8-results)
9. [Testing](#9-testing)
10. [Troubleshooting](#10-troubleshooting)
11. [License and academic integrity](#11-license-and-academic-integrity)

---

## 1. Project overview

In middle-mile freight logistics, unexpected transit delays create dock-door bottlenecks and missed outbound connections across downstream hubs. **HyperLogistics-Stream** ingests, cleans, enriches, and predicts in-air arrival delay changes ($\Delta\text{delay} = \text{ArrDelay} - \text{DepDelay}$) and dynamic ETAs right at takeoff across **40 major U.S. logistics and passenger hubs**.

> Commercial flights serve as a **public proxy** for middle-mile freight movement because private cargo carriers do not publish shipment-level timestamps, while passenger belly cargo and air freighters share the same 40 hubs (including Memphis `MEM`, Louisville `SDF`, Indianapolis `IND`, Ontario `ONT`, Chicago `ORD`, and Dallas `DFW`), runways, NWS weather, and FAA traffic programs.

### Architecture

1. **Ingestion:** Apache Kafka replays historical trips in chronological `WheelsOff` order at controlled rates (1k, 5k, and 20k events/sec). A multi-threaded **live producer** also publishes FlightAware AeroAPI flights, OpenSky aircraft positions, FAA NAS airport status, and NWS weather.
2. **Storage (Medallion):** Parquet tables in **Bronze** (raw Kafka events), **Silver** (UTC-normalized and weather-joined), and **Gold** (deduplicated ETA predictions and 1-hour sliding-window hub rollups).
3. **Processing and ML:** PySpark Structured Streaming runs event-time watermarks and broadcast joins against NOAA and NWS weather, while Spark MLlib (`GBTRegressor` and `RandomForestRegressor`) scores inbound trips across 14 features.
4. **Serving:** FastAPI REST endpoints and a 3-tab Streamlit dashboard (**Live Radar**, **Replay & Benchmark**, and **Model Insights**).

### What is new, and how it differs

* **Live and historical worlds in one pipeline.** Today's NWS weather, FAA ground delay programs, OpenSky traffic, and landed FlightAware flights at each hub are compared directly against typical delays for that hub, local hour, and weekday across **40 U.S. hubs** (`14.38M` training flights from `2018-2023` and `3.04M` unseen `2024` holdout flights).
* **Not another status board or static statistics page.** Flight-status apps show only current departure delay and delay-statistics sites show only historical averages. This pipeline joins both at takeoff, labels model outputs clearly as predictions, and documents where tree regression fails (`EVALUATION.md` sections 3.1 and 6).
* **Ready to score on fresh clones.** Pre-trained 40-hub Spark `PipelineModel` weights (`models/gbt_pipeline_v1` and `models/rf_pipeline_v1`) and Parquet route lookup tables (`models/lane_mean.parquet` and `models/baseline_hub_hour_dow.parquet`) ship in the repository, so streaming replay and the dashboard run without a multi-hour retraining step.

### Real-world applications and operational impact

* **Cross-dock truck hold-or-release decisions for freight dispatchers.** When an inbound feeder flight departs `15` minutes late, assuming the delay stays constant triggers premature truck releases or idle dock crews. Across `3,041,312` unseen `2024` flights, `57.2%` recover more than `5` minutes in the air, and the tuned 14-feature GBT cuts arrival-time MAE from `12.04` minutes ("no change") to `9.77` minutes (`18.8%` closer, beating the historical route average at all `40` hubs). Dispatchers can hold an outbound linehaul truck when a long route will absorb its delay and release the dock early when destination wind or low ceiling erodes the schedule cushion.
* **Airport gate, tug, and ground-crew allocation.** Hub operations teams schedule tow tugs, de-icing pads, baggage belts, and tight `45`-minute connection banks around arrival waves. Comparing live FAA NAS delay programs and NWS `IFR`/`LIFR` weather against `6,373` historical hub-hour-weekday baseline cells highlights abnormal hub stress (`0` to `6`) before inbound aircraft bunch up on the ramp.
* **Downstream warehouse, courier, and pickup SLAs.** On `505` real landed FlightAware flights, `77.2%` made up time in the air (recovering `8.5` minutes on average), and historical hub adjustment cut arrival error from `12.19` minutes to `9.67` minutes (`20.7%` closer). Warehouse management systems and perishable-cargo couriers can poll FastAPI (`/api/v1/hubs/{hub_code}/congestion` and `/api/v1/flights/active`) to adjust driver dispatch times while treating severe mid-air reroutes (`> 15` min lost) as live radar exceptions rather than tree-regression forecasts.

---

## 2. Quick start

Runs the live radar and dashboard without downloading the multi-gigabyte historical archive. Pre-trained 40-hub tables and recorded live sessions are already in `models/`, and if live Kafka output is absent, the Replay tab falls back cleanly to demo data.

**Prerequisites:** Python 3.10+ and Docker Desktop (running). Spark and Java run inside Docker for the live path, so you do not need a local Java or Hadoop setup.

```bash
# 1. Clone and install Python dependencies
git clone https://github.com/JoeDoan/HyperLogistic_Stream.git
cd HyperLogistic_Stream
pip install -r requirements.txt

# 2. (Optional) copy environment template and add AEROAPI_KEY if polling FlightAware live
cp .env.example .env            # Windows: copy .env.example .env

# 3. Start Kafka (Zookeeper + broker + topic init)
docker compose up -d

# 4. Terminal A - publish live feeds to Kafka (add --record to save a session for offline replay)
#    Start this first so it creates the Kafka topics that the live processor reads.
python live/live_producer.py --record

# 5. Terminal B - run Spark live processor in Docker (wait ~10 s after step 4)
docker compose --profile live up -d live-processor

# 6. Start the dashboard
streamlit run dashboard/app.py
```

The live processor runs in Docker (`Dockerfile.live`) because Windows lacks Hadoop `winutils` and newer host PySpark builds do not match the Spark 3.5 Kafka connector. On Linux or Google Colab you can run `python pipeline/04_live_processor.py` directly.

Check container progress with `docker logs --tail 5 hyperlogistics-live-processor`. The Live Radar badge switches from `RECORDED` to `LIVE` after the next producer cycle (about one minute). Stop with `docker compose --profile live down` and `Ctrl+C` in the producer terminal.

Open **http://localhost:8501**.

| Service | URL |
|---|---|
| Dashboard | http://localhost:8501 |
| API docs (when `uvicorn` is running) | http://localhost:8000/docs |
| Kafka (host port) | `localhost:9094` |

**Dashboard only (no Kafka, Spark, or Docker):** `streamlit run dashboard/app.py` loads the committed 40-hub model insights and recorded live sessions immediately.
**Offline feed smoke check:** `python live/live_producer.py --dry-run --once`

---

## 3. Data setup (required for batch retraining and replay)

> [!IMPORTANT]
> `data/` holds `11 GB` to `20.5 GB` of raw CSV records (depending on selected years) and is **excluded from Git** via `.gitignore`. Download it before running batch training, historical Kafka replay, or raw EDA.

```bash
# BTS On-Time Flight Records (2022-2024 quick setup ~9.8 GB, or 2018 2019 2021 2022 2023 2024 full Colab corpus ~20.5 GB)
python scripts/download_bts.py --years 2022 2023 2024 --extract

# NOAA ISD hourly weather across all 40 hubs (~911 MB for 3 years, 240 station-year files for 6 years) -> data/raw/noaa/
python scripts/download_noaa.py --years 2022 2023 2024
```

* **Local config (`config/pipeline_config.yaml`):** trains on `2022`, validates on `2023`, and replays `2024` within a `4g` driver budget.
* **Colab full-corpus config (`config/pipeline_config_colab.yaml`):** trains on `2018, 2019, 2021, 2022, 2023` (`14,384,130` flights across `40` hubs) and validates/replays on unseen `2024` (`3,041,312` flights).

---

## 4. Pipeline script cheat sheet

Run commands from the project root. Start Kafka first (`docker compose up -d`) for steps 3 through 6 and step 9. On Windows or macOS laptops without a local Spark cluster, run steps 1, 2, 4, 9, and 11 on Google Colab or Linux.

| Step | Script / Command | Technical function | Real-world operational role |
|---|---|---|---|
| 1. Batch train | `python pipeline/01_batch_train.py --config config/pipeline_config_colab.yaml` | Cleans BTS + NOAA records across 40 hubs, runs the 4-step weather ablation, fits `GBTRegressor` and `RandomForestRegressor`, and writes `models/` (`--cache-dir`, `--skip-rf`). | Refreshes route padding baselines (`lane_mean.parquet`) and weather split rules before a new scheduling season so dispatchers do not rely on stale carrier timetables. |
| 2. GBT hillclimb | `python scripts/tune_gbt.py --config config/pipeline_config_colab.yaml --cache-dir /content/cache_40hubs` | Warm-JVM 5-stage hyperparameter search over tree depth, leaf floor, subsampling, and shrinkage gated by train-vs-holdout overfitting checks. | Squeezes extra ETA accuracy (`9.94` to `9.77` min MAE) across 1,388 lanes while blocking overfit trees that would misfire on unseen 2024 schedules. |
| 3. Replay producer | `python pipeline/02_kafka_producer.py --rate 1000` | Streams 2024 flights in `WheelsOff` order into Kafka topic `flights_stream` (`--limit`, `--loop`). | Simulates normal morning traffic (`1k` events/s) or storm-day hub bursts (`5k` to `20k` events/s) to verify downstream dispatch consumers keep up without dropping flights. |
| 4. Replay processor | `python pipeline/03_stream_processor.py` | Reads `flights_stream`, broadcast-joins weather and `lane_mean.parquet`, scores the 14-feature GBT, deduplicates `flight_id`, and writes Bronze/Silver/Gold Parquet (`--reset`, `--test`). | Updates post-takeoff arrival ETAs and rolling 1-hour inbound hub pressure (`data/gold/hub_congestion`) so cross-dock managers see arrival waves 1 to 5 hours ahead. |
| 5. Live producer | `python live/live_producer.py --record` | Polls FAA NAS (60s), NWS METAR (300s), OpenSky radar (120s), and FlightAware AeroAPI (600s) with `HTTP 429` backoff (`--no-opensky`, `--dry-run`, `--once`). | Gives hub control rooms a single fault-tolerant ingestion daemon that keeps publishing weather and FAA ground stops even when one external API rate-limits. |
| 6. Live processor | `docker compose --profile live up -d live-processor` (Linux/Colab: `python pipeline/04_live_processor.py`) | Aggregates live hub stress (`0` to `6`), deduplicates OpenSky `icao24` transponders, and compares delays against `baseline_hub_hour_dow.csv`. | Flags hubs running hotter than normal for the current weekday and hour (for example, an evening ground delay program at `EWR` or `ORD`) within 60 seconds. |
| 7. Live vs. historical eval | `python live/eval_live_vs_historical.py` | Scores landed AeroAPI recordings in `data/live_recordings/` against 2024 baselines and writes `models/live_vs_historical.*`. | Audits whether historical route recovery still holds on today's live carrier schedules before operations teams trust live ETA adjustments. |
| 8. REST API | `uvicorn api.main:app --port 8000` | Serves `/`, `/api/v1/flights/active`, `/api/v1/hubs/{hub_code}/congestion`, `/api/v1/system/metrics`, and `/api/v1/benchmark/results` from Gold Parquet tables. | Lets external warehouse management (WMS), transportation management (TMS), or Slack alert bots query congested hubs and high-risk inbound flights over HTTP. |
| 9. Scalability benchmark | `python benchmark/run_benchmark.py --quick` | Sweeps CPU cores, shuffle partitions, Kafka rates, and `maxOffsetsPerTrigger` in isolated output directories (`benchmark/analyze_results.py` builds charts). | Sizes Spark CPU cores and micro-batch caps so platform engineers provision enough compute to keep batch p95 latency under 4 seconds during peak sort waves. |
| 10. Dashboard | `streamlit run dashboard/app.py` | Launches the 3-tab operational UI (**Live Radar**, **Replay & Benchmark**, and **Model Insights**). | Gives dispatchers, gate planners, and reviewers one screen combining live hub stress, route risk maps, throughput curves, and per-hub model error. |
| 11. Error analysis | `python pipeline/04_error_analysis.py --config config/pipeline_config_colab.yaml` | Writes `models/error_by_hub.csv`, `error_by_hour.csv`, `error_by_month_hub.csv`, and `error_by_bucket.csv` across all 40 hubs. | Tells operators where to trust the ETA (`SMF`, `PHX`, `STL` under `8.5` min MAE) and where to pad extra buffer (`JFK` `13.3` min MAE or severe mid-air storms). |

Defaults live in [`config/pipeline_config.yaml`](config/pipeline_config.yaml) and the canonical 40-hub registry lives in [`config/hubs.py`](config/hubs.py). Override the config file with `--config <path>` or `HYPERLOGISTICS_CONFIG`.

---

## 5. Repository structure

```text
HyperLogistic_Stream/
├── README.md
├── requirements.txt            # Runtime Python dependencies
├── docker-compose.yml          # Zookeeper + Kafka + topic init (+ live-processor profile)
├── Dockerfile.live             # Spark 3.5 container image for the live processor
├── .env.example                # Environment variable template (AEROAPI_KEY, OpenSky creds)
├── config/
│   ├── hubs.py                 # Single source of truth for all 40 U.S. hubs, coordinates, and USAF stations
│   ├── pipeline_config.yaml    # Local 1-year laptop defaults (4g driver, 50-tree depth-5 GBT)
│   └── pipeline_config_colab.yaml # Full 5-year Colab config (90-tree depth-8 tuned GBT champion)
├── scripts/                    # download_bts.py, download_noaa.py, tune_gbt.py,
│                               # run_eda.py, build_notebook.py, setup_colab.sh
├── pipeline/
│   ├── 01_batch_train.py       # Spark MLlib training, weather ablation, and overfitting telemetry
│   ├── 02_kafka_producer.py    # Chronological 2024 Kafka replay producer
│   ├── 03_stream_processor.py  # Structured Streaming -> Bronze / Silver / Gold Parquet
│   ├── 04_live_processor.py    # Live-topic Spark processor for hub stress and radar counts
│   └── 04_error_analysis.py    # 40-hub, hour, month, and delay-bucket holdout error tables
├── live/                       # live_producer.py, aeroapi_predictor.py, eval_live_vs_historical.py,
│                               # fetch_aeroapi.py, fetch_opensky.py, fetch_faa_nas.py, fetch_nws_weather.py
├── api/main.py                 # FastAPI REST service over Gold Parquet tables
├── dashboard/                  # app.py, live_radar.py (Streamlit 3-tab operational dashboard)
├── benchmark/                  # run_benchmark.py, analyze_results.py, results/benchmark_summary.csv
├── notebooks/                  # 01_data_cleaning_and_eda.ipynb, 02_colab_pipeline.ipynb
├── models/                     # Committed 40-hub gbt_pipeline_v1, rf_pipeline_v1, lane_mean.parquet,
│                               # baseline_hub_hour_dow.*, ablation.csv, tuning_results.csv, error_by_*.csv
├── tests/                      # 76 pytest checks (68 pure-Python + 8 Spark integration tests)
├── .github/workflows/ci.yml    # GitHub Actions CI running full pytest suite with Spark 3.5.1 on Ubuntu
├── reports/figures/            # EDA figures
├── docs/                       # Poster (48 x 36 in), Midterm, and Phase 2 design docs
├── EVALUATION.md               # Full model, ablation, tuning, error-analysis, and benchmark evaluation
├── PIPELINE_SPECIFICATION.md   # Architecture and schema specification
└── CS5540_HyperLogistics_Project_Summary.md
```

`data/` and `checkpoints/` are generated at runtime and ignored by Git.

---

## 6. Exploratory data analysis

```bash
python scripts/run_eda.py
jupyter notebook notebooks/01_data_cleaning_and_eda.ipynb
```

`scripts/run_eda.py` filters cancelled and diverted trips across all 40 hubs in `config/hubs.py`, converts local hub schedules to UTC, parses NOAA `WND`, `VIS`, `CIG`, and `TMP` fields, tests the stream-static weather join, and saves plots to `reports/figures/` plus a Bronze sample to `data/bronze/sample_cleaned_joined.parquet`.

---

## 7. Research questions and evaluation matrix

* **RQ1 (Model accuracy):** Does takeoff ETA prediction using departure delay, origin and destination weather, schedule hours, and historical route cushion beat:
  1. *Baseline 1 (No change):* arrival delay equals departure delay ($\Delta\text{delay} = 0$)
  2. *Baseline 2 (Lane mean):* training-split origin-to-destination route mean $\Delta\text{delay}$
* **RQ2 (Streaming scalability):** How do throughput (`records/s`), micro-batch duration (`p50`/`p95` ms), and end-to-end latency scale across:
  * Spark CPU cores (`2`, `4`)
  * Shuffle partitions (`8`, `64`)
  * Kafka producer rates (`1k`, `5k` events/sec)
  * `maxOffsetsPerTrigger` (`5k`, `20k`)
* **RQ3 (Operational interface):** Can one serving layer combine live hub disruption monitoring, historical replay, and model error diagnostics through FastAPI and Streamlit?

---

## 8. Results

Full tables, methodology, hyperparameter tuning logs, and honest limits live in [`EVALUATION.md`](EVALUATION.md). Headlines across all **40 U.S. hubs** (`14,384,130` training flights in `2018-2023`, `3,041,312` unseen `2024` validation flights):

| Question | Measured result |
|---|---|
| **RQ1:** better than baselines? | Yes. Tuned 14-feature GBT (`maxDepth=8, maxIter=90, stepSize=0.07, minInstancesPerNode=500, subsamplingRate=0.8`) reaches **9.77 min MAE** (`13.82` min RMSE) on `3,041,312` unseen `2024` flights, beating **12.04 min** for "no change" (**18.8% better**) and **10.19 min** for the historical lane mean (**4.1% better**), winning at **40 of 40 hubs**. |
| Does weather help, and did tuning overfit? | On the default depth-5 GBT, adding origin and destination weather lowers holdout MAE by **1.12%** (`10.05` to `9.94` min). Warm-JVM capacity tuning (`scripts/tune_gbt.py`) lowers MAE another `0.17` min to **9.77 min** with only `+0.016` min excess train-to-validation gap (`9.47` train vs `9.77` val MAE). |
| Where is the model right and wrong? | `57.2%` of 2024 flights recover more than 5 minutes in the air, where GBT cuts error sharply (`4.58` vs `10.31` min in the 5-to-15 min recovery bucket). Flights that lose more than 15 minutes in the air (`7.0%` of traffic) remain under-predicted (`32.16` min MAE), so the model is an ETA adjuster, not a severe-storm alarm. |
| **RQ2:** how fast does it stream? | Best of 12 Colab configurations reaches **4,448 records/s** (`4` cores, `8` shuffle partitions, `5k` events/s producer, `20k` max offsets) with **3.48 s** median batch time (`3.92 s` p95). Moving from 2 to 4 cores boosts throughput **22%**, while shuffle partitions (`8` vs `64`) change throughput by under 5%. |
| **Live vs. history:** does recovery hold today? | Yes, on a 1-evening sample. Across `505` landed FlightAware flights (`77.2%` recovering time in the air), historical hub baselines and the Python live estimator cut MAE from **12.19 min to 9.67 min** (**20.7% better**). Live rows use `live/aeroapi_predictor.py` rather than the Spark GBT binary (`EVALUATION.md` section 8). |
| Repeat-safe Gold writes? | Yes. Replaying the exact same micro-batch adds **0 duplicate rows** to Gold Parquet (`tests/test_dedupe_spark.py` on real Spark). |

The streaming benchmark is provisional (4 active micro-batches per configuration in a single sweep), and end-to-end latency (`6.4` to `13.0` s) includes Kafka queue wait time. Dashboard charts display model predictions rather than observed post-landing delays unless labeled otherwise.

---

## 9. Testing

```bash
# Full suite on Linux or Google Colab (Java 17 + Python <= 3.11 for PySpark 3.5)
pip install pytest pandas numpy pyyaml pyarrow streamlit plotly tqdm pyspark==3.5.1
python -m pytest -v

# Fast pure-Python suite on macOS or Windows without starting a Spark JVM (< 2 seconds)
python -m pytest --ignore=tests/test_dedupe_spark.py --ignore=tests/test_error_analysis_spark.py --ignore=tests/test_live_processor_spark.py -q
```

The 76-test suite (`68` pure-Python unit/contract tests plus `8` Spark JVM integration tests) covers 40-hub registry parity, train/serve UTC timezone contracts, `--reset` path isolation, `PipelineModel` fail-fast guards, dashboard story and insight helpers, HTTP `429` producer backoff, error-analysis aggregations, and repeat-safe Gold deduplication. GitHub Actions CI runs the full suite with Java 17 and PySpark 3.5.1 on every push.

---

## 10. Troubleshooting

| Symptom | Fix |
|---|---|
| Replay tab shows "synthetic demo data" | `data/gold/` is empty because replay has not run locally. Start Kafka, `02_kafka_producer.py`, and `03_stream_processor.py`, or inspect Tab 1 (**Live Radar**) and Tab 3 (**Model Insights**), which read committed `models/` tables directly. |
| Kafka connection refused | Run `docker compose up -d` and connect to `localhost:9094` from the host. |
| `docker compose up` fails to reach Docker API | Start Docker Desktop and wait until its engine status reports running. |
| Kafka container exits with `NodeExists` | Stale ZooKeeper state after an unclean stop. Run `docker compose down` and then `docker compose up -d`. |
| `HADOOP_HOME and hadoop.home.dir are unset` (Windows) | Do not run Spark scripts directly on Windows. Use `docker compose --profile live up -d live-processor` or run batch scripts on Google Colab/Linux. |
| Live processor stops with `UnknownTopicOrPartitionException` | Kafka topics do not exist yet. Start `python live/live_producer.py` once, then restart `live-processor`. |
| Live Radar badge says `RECORDED` | Live Gold output is older than 3 minutes or not running; the tab is displaying the latest saved snapshot. Start `live_producer.py` and `live-processor` and wait ~60 seconds. |
| OpenSky rate-limit warnings (`HTTP 429`) | Run `live_producer.py --no-opensky`; FAA NAS, NWS weather, and AeroAPI continue unaffected. |
| `StreamlitInvalidWidthError` | Pass `width='stretch'` (not `'container'`) to `st.plotly_chart`. |
| Replay shows stale rows from a previous test | Re-run the stream processor with `--reset`. |

---

## 11. License and academic integrity

Developed as graduate coursework for COMP-SCI 5540 Principles of Big Data Management at the University of Missouri-Kansas City (UMKC). All code, experiments, and documentation follow UMKC academic integrity standards.
