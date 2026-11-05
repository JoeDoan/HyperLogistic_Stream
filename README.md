# HyperLogistics-Stream

**Real-Time Hub-to-Hub Logistics ETA Prediction with Kafka and Spark Structured Streaming**

[![CI](https://github.com/mosomo82/HyperLogistic_Stream/actions/workflows/ci.yml/badge.svg?branch=feat/tony-pipeline)](https://github.com/mosomo82/HyperLogistic_Stream/actions/workflows/ci.yml)

| | |
|---|---|
| **Course** | COMP-SCI 5540 Principles of Big Data Management (Fall 2026) |
| **Institution** | School of Science and Engineering, University of Missouri-Kansas City (UMKC) |
| **Instructor** | Dr. Md Yusuf Sarwar Uddin |
| **Authors** | Tony Nguyen (`tmnc62@umsystem.edu`) & Joe Doan (`jdbc6@umsystem.edu`) |

## Contents

1. [Project Overview](#1-project-overview)
2. [Quick Start](#2-quick-start)
3. [Data Setup](#3-data-setup-required-for-batch-and-replay)
4. [Running the Pipeline](#4-running-the-pipeline)
5. [Repository Structure](#5-repository-structure)
6. [Exploratory Data Analysis](#6-exploratory-data-analysis)
7. [Research Questions](#7-research-questions--evaluation-matrix)
8. [Results](#8-results)
9. [Testing](#9-testing)
10. [Troubleshooting](#10-troubleshooting)
11. [License & Academic Integrity](#11-license--academic-integrity)

---

## 1. Project Overview

In middle-mile freight logistics, unexpected transit delays create scheduling bottlenecks and dock-door inefficiencies across downstream hubs. **HyperLogistics-Stream** is an end-to-end big data pipeline that ingests, cleans, enriches, and predicts arrival delay changes ($\Delta\text{delay} = \text{ArrDelay} - \text{DepDelay}$) and ETAs in real time.

> Flights are used as a **public proxy** for hub-to-hub freight movement.

### Architecture

1. **Ingestion:** Apache Kafka replays historical trips in chronological `WheelsOff` order at controlled rates (1k, 5k, 20k events/sec). A **live producer** also publishes OpenSky aircraft positions, FAA NAS airport status, and NWS weather.
2. **Storage (Medallion):** Parquet tables in **Bronze** (raw), **Silver** (cleaned, weather-joined), and **Gold** (scored ETAs and hub rollups).
3. **Processing & ML:** PySpark Structured Streaming with windowed aggregations and stream-static joins against NOAA weather; Spark MLlib (`GBTRegressor`, `RandomForestRegressor`) scores inbound trips.
4. **Serving:** FastAPI REST endpoints and a Streamlit dashboard (**Live Radar** and **Replay & Benchmark** tabs).

---

## 2. Quick Start

Runs the live radar and dashboard. It does **not** need the 11 GB dataset. If no pipeline output exists yet, the dashboard falls back to synthetic demo data.

**Prerequisites:** Python 3.10+, Java 11/17, Apache Spark 3.5+, Docker Desktop.

```bash
# 1. Clone and install
git clone <your-repo-url> HyperLogistic_Stream
cd HyperLogistic_Stream
pip install -r requirements.txt

# 2. (Optional) copy environment template
cp .env.example .env            # Windows: copy .env.example .env

# 3. Start Kafka (Zookeeper + broker + topic)
docker compose up -d

# 4. Terminal A - live feeds -> Kafka (add --record to save a session for replay)
python live/live_producer.py --record

# 5. Terminal B - Spark live processor -> Gold tables
python pipeline/04_live_processor.py

# 6. Terminal C - dashboard
streamlit run dashboard/app.py
```

Open **http://localhost:8501**.

| Service | URL |
|---|---|
| Dashboard | http://localhost:8501 |
| API docs (if started) | http://localhost:8000/docs |
| Spark UI | http://localhost:4040 |
| Kafka (external) | `localhost:9094` |

**Dashboard only (no Kafka/Spark):** `streamlit run dashboard/app.py` shows demo data.
**Offline check of the feeds:** `python live/live_producer.py --dry-run --once`

---

## 3. Data Setup (required for batch and replay)

> [!IMPORTANT]
> `data/` holds **~11 GB** of raw records and is **excluded from Git** via `.gitignore`. Download it before running training, replay, or the EDA.

```bash
# BTS On-Time Flight Records 2022-2024 (~9.8 GB) -> data/raw/bts/
python scripts/download_bts.py --years 2022 2023 2024 --extract

# NOAA ISD hourly weather, top 40 hubs (~911 MB) -> data/raw/noaa/
python scripts/download_noaa.py --years 2022 2023 2024
```

* **2022-2023 (~6.5 GB):** training partition and historical lane baselines.
* **2024 (~3.3 GB):** streaming replay partition.

---

## 4. Running the Pipeline

Run from the project root. Start Kafka first (`docker compose up -d`).

| Step | Command | Purpose |
|---|---|---|
| 1. Train | `python pipeline/01_batch_train.py` | Train models, run the weather ablation, build the baseline table (years from the config; `--skip-rf` to save time; long, use Colab) |
| 2. Replay producer | `python pipeline/02_kafka_producer.py --rate 1000` | Replay 2024 trips into Kafka (`--limit`, `--loop`) |
| 3. Replay processor | `python pipeline/03_stream_processor.py` | Score and write Gold tables (`--reset`, `--test`, `--cores`, `--shuffle-partitions`) |
| 4. Live producer | `python live/live_producer.py --record` | FAA + weather + OpenSky to Kafka (`--no-opensky`, `--dry-run`, `--once`) |
| 5. Live processor | `python pipeline/04_live_processor.py` | Live Spark stream (`--reset`, `--test`) |
| 6. API | `uvicorn api.main:app --port 8000` | REST endpoints for hub congestion |
| 7. Dashboard | `streamlit run dashboard/app.py` | Live Radar, Replay & Benchmark, Model Insights |
| 8. Benchmark | `python benchmark/run_benchmark.py --quick` | RQ2 grid (`--quick` = reduced grid) |
| 9. Analyze | `python benchmark/analyze_results.py` | Benchmark charts and summary |
| 10. Error analysis | `python pipeline/04_error_analysis.py --config config/pipeline_config_colab.yaml` | Per-hub, hour, month and delay-bucket error tables for the Model Insights tab |

Defaults live in [`config/pipeline_config.yaml`](config/pipeline_config.yaml). Override with `--config <path>` or `HYPERLOGISTICS_CONFIG`.

---

## 5. Repository Structure

```
HyperLogistic_Stream/
├── README.md
├── requirements.txt            # Python dependencies
├── docker-compose.yml          # Zookeeper + Kafka + topic init
├── .env.example                # Environment variable template
├── config/                     # pipeline_config.yaml
├── scripts/                    # download_bts.py, download_noaa.py, run_eda.py,
│                               # build_notebook.py, setup_colab.sh
├── pipeline/
│   ├── 01_batch_train.py       # Spark MLlib training
│   ├── 02_kafka_producer.py    # Historical replay producer
│   ├── 03_stream_processor.py  # Structured Streaming -> Gold
│   ├── 04_live_processor.py    # Live-topic Spark processor
│   └── 04_error_analysis.py    # Error tables for the served model
├── live/                       # live_producer.py, fetch_opensky.py,
│                               # fetch_faa_nas.py, fetch_nws_weather.py
├── api/main.py                 # FastAPI service
├── dashboard/                  # app.py, live_radar.py (Streamlit)
├── benchmark/                  # run_benchmark.py, analyze_results.py, results/
├── notebooks/                  # EDA and Colab pipeline notebooks
├── models/                     # Trained models, ablation.csv, baseline and error tables
├── tests/                      # pytest suite (run by CI)
├── .github/workflows/ci.yml    # CI: pytest on every push
├── reports/figures/            # EDA plots
├── docs/                       # Midterm/ and Phase 2/ (plans, run steps)
├── EVALUATION.md               # Model, ablation, error-analysis and benchmark results
├── PIPELINE_SPECIFICATION.md   # Full architecture specification
└── CS5540_HyperLogistics_Project_Summary.md
```

`data/` and `checkpoints/` are generated locally and ignored by Git.

---

## 6. Exploratory Data Analysis

```bash
python scripts/run_eda.py
jupyter notebook notebooks/01_data_cleaning_and_eda.ipynb
```

The EDA script filters cancelled/diverted trips, converts local times to UTC, cleans NOAA `WND`/`VIS`/`TMP` fields, simulates the stream-static weather join, and writes figures to `reports/figures/` plus a Bronze sample to `data/bronze/sample_cleaned_joined.parquet`.

---

## 7. Research Questions & Evaluation Matrix

* **RQ1 (Model Accuracy):** Does ETA prediction using departure delay, arrival weather, and rolling lane congestion beat:
  1. *Baseline 1:* delay unchanged ($\Delta\text{delay} = 0$)
  2. *Baseline 2:* 30-day historical origin-destination lane mean
* **RQ2 (Scalability):** Measure p50/p95 latency, batch time, and throughput across:
  * Spark cores (2, 4, 8)
  * Shuffle partitions (8, 64, 200)
  * Kafka rates (1k, 5k, 20k events/sec)
  * `maxOffsetsPerTrigger`
* **RQ3 (Operational Interface):** Serve live predictions through FastAPI and Streamlit.

---

## 8. Results

Full tables, method and limits are in [`EVALUATION.md`](EVALUATION.md). Headlines:

| Question | Result |
|---|---|
| **RQ1:** better than baselines? | Yes, modestly. Validation MAE on 2024 is **10.53 min**, against 12.75 for "no change" and 10.80 for the lane mean (17% and 2.5% better). |
| Does weather help? | A little: **1.0%** lower MAE with destination and origin weather. Lane and schedule features do most of the work. |
| Where is the model wrong? | It beats "no change" mainly on flights that arrive early. Flights that end up more than 15 min late are under-predicted by about 34 min, so it is not a severe-delay warning tool. |
| **RQ2:** how fast? | Best of 12 configurations: about **4,450 records/s** (4 cores, 8 shuffle partitions, 5k events/s producer, 20k max offsets). 4 cores beat 2 by about 22%. Shuffle partitions made no clear difference. |
| Repeat-safe writes? | Yes. Replaying the same batch adds 0 rows to Gold (tested on real Spark). |

The benchmark is provisional (4 batches per configuration, one run each), and end-to-end latency includes Kafka backlog time. Predictions on the dashboard are model outputs, not observed delays.

---

## 9. Testing

```bash
pip install pytest pandas numpy pyyaml pyarrow streamlit plotly pyspark==3.5.1
python -m pytest -v
```

Covers output isolation and `--reset` safety, config consistency, the dashboard's story helpers, error-analysis aggregation, and repeat-safe Gold writes. The Spark tests need Java and Python 3.11 or older (pyspark 3.5 imports `distutils`), and two of them skip on Windows without Hadoop's `winutils`. CI runs the same suite on every push.

---

## 10. Troubleshooting

| Symptom | Fix |
|---|---|
| Dashboard shows "synthetic demo data" | Gold tables are empty. Start the producer and processor, then wait for a micro-batch |
| Kafka connection refused | Run `docker compose up -d`. Use `localhost:9094` from the host |
| OpenSky rate-limit warnings | Run with `--no-opensky`. FAA + weather carry the story |
| `StreamlitInvalidWidthError` | Use `width='stretch'` (not `'container'`) in `st.plotly_chart` |
| Replay shows repeated rows | Rerun the processor with `--reset` |

---

## 11. License & Academic Integrity

Developed as coursework for COMP-SCI 5540 Principles of Big Data Management at UMKC. All code and reports follow institutional honor codes and academic integrity standards.
