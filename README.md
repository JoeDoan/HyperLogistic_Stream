# HyperLogistics-Stream

**Real-Time Hub-to-Hub Logistics ETA Prediction with Kafka and Spark Structured Streaming**

* **Course:** COMP-SCI 5540 Principles of Big Data Management (Fall 2026)  
* **Institution:** School of Science and Engineering, University of Missouri-Kansas City (UMKC)  
* **Instructor:** Dr. Md Yusuf Sarwar Uddin  
* **Authors:** Tony Nguyen (`tmnc62@umsystem.edu`) & Joe Doan (`jdbc6@umsystem.edu`)  

---

## 1. Project Overview

In middle-mile freight logistics, unexpected transit delays create severe scheduling bottlenecks and dock-door inefficiencies across downstream distribution hubs. **HyperLogistics-Stream** is an end-to-end distributed big data processing pipeline designed to ingest, clean, enrich, and predict arrival delay updates ($\Delta\text{delay} = \text{ArrDelay} - \text{DepDelay}$) and estimated times of arrival (ETA) in real time.

### 4-Tier Big Data Architecture
1. **Ingestion Layer:** Apache Kafka replays high-velocity trip events in chronological `WheelsOff` order at controlled throughput rates (1k, 5k, 20k events/sec).
2. **Storage Layer (Medallion Architecture):** HDFS/S3-backed Parquet storage partitioned across **Bronze** (raw streaming dumps), **Silver** (cleaned and weather-joined records), and **Gold** (scored dynamic ETAs and hub arrival rollups).
3. **Processing & ML Layer:** PySpark Structured Streaming performs stateful window aggregations and stream-static broadcast joins against NOAA surface weather stations; Spark MLlib (`GBTRegressor` and `RandomForestRegressor`) scores inbound trips dynamically.
4. **Serving & Dashboard Layer:** FastAPI exposes low-latency REST endpoints to query hub congestion, consumed by an interactive Streamlit live operations dashboard.

---

## 2. Prerequisites & Data Setup (REQUIRED)

> [!IMPORTANT]
> The `data/` directory contains **~11 GB** of raw records and is **strictly excluded from this Git repository** via `.gitignore`. You **must run the data download scripts first** before attempting to execute any exploratory analysis, notebooks, or streaming jobs.

### Step 1: Install Python Dependencies
Ensure Python 3.10+ is installed, then set up required packages:
```bash
pip install pandas numpy matplotlib seaborn tqdm nbformat pyarrow
```
*(For PySpark and streaming components, ensure Java 11/17 and Apache Spark 3.5+ are configured on your system).*

### Step 2: Download Raw Datasets (~11 GB Total)

#### A. Download BTS On-Time Flight Records (2022–2024, ~9.8 GB)
Downloads all 36 monthly archives and extracts the raw CSVs into `data/raw/bts/`:
```bash
python3 scripts/download_bts.py --years 2022 2023 2024 --extract
```
* **2022–2023 (~6.5 GB):** Offline training partition for Spark MLlib models and historical lane delay baselines.
* **2024 (~3.3 GB):** Streaming replay partition for Kafka real-time ingestion and evaluation.

#### B. Download NOAA ISD Hourly Weather Data (2022–2024, ~911 MB)
Fetches hourly surface weather (wind speed, visibility, temperature) for top 40 US logistics and air cargo hubs into `data/raw/noaa/`:
```bash
python3 scripts/download_noaa.py --years 2022 2023 2024
```

---

## 3. Repository Structure

```
project_BigData/
├── .gitignore                                 # Ignores data/, Parquet, checkpoints, and OS artifacts
├── README.md                                  # Project overview and setup instructions
├── PIPELINE_SPECIFICATION.md                  # Comprehensive end-to-end architecture specification
├── CS5540_HyperLogistics_Project_Summary.md    # Project overview, course rubric & milestones (VN)
├── project_proposal_big_data.tex              # 1-page academic project proposal (LaTeX source)
├── project_proposal_big_data_Tony_Joe.pdf     # Compiled proposal document
├── Hyperlogistics_Stream_Tony_Joe.pptx        # Presentation slide deck
├── proposal_preview.png                       # Visual overview preview
├── scripts/
│   ├── download_bts.py                        # BTS multi-year bulk downloader with auto-extract
│   ├── download_noaa.py                       # NOAA ISD AWS S3 weather downloader & station resolver
│   ├── run_eda.py                             # Data cleaning, UTC normalization & figure generation
│   └── build_notebook.py                      # Programmatic builder for interactive EDA notebook
├── notebooks/
│   └── 01_data_cleaning_and_eda.ipynb         # Interactive Jupyter Notebook for data inspection & EDA
└── reports/
    └── figures/                               # Publication-ready analytical plots
        ├── fig1_delay_distributions.png       # Flight departure vs. arrival delay skew
        ├── fig2_wind_speed_vs_delay.png       # Impact of destination surface wind on delay
        ├── fig3_visibility_vs_delay.png       # Flight delay progression under degraded visibility
        ├── fig4_hourly_departure_patterns.png # Diurnal traffic congestion volume by UTC hour
        └── fig5_hub_congestion_comparison.png  # Average delay ranking across key cargo & passenger hubs
```

---

## 4. Running the EDA & Generating Figures

Once the raw datasets are downloaded, you can execute the cleaning and exploratory pipeline:

```bash
# Run the complete data cleaning and figure generation pipeline
python3 scripts/run_eda.py
```

This script:
1. Filters cancelled and diverted trips.
2. Aligns airport local timestamps to universal UTC datetimes using IANA timezone lookups.
3. Cleans compound NOAA ISD weather strings (WND, VIS, TMP).
4. Simulates a stream-static join between airborne flights and destination weather.
5. Saves high-resolution visualization charts to `reports/figures/` and a validated Bronze dataset sample to `data/bronze/sample_cleaned_joined.parquet`.

You can also explore the analysis interactively inside Jupyter:
```bash
jupyter notebook notebooks/01_data_cleaning_and_eda.ipynb
```

---

## 5. Research Questions & Evaluation Matrix

* **RQ1 (Model Accuracy):** Does dynamic ETA prediction conditioning on departure delay, real-time arrival weather, and rolling lane congestion outperform:
  1. *Baseline 1:* Delay unchanged ($\Delta\text{delay} = 0$).
  2. *Baseline 2:* 30-day historical origin-destination lane mean.
* **RQ2 (System Scalability & Benchmarks):** Measure end-to-end p50/p95 latency, batch processing time, and throughput (records/sec) across variations in:
  * Spark CPU cores ($2, 4, 8$)
  * Shuffle partitions ($8, 64, 200$)
  * Kafka ingestion rates ($1\text{k}, 5\text{k}, 20\text{k}\text{ events/sec}$)
  * Micro-batch trigger offsets (`maxOffsetsPerTrigger`).
* **RQ3 (Operational Dispatcher Interface):** Serve live aggregated predictions to operations staff through FastAPI and a live interactive Streamlit dashboard.

---

## 6. License & Academic Integrity

This project is developed as part of academic coursework for COMP-SCI 5540 Principles of Big Data Management at UMKC. All code and reports follow institutional honor codes and academic integrity standards.
