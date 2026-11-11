# EVALUATION.md: HyperLogistic_Stream

> CS 5540. RQ1 accuracy, ablation, hyperparameter tuning, and error tables come directly from Spark
> training outputs committed in `models/` (`ablation.csv`, `tuning_results.csv`,
> `training_metrics.json`, `baseline_hub_hour_dow.csv`, `error_by_*.csv`). Section 4's
> 12-configuration Colab streaming sweep comes from `benchmark/results/benchmark_summary.csv`, and
> correctness counts come from `pytest`.

## 1. What is evaluated

| Question | Evidence | Section |
|---|---|---|
| Does the model predict delay change better than simple baselines? | `models/training_metrics.json`, `models/tuning_results.csv` | 2 |
| Do weather features and capacity tuning help without overfitting? | `models/ablation.csv`, `models/tuning_results.csv` | 3 |
| Where is the model wrong across all 40 hubs? | `models/error_by_*.csv` | 3.1 |
| How fast is the streaming pipeline, and what drives it? | `benchmark/results/benchmark_summary.csv` | 4 |
| Are Gold writes repeat-safe, and are contracts tested? | `tests/test_dedupe_spark.py`, `tests/` | 5 |
| Do the historical patterns hold on live flights? | `models/live_vs_historical.csv` | 8 |
| What can we **not** claim? | | 6 |

The target is `delta_delay` (arrival delay minus departure delay, in minutes). Commercial flights
serve as a public proxy for middle-mile freight movement across the 40 U.S. hubs in
`config/hubs.py`.

**How the evaluation works**

- **Time-based split.** The model trains on 2018, 2019, 2021, 2022, and 2023 (`14,384,130` flights)
  and is scored on unseen 2024 (`3,041,312` flights across `1,388` lanes and `40` hubs). 2020 is
  left out because pandemic schedules are not representative.
- **Metric.** MAE is the average arrival-time error in minutes (lower is better). RMSE punishes
  large misses more heavily. "Better than no change" means a lower MAE than assuming the departure
  delay stays unchanged all the way to the destination.
- **Same yardstick everywhere.** Both baselines, Random Forest, default GBT, and the tuned champion
  GBT are scored on the exact same `3,041,312` holdout 2024 flights. Section 3.1 slices that same
  holdout set by hub, departure hour, month, and actual delay bucket.
- **Live check.** Section 8 repeats the comparison on `505` FlightAware flights that landed while
  the live feed was recording, using the lightweight Python rule-based estimator (see caveats there).

## 2. Model vs. baselines (40 U.S. hubs)

Train on 2018, 2019, 2021, 2022, and 2023 (`14,384,130` flights). Validate on unseen 2024
(`3,041,312` flights). MAE and RMSE are in minutes, lower is better.

| Model | Features | Train MAE | Val MAE | Val RMSE | vs. "no change" |
|---|---|---|---|---|---|
| Baseline 1: predict 0 (no change) | 0 | 11.846 | 12.042 | 15.801 | n/a |
| Baseline 2: training lane mean | 1 | 9.905 | 10.193 | 14.614 | 15.4% better |
| Random forest, full weather (100 trees, depth 5) | 14 | 9.800 | 10.079 | 14.459 | 16.3% better |
| GBT default, full weather (50 trees, depth 5) | 14 | 9.671 | 9.940 | 14.155 | 17.5% better |
| **GBT tuned champion (`iter4`, 90 trees, depth 8)** | 14 | **9.469** | **9.774** | **13.822** | **18.8% better** |

Reading it plainly:

- Knowing the **origin-destination lane** removes most of the raw error (`12.042` down to `10.193`
  min, or `1.85` of the `2.27` minutes gained by the champion GBT).
- On `14.38M` training rows, the default depth-5 GBT was slightly underfitting (`9.940` val MAE).
  Controlled capacity tuning (`maxDepth=8, maxIter=90, stepSize=0.07, maxBins=128,
  minInstancesPerNode=500, subsamplingRate=0.8`) lowers holdout MAE to **9.774 min**, beating the
  lane-mean baseline by **0.420 min (4.12%)** and beating Random Forest by **0.305 min**.
- Overfitting remained negligible: `baseline_lane_mean` already exhibits a `+0.288` min temporal
  shift between `2018-2023` (`9.905`) and `2024` (`10.193`), and the tuned GBT's train-to-val gap is
  `+0.304` min (`+0.016` min excess gap).

### 2.1 The models in this project

| Model | What it is | Where it runs | Role |
|---|---|---|---|
| **Baseline 1: no change** | Predicts `delta_delay` = 0, so arrival delay equals departure delay | anywhere | Floor: what standard flight boards imply |
| **Baseline 2: lane mean** | Mean `delta_delay` of each `Origin -> Dest` lane (`1,388` lanes) in `2018-2023` | Spark (`models/lane_mean.parquet`) | Shows how much static route padding explains |
| **GBT tuned champion, 14 features** | Spark MLlib `GBTRegressor` (`90` trees, `maxDepth=8`, `stepSize=0.07`, `minInstancesPerNode=500`, `subsamplingRate=0.8`) | Spark, committed in `models/gbt_pipeline_v1` | Primary model served by `03_stream_processor.py` and scored in sections 2 to 3.1 |
| Random forest | 100-tree `RandomForestRegressor` on the same 14 features | Spark, committed in `models/rf_pipeline_v1` | Bagging comparison baseline |
| **Live estimator** | Calibrated Python rule formula using hub/hour averages plus departure-delay and live NWS weather adjustments | Pure Python (`live/aeroapi_predictor.py`) | Scores live FlightAware flights without a local Spark JVM; **not** the Spark GBT |

## 3. Weather ablation and GBT hillclimb

### 3.0 Weather ablation (fixed depth-5 GBT across 40 hubs, `models/ablation.csv`)

| Step | Added features | Val MAE | Val RMSE | Gain vs. no weather |
|---|---|---|---|---|
| `gbt_no_weather` | none (6 lane + schedule features) | 10.053 | 14.330 | 0.00% |
| `gbt_dest_wind_vis_temp` | + destination wind, visibility, temperature | 10.017 | 14.278 | 0.36% |
| `gbt_dest_plus_ceiling` | + destination ceiling | 10.005 | 14.259 | 0.48% |
| `gbt_full_weather` | + origin wind, visibility, ceiling, temperature | **9.940** | **14.155** | **1.12%** |

### 3.0b Controlled 5-stage GBT tuning (`scripts/tune_gbt.py`, `models/tuning_results.csv`)

| Stage | Hyperparameters (`depth / trees / lr / bins / minLeaf / subsample`) | Train MAE | Val MAE | Val RMSE | Excess gap | Outcome |
|---|---|---|---|---|---|---|
| `iter0_baseline_default` | `5 / 50 / 0.10 / 32 / 1 / 1.0` | 9.671 | 9.940 | 14.155 | -0.018 | Baseline |
| `iter1_fine_bins_128` | `5 / 50 / 0.10 / 128 / 100 / 1.0` | 9.673 | 9.950 | 14.170 | -0.011 | Rejected (no depth gain) |
| `iter2_depth6_leaf200` | `6 / 65 / 0.10 / 128 / 200 / 1.0` | 9.586 | 9.869 | 14.019 | -0.005 | Accepted (`-0.071` min) |
| `iter3_depth7_stochastic` | `7 / 85 / 0.08 / 128 / 300 / 0.8` | 9.514 | 9.809 | 13.895 | +0.007 | Accepted (`-0.131` min) |
| **`iter4_depth8_capacity_probe`** | **`8 / 90 / 0.07 / 128 / 500 / 0.8`** | **9.469** | **9.774** | **13.822** | **+0.016** | **Champion (`-0.167` min)** |
| `iter5_adaptive_refinement` | `8 / 115 / 0.055 / 128 / 600 / 0.8` | 9.473 | 9.776 | 13.829 | +0.015 | Rejected (plateau `+0.003`) |

**Finding:** Weather lowers holdout MAE by **0.112 min (1.12%)** on the fixed depth-5 GBT, and
regularized depth-8 capacity adds another **0.167 min** gain (`9.940` down to `9.774` min). Split
importances on the depth-8 GBT assign `0.200` to the eight weather columns and `0.426` to schedule
time-of-day/weekday columns; because continuous columns offer more split candidates than the single
pre-aggregated `LaneMeanDelay` feature (`0.054` split share), we treat the ablation table as the
authoritative measure of weather impact.

### 3.1 Error analysis: where the model is wrong (`models/error_by_*.csv`)

`pipeline/04_error_analysis.py` scores the served 90-tree champion GBT (`models/gbt_pipeline_v1`)
across all `3,041,312` unseen 2024 flights (`40` hubs). Weighted over all hubs, its MAE is
**9.774 min**, matching `training_metrics.json`.

**By origin hub** (selected hubs out of 40; MAE in minutes):

| | Hub | 2024 flights | GBT MAE | Lane-mean MAE | No-change MAE |
|---|---|---|---|---|---|
| Worst | JFK | 85,475 | 13.32 | 14.11 | 17.15 |
| | EWR | 89,769 | 12.23 | 13.02 | 15.48 |
| | BOS | 110,299 | 11.44 | 11.96 | 14.46 |
| Cargo / Midwest | ORD | 153,890 | 10.49 | 11.23 | 12.81 |
| | MEM | 18,576 | 9.16 | 9.57 | 11.50 |
| | SDF | 19,719 | 9.10 | 9.57 | 11.01 |
| | MCI | 39,011 | 8.70 | 8.95 | 10.60 |
| | ATL | 163,095 | 8.54 | 8.70 | 10.00 |
| | PHX | 118,045 | 8.43 | 8.57 | 10.44 |
| Best | SMF | 42,195 | 7.36 | 7.55 | 9.34 |

- Northeast airspace hubs remain hardest (`JFK` at `13.32` min is `81%` higher than Sacramento `SMF`
  at `7.36` min).
- The tuned GBT beats both "no change" and the lane-mean baseline at **all 40 of 40 hubs**, cutting
  `0.12` min (`AUS`) to `0.79` min (`EWR`, `JFK`, `PHL`) off the lane-mean error.

**By actual outcome** (`3,041,312` holdout 2024 flights grouped by realized `delta_delay`):

| Actual delta delay | Flights | Share | GBT MAE | Lane-mean MAE | No-change MAE | GBT bias |
|---|---|---|---|---|---|---|
| Early >15 min | 651,055 | 21.4% | 14.41 | 15.60 | 22.20 | +14.40 |
| Early 5-15 min | 1,089,815 | 35.8% | 4.58 | 4.70 | 10.31 | +3.87 |
| Within 5 min | 782,469 | 25.7% | 5.42 | 4.78 | 2.61 | -4.65 |
| Late 5-15 min | 306,534 | 10.1% | 14.07 | 14.61 | 8.67 | -13.85 |
| Late >15 min | 211,439 | 7.0% | 32.16 | 35.48 | 29.48 | -32.06 |

(Bias is prediction minus actual; negative means the model predicted too little delay.)

- **The overall 18.8% MAE reduction comes from the 57.2% of flights (`1,740,870`) that recover more
  than 5 minutes in the air.** On on-time and late-worsening flights, predicting 0 has lower conditional
  error, though GBT still improves on the lane mean in the late buckets (`32.16` vs `35.48` min).
- **Severe in-air delays remain missed.** Flights losing more than 15 minutes in the air (`7.0%`)
  are under-predicted by `32.1` minutes on average, so the model is an ETA adjuster for normal
  operations rather than a severe-storm alarm.

**By hour and season:**

- Error is highest for late-evening departures around `22:00` to `00:00` UTC (MAE `10.40` to `10.65`
  min) and lowest around `09:00` to `10:00` UTC (`8.52` to `8.70` min).
- Weighted monthly MAE ranges from `8.75` min (October) to `10.74` min (January). Flights make up
  the least time in May (mean actual delta `-3.94` min) and the most in February (`-7.29` min).
- The largest single month-hub bias is `BOS` in February (`+5.43` min).

The dashboard's **Model Insights** tab renders all four 40-hub tables and the seasonality heatmap.

## 4. Pipeline benchmark

12 configurations, 4 batches each, 48 batches in total. Config names read
`c<cores>_sp<shuffle partitions>_r<producer rate>_mo<max offsets per trigger>`.

| Config | Mean throughput (rec/s) | p50 batch (ms) | p95 batch (ms) | Median p95 E2E latency (ms) |
|---|---|---|---|---|
| c4_sp8_r5000_mo20000 | 4,448 | 3,481 | 3,923 | 8,967 |
| c2_sp8_r5000_mo20000 | 3,640 | 4,525 | 5,404 | 13,041 |
| c2_sp64_r5000_mo20000 | 3,484 | 4,172 | 5,274 | 12,019 |
| c4_sp8_r5000_mo5000 | 1,900 | 2,405 | 4,406 | 11,252 |
| c2_sp64_r5000_mo5000 | 1,743 | 2,636 | 4,849 | 12,536 |
| c2_sp64_r1000_mo20000 | 1,376 | 2,627 | 4,349 | 7,581 |
| c4_sp8_r1000_mo5000 | 1,352 | 2,305 | 3,207 | 6,382 |
| c4_sp8_r1000_mo20000 | 1,337 | 2,293 | 3,217 | 6,530 |
| c2_sp8_r5000_mo5000 | 1,335 | 3,098 | 4,325 | 12,215 |
| c2_sp64_r1000_mo5000 | 1,299 | 2,786 | 4,034 | 7,501 |
| c2_sp8_r1000_mo5000 | 1,273 | 3,145 | 5,116 | 9,712 |
| c2_sp8_r1000_mo20000 | 1,203 | 2,919 | 3,970 | 7,392 |

What the data supports:

- **Best config:** `c4_sp8_r5000_mo20000` at about 4,450 records/s.
- **Cores:** 4 cores vs. 2 at the same settings (sp8, r5000, mo20000): 4,448 vs. 3,640 rec/s, about
  22% higher.
- **Shuffle partitions:** 8 vs. 64 at 2 cores (r5000, mo20000): 3,640 vs. 3,484, within 5%. No
  clear effect.
- **Producer rate:** at 1,000 rec/s every config lands near 1,200 to 1,400 rec/s. Those runs
  measure keeping up with the input, not the pipeline's capacity. Only the 5,000 rec/s runs can
  show a ceiling.
- **Max offsets per trigger:** at 5,000 rec/s, raising it from 5,000 to 20,000 raised
  throughput 2.0x to 2.7x (for example 1,335 to 3,640 at 2 cores). With a backlog, larger
  batches are the likely reason, but we did not isolate it.

## 5. Correctness tests

`pytest` collects **76 tests** across 11 files in `.github/workflows/ci.yml`, split into **68
pure-Python tests** and **8 Spark JVM integration tests**.

| Area | Tests | What it proves |
|---|---|---|
| Repeat-safe Gold writes | 3 (`test_dedupe_spark.py`, real Spark) | Duplicates inside a batch are dropped, rows already in Gold are dropped, and **replaying the same batch adds 0 rows** |
| Error analysis tables | 2 (`test_error_analysis_spark.py`, real Spark) | Per-bucket and per-hub MAE, bias, and baseline comparisons match expected aggregations |
| Live OpenSky aggregation | 3 (`test_live_processor_spark.py`, real Spark) | Only the latest `fetched_at` snapshot is counted, duplicate `icao24` transponders are deduplicated, and zero-traffic hubs emit `aircraft_nearby = 0` |
| Train/serve, tuning, and 40-hub contracts | 17 (`test_pipeline_contracts.py`) | Canonical 40-hub registry parity, local `flight_date` across UTC rollovers, `resolve_gbt_params` and `should_accept` overfitting gates, dynamic UI counts, and `spark.sql.session.timeZone=UTC` across all Spark builders |
| Live radar, producer, and benchmark | 14 (`test_live_radar_and_producer.py`) | Snapshot-anchored FAA freshness, HTTP `429` `Retry-After` cooldowns, Kafka sink fault isolation, `--config` forwarding, and benchmark CSV loading |
| Output isolation and reset | 9 (`test_stream_utils.py`) | Isolated benchmark output roots and atomic `--reset` path validation |
| Config consistency | 10 (`test_config.py`) | Both configs list all 8 weather features, stream year is disjoint from train years, and `download_noaa.py` imports cleanly with or without `tqdm` |
| Model insight helpers | 6 (`test_model_insights.py`) | Feature importances sum cleanly by group, 40-hub MAE gain sorts properly, and summary copy handles edge cases |
| Dashboard story helpers | 9 (`test_dashboard_helpers.py`) | Weather buckets, "so what" narrative copy, thin-lane filtering, and benchmark headlines match measured inputs |
| Benchmark & live-vs-historical parity | 3 (`test_benchmark_summary_matches_evaluation.py`, `test_live_vs_historical.py`) | Section 4 table matches `benchmark_summary.csv` and AeroAPI comparison metrics match committed artifacts |

The 68 pure-Python tests run in ~1.3 seconds without starting a JVM. GitHub Actions runs the full
suite on Ubuntu with Java 17 and PySpark 3.5.1 (1 live-recording test skips on clean clones when
`data/live_recordings/` is absent).

## 6. Limits: what we cannot claim

1. **The model does not predict sudden severe mid-air delays.** Flights losing more than 15 min in
   the air (`7.0%`) are under-predicted by `32.1` min on average (section 3.1). Use the model for
   normal-operations ETA adjustment, not as a severe-storm alarm.
2. **Weather is a modest effect (`1.12%`).** Route cushion (`LaneMeanDelay`), departure delay, and
   schedule time-of-day carry most of the predictive signal.
3. **Small benchmark sample.** Each of the 12 streaming configurations ran for 4 active batches in
   one sweep (captured on the 24-hub model prior to the 40-hub expansion). Differences under ~20%
   (such as `sp8` vs `sp64`) should not be over-interpreted.
4. **End-to-end latency includes queue backlog.** The `6.4` to `13.0` second p95 latencies include
   Kafka wait time behind a backlog; batch duration (`3.2` to `5.4` s p95) is the cleaner compute
   metric.
5. **Predictions, not observations.** Dashboard replay charts display model predictions unless
   explicitly labeled as actuals.
6. **Proxy data.** Results measure commercial passenger flights; applying them to dedicated night-sort
   cargo fleets (`MEM`, `SDF`) is a domain proxy assumption.
7. **Observed arrival-hour weather in historical training.** Destination weather in `2018-2024`
   joins NOAA ISD observations at `sched_arr_hour_utc` rather than archived takeoff TAF forecasts,
   making `+1.12%` an upper bound on forecast arrival weather.
8. **Live comparison uses the rule-based estimator on a 1-evening snapshot.** Section 8's
   `models/live_vs_historical.csv` was recorded across 11 FlightAware sessions on one evening using
   `live/aeroapi_predictor.py` (at the time of the 24-hub `10.53` min reference), not the Spark GBT
   binary (`models/gbt_pipeline_v1`).

## 7. Reproduce

```bash
python -m pytest -v                                                              # full 76-test suite (Linux / Colab)
python pipeline/01_batch_train.py --config config/pipeline_config_colab.yaml --cache-dir /content/cache_40hubs
python scripts/tune_gbt.py --config config/pipeline_config_colab.yaml --cache-dir /content/cache_40hubs
python pipeline/04_error_analysis.py --config config/pipeline_config_colab.yaml  # 40-hub error tables
python benchmark/run_benchmark.py --config config/pipeline_config_colab.yaml     # streaming benchmark
python live/eval_live_vs_historical.py                                           # live vs. 2024 table (needs data/live_recordings)
```

## 8. Live flights vs. history

**Question:** do the recovery patterns learned from `2018-2024` history appear in live flights today?
`models/live_vs_historical.csv` records `505` landed FlightAware AeroAPI flights paired with live
NWS weather (captured alongside the initial 24-hub `1,665,362`-flight `2024` snapshot; across all 40
hubs and `3,041,312` holdout 2024 flights in Section 2, the tuned Spark GBT reaches `9.77` min vs
`12.04` min no-change, or `18.8%` better).

| Model | Live MAE (505 landed flights) | 2024 snapshot MAE (24-hub ref) | Live vs. no change | 2024 ref vs. no change |
|---|---|---|---|---|
| Assume the delay stays the same | 12.19 | 12.75 | n/a | n/a |
| Historical hub average | 9.66 | 10.80 | 20.7% better | 15.3% better |
| Live: rule-based estimator, 2024: Spark GBT | 9.67 | 10.53 (40-hub tuned: **9.77**) | 20.7% better | 17.4% (**18.8%** on 40 hubs) |

What this shows:

- **The historical recovery pattern transfers.** `77%` of the `505` landed live flights arrived
  earlier than their departure delay implied (mean `delta_delay` of `-8.5` min), and historical hub
  adjustment cut MAE by `20.7%`.
- **Operational meaning.** Assuming a late departure arrives equally late overstates arrival delay
  by ~8 minutes on average (`12.2` min MAE). Adjusting at wheels-off cuts error under `10` minutes,
  preventing premature cross-dock truck releases and tightening pickup windows.
- **Big mid-air delays remain hard.** The 12 live flights losing `>15` min in the air had a `46` min
  mean error, matching section 3.1.
