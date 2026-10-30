# EVALUATION.md: HyperLogistic_Stream

> CS 5540. Every number below comes from a file committed in this repo (`models/ablation.csv`,
> `models/training_metrics.json`, `models/baseline_hub_hour_dow.csv`, `models/error_by_*.csv`,
> `benchmark/results/summary.csv`) or from `pytest`. Nothing is hand-edited.

## 1. What is evaluated

| Question | Evidence | Section |
|---|---|---|
| Does the model predict delay change better than simple baselines? | `models/ablation.csv` | 2 |
| Do weather features help? | same ablation | 3 |
| Where is the model wrong? | `models/error_by_*.csv` | 3.1 |
| How fast is the streaming pipeline, and what drives it? | `benchmark/results/summary.csv` | 4 |
| Are Gold writes repeat-safe? | `tests/test_dedupe_spark.py` | 5 |
| What can we **not** claim? | | 6 |

The target is `delta_delay` (arrival delay minus departure delay, in minutes). Flights are a
public proxy for freight movement.

## 2. Model vs. baselines

Train on 2018, 2019, 2021, 2022 and 2023 (7,996,449 flights). Validate on 2024 (1,665,362 flights).
MAE is in minutes, lower is better.

| Model | Features | Val MAE | Val RMSE | vs. "no change" |
|---|---|---|---|---|
| Baseline: predict 0 | 0 | 12.749 | 16.627 | n/a |
| Baseline: training lane mean | 1 | 10.799 | 15.329 | 15.3% better |
| GBT, no weather | 6 | 10.640 | 15.020 | 16.5% better |
| **GBT, full weather** | 14 | **10.533** | **14.863** | **17.4% better** |
| Random forest, full weather | 14 | 10.669 | 15.161 | 16.3% better |

Reading it plainly:

- Most of the improvement over "no change" comes from knowing the **lane**. The lane-mean
  baseline alone removes 2.0 of the 2.2 minutes the GBT gains.
- The GBT beats the lane-mean baseline by only **0.27 min (2.5%)**. The model is better,
  but not by a lot.
- GBT beats random forest by 0.14 min on the same features.

## 3. Weather ablation

Same GBT, same data, features added step by step (`models/ablation.csv`):

| Step | Added features | Val MAE | Gain vs. no weather |
|---|---|---|---|
| `gbt_no_weather` | none (lane + schedule) | 10.640 | 0.00% |
| `gbt_dest_wind_vis_temp` | destination wind, visibility, temperature | 10.601 | 0.36% |
| `gbt_dest_plus_ceiling` | + destination ceiling | 10.592 | 0.45% |
| `gbt_full_weather` | + origin wind, visibility, ceiling, temperature | 10.533 | **1.01%** |

**Finding:** weather helps, but modestly: **0.11 min (1.0%)** of MAE. Adding the weather
features costs about 7 seconds of training (288 s to 295 s), so the cost is negligible.

**Feature importance does not tell the same story.** The GBT's built-in importances give the
eight weather features a combined 0.40 of the total, and the two hour-of-day features 0.35.
That looks large next to a 1% MAE gain. Split-based importance rewards continuous features
that offer many split points, so it overstates their value. We treat the ablation as the
reliable number and the importances as descriptive only.

The dashboard's Weather-Impact chart (Replay tab) shows the model's *predicted* delay by weather
bucket. It describes what the model learned, not observed delays.

### 3.1 Error analysis: where the model is wrong

`pipeline/04_error_analysis.py` scores the served full-weather GBT on the 2024 validation flights
(1,665,362, the same set as section 2) and writes `models/error_by_*.csv`. Weighted over all
hubs, its MAE is 10.53 min, matching `training_metrics.json`. The model never trained on 2024.

**By origin hub** (24 hubs; MAE in minutes):

| | Hub | GBT MAE | Lane-mean MAE | No-change MAE |
|---|---|---|---|---|
| Worst | JFK | 14.02 | 14.43 | 17.66 |
| | EWR | 12.81 | 13.28 | 15.81 |
| | BOS | 12.11 | 12.43 | 15.04 |
| | ATL | 9.20 | 9.25 | 10.72 |
| Best | PHX | 8.88 | 9.01 | 10.93 |

- Northeast hubs are hardest to predict. JFK's error is about 58% higher than PHX's.
- The GBT beats the lane-mean baseline at **every** hub, but only by 0.05 min (ATL) to 0.50 min
  (DEN). The model's edge over the baseline is small everywhere.

**By actual outcome** (flights grouped by what really happened):

| Actual delta delay | Flights | GBT MAE | No-change MAE | GBT bias |
|---|---|---|---|---|
| Early >15 min | 403,493 | 15.0 | 22.7 | +15.0 |
| Early 5-15 min | 568,491 | 4.5 | 10.4 | +3.8 |
| Within 5 min | 402,498 | 5.7 | 2.6 | -5.1 |
| Late 5-15 min | 167,989 | 14.6 | 8.7 | -14.5 |
| Late >15 min | 122,891 | 34.1 | 29.8 | -34.1 |

(Bias is prediction minus actual; negative means the model predicted too little delay.)

- **The model beats "no change" only on flights that arrive early.** On on-time and late flights,
  predicting 0 is better. The overall 17% gain comes from the 58% of flights that make up time.
- **Big delays are badly missed.** Flights that end up more than 15 min late are under-predicted by
  34 min on average. The model predicts roughly a 4 min *gain* for them. It does not recognise a
  bad day in advance.
- This table groups by the outcome, so some of the bias is expected: any model that predicts toward
  the average will look biased at the extremes. It still shows the model is not a delay-warning
  tool in its current form.

**By hour and season:**

- Error is highest for departures around 22:00 to 00:00 UTC (MAE about 11.2 to 11.5) and lowest
  around 09:00 to 10:00 UTC (9.2 to 9.6, on low volume).
- Monthly MAE ranges from 9.3 (October) to 11.6 (January). Flights make up the least time in
  May (average actual delta -4.2 min) and the most in October (-7.8 min).
- The largest single month-hub bias is BOS in February (+5.9 min): the model expected less time
  gained than winter flights actually made up.

The dashboard's **Model Insights** tab shows these tables and the seasonality heatmap.

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

`pytest` runs 58 tests in GitHub Actions (`.github/workflows/ci.yml`), split into 50 pure-Python tests and 8 Spark tests.

| Area | Tests | What it proves |
|---|---|---|
| Repeat-safe Gold writes | 3 (`test_dedupe_spark.py`, real Spark) | Duplicates inside a batch are dropped, rows already in Gold are dropped, and **replaying the same batch adds 0 rows** |
| Error analysis tables | 2 (`test_error_analysis_spark.py`, real Spark) | Per-bucket and per-hub MAE, bias, and baseline comparisons match expected aggregations |
| Live OpenSky aggregation | 3 (`test_live_processor_spark.py`, real Spark) | Only the latest `fetched_at` snapshot is counted, duplicate `icao24` transponders are deduplicated, and zero-traffic hubs emit `aircraft_nearby = 0` |
| Train/serve and Spark contracts | 10 (`test_pipeline_contracts.py`) | Local `flight_date` survives UTC midnight rollovers, missing models fail fast unless `--allow-dummy-model` is set, and all 6 Spark builders pin `spark.sql.session.timeZone=UTC` |
| Live radar and producer resilience | 11 (`test_live_radar_and_producer.py`) | Recorded sessions anchor FAA freshness to snapshot timestamps, HTTP `429` `Retry-After` triggers cooldowns, Kafka sink errors do not crash polling threads, and `summary.csv` loads cleanly |
| Output isolation and reset | 9 (`test_stream_utils.py`) | Benchmark runs write to isolated roots, and `--reset` validates every target atomically before deleting inside the project or configured external roots |
| Config consistency | 5 (`test_config.py`) | Both configs list all 8 weather features, and the stream year is excluded from training years |
| Model insight helpers | 6 (`test_model_insights.py`) | Feature importances sum cleanly by group, per-hub MAE gain is computed accurately, and error summary copy handles edge cases |
| Dashboard story helpers | 9 (`test_dashboard_helpers.py`) | Weather buckets, the "so what" sentence, thin-lane filtering, and the benchmark headline match measured inputs |

The 50 pure-Python tests run in under 1 second without starting a JVM. GitHub Actions runs all 58 tests on Ubuntu with Java 17 and PySpark 3.5.0. On Windows without Hadoop's `winutils`, the two Gold-reading Spark tests skip and the pipeline falls back to batch-level dedupe.

## 6. Limits: what we cannot claim

1. **The model does not predict big delays.** Flights that end up more than 15 min late are
   under-predicted by about 34 min, and on on-time and late flights "no change" scores better
   (section 3.1). Use it for typical-case ETA adjustment, not to warn of severe delays.
2. **Weather is a small effect here.** A 1.0% MAE gain is real in the ablation but too small
   to support a headline like "weather predicts delay". The honest sentence is "weather adds
   about 1%; lane and schedule do most of the work".
3. **Small benchmark sample.** Each config has 4 batches and one run. Differences under roughly
   20% (for example sp8 vs. sp64) should not be treated as real.
4. **End-to-end latency is inflated.** The 6 to 13 second latencies include time records waited
   in Kafka behind a backlog, not just processing time. Do not quote them as the pipeline's
   latency. Batch time (p95 of 3 to 5 seconds) is the cleaner measure.
5. **The benchmark is provisional.** The reduced benchmark is planned to be re-run on
   Mon Nov 2 with the final code. Treat this table as provisional until then.
6. **Predictions, not observations.** Dashboard charts show model output (predicted
   `delta_delay`), not measured delays.
7. **Proxy data.** Results describe commercial flights. Applying them to freight is an
   assumption.
8. **Oracle destination weather.** Origin weather is joined at the departure hour (`wheels_off_hour_utc`),
   which is observed when the prediction runs. Destination weather (`DestWindSpeed`, `DestVisibility`,
   `DestTemp`, `DestCeiling`) is joined from observed NOAA ISD at the scheduled arrival hour
   (`sched_arr_hour_utc`), 1 to 6 hours after departure, rather than an airport forecast (TAF). That
   makes the +1.01% weather ablation gain an upper bound on what forecast arrival weather can deliver.

## 7. Reproduce

```bash
python -m pytest -v                                                              # correctness suite
python pipeline/01_batch_train.py --config config/pipeline_config_colab.yaml     # 5-year train + 2024 ablation (Colab)
python pipeline/04_error_analysis.py --config config/pipeline_config_colab.yaml  # 2024 error tables
python benchmark/run_benchmark.py --config config/pipeline_config_colab.yaml     # pipeline benchmark
```
