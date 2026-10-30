# EVALUATION.md: HyperLogistic_Stream

> CS 5540. Every number below comes from a file in this repo (`models/ablation.csv`,
> `models/training_metrics.json`, `models/baseline_hub_hour_dow.csv`,
> `benchmark/results/*.jsonl`) or from `pytest`. Nothing is hand-edited.

## 1. What is evaluated

| Question | Evidence | Section |
|---|---|---|
| Does the model predict delay change better than simple baselines? | `models/ablation.csv` | 2 |
| Do weather features help? | same ablation | 3 |
| How fast is the streaming pipeline, and what drives it? | `benchmark/results/` | 4 |
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

`pytest` runs 23 tests locally and in GitHub Actions (`.github/workflows/ci.yml`).

| Area | Tests | What it proves |
|---|---|---|
| Repeat-safe Gold writes | 3 (`test_dedupe_spark.py`, real Spark) | Duplicates inside a batch are dropped, rows already in Gold are dropped, and **replaying the same batch adds 0 rows** |
| Output isolation and reset | 6 | Benchmark runs write to their own root. `--reset` deletes inside the project but refuses outside paths and the project root |
| Config consistency | 5 | Both configs list all 8 weather features. The stream year is not in the training years |
| Dashboard story helpers | 9 | Weather buckets, the "so what" sentence, thin-lane filtering and the benchmark headline are computed correctly |

The Spark tests were run on Linux (Colab, 23 passed). On Windows without Hadoop's `winutils`, the
two Gold-reading tests skip, and the pipeline falls back to batch-level dedupe.

## 6. Limits: what we cannot claim

1. **Weather is a small effect here.** A 1.0% MAE gain is real in the ablation but too small
   to support a headline like "weather predicts delay". The honest sentence is "weather adds
   about 1%; lane and schedule do most of the work".
2. **Small benchmark sample.** Each config has 4 batches and one run. Differences under roughly
   20% (for example sp8 vs. sp64) should not be treated as real.
3. **End-to-end latency is inflated.** The 6 to 13 second latencies include time records waited
   in Kafka behind a backlog, not just processing time. Do not quote them as the pipeline's
   latency. Batch time (p95 of 3 to 5 seconds) is the cleaner measure.
4. **The benchmark is provisional.** The reduced benchmark is planned to be re-run on
   Mon Nov 2 with the final code. Treat this table as provisional until then.
5. **Predictions, not observations.** Dashboard charts show model output (predicted
   `delta_delay`), not measured delays.
6. **Proxy data.** Results describe commercial flights. Applying them to freight is an
   assumption.

## 7. Reproduce

```bash
python -m pytest -v                       # tests (23)
python pipeline/01_batch_train.py         # retrain; writes models/ablation.csv (long, use Colab)
python benchmark/run_benchmark.py         # pipeline benchmark
```
