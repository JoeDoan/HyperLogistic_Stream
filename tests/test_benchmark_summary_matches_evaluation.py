import csv
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _evaluation_benchmark_rows():
    text = (ROOT / "EVALUATION.md").read_text()
    section = text.split("## 4. Pipeline benchmark", 1)[1].split("What the data supports", 1)[0]
    rows = {}
    for line in section.splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) == 5 and re.fullmatch(r"c\d+_sp\d+_r\d+_mo\d+", cells[0]):
            rows[cells[0]] = [float(c.replace(",", "")) for c in cells[1:]]
    return rows


def test_benchmark_summary_csv_matches_evaluation_table():
    expected = _evaluation_benchmark_rows()
    assert len(expected) == 12
    with (ROOT / "benchmark/results/benchmark_summary.csv").open() as f:
        actual = {
            r["label"]: [float(r["avg_throughput_rps"]), float(r["p50_batch_ms"]),
                         float(r["p95_batch_ms"]), float(r["median_p95_latency_ms"])]
            for r in csv.DictReader(f)
        }
    assert actual == expected
