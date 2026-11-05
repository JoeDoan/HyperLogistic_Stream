"""Repeat-safe Gold writes: replaying a batch must not add rows. Needs a real Spark."""
import os
import sys

import pandas as pd
import pytest

pytest.importorskip("pyspark")

# Reading Parquet on Windows needs Hadoop's winutils; without it dedupe_batch falls back
# to batch-level dedupe, so the "already in Gold" tests cannot pass there.
needs_hadoop = pytest.mark.skipif(
    sys.platform == "win32" and not os.environ.get("HADOOP_HOME"),
    reason="Windows without HADOOP_HOME cannot read Parquet in Spark",
)


@pytest.fixture(scope="module")
def spark():
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    from pyspark.sql import SparkSession
    try:
        s = (SparkSession.builder.master("local[1]").appName("dedupe-test")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "1").getOrCreate())
    except Exception as exc:  # no JVM / broken Spark install
        pytest.skip(f"Spark unavailable: {exc}")
    yield s
    s.stop()


def batch(spark, ids):
    return spark.createDataFrame(pd.DataFrame({"flight_id": ids, "v": range(len(ids))}))


def test_drops_duplicates_within_batch(stream_processor, spark, tmp_path):
    out = stream_processor.dedupe_batch(spark, batch(spark, ["a", "a", "b"]), str(tmp_path / "none"))
    assert sorted(r.flight_id for r in out.collect()) == ["a", "b"]


@needs_hadoop
def test_drops_rows_already_in_gold(stream_processor, spark, tmp_path):
    gold = tmp_path / "gold"
    gold.mkdir()
    pd.DataFrame({"flight_id": ["a", "b"]}).to_parquet(gold / "part-0.parquet")  # pyarrow, no Hadoop
    out = stream_processor.dedupe_batch(spark, batch(spark, ["a", "b", "c"]), str(gold))
    assert [r.flight_id for r in out.collect()] == ["c"]


@needs_hadoop
def test_replaying_same_batch_adds_nothing(stream_processor, spark, tmp_path):
    gold = tmp_path / "gold"
    gold.mkdir()
    first = stream_processor.dedupe_batch(spark, batch(spark, ["a", "b", "c"]), str(gold))
    first.toPandas()[["flight_id"]].to_parquet(gold / "part-0.parquet")
    replay = stream_processor.dedupe_batch(spark, batch(spark, ["a", "b", "c"]), str(gold))
    assert replay.count() == 0
