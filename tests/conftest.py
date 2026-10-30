"""Shared test setup: import paths and Spark stubs so tests run without a Spark install."""
import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

ROOT = Path(__file__).resolve().parent.parent
for p in (ROOT, ROOT / "dashboard"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))


def _stub_spark_if_missing():
    """CI installs no pyspark; stub it so module-level imports succeed."""
    try:
        if importlib.util.find_spec("pyspark") is not None:
            return
    except (ImportError, ValueError):
        pass
    for name in ("pyspark", "pyspark.ml", "pyspark.sql", "pyspark.sql.types",
                 "pyspark.sql.functions", "py4j", "py4j.protocol"):
        sys.modules.setdefault(name, MagicMock(name=name))


@pytest.fixture(scope="session")
def stream_processor():
    """pipeline/03_stream_processor.py (name starts with a digit, so load by path)."""
    _stub_spark_if_missing()
    spec = importlib.util.spec_from_file_location(
        "stream_processor", ROOT / "pipeline" / "03_stream_processor.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="session")
def dash():
    """dashboard/app.py helpers (Streamlit runs in bare mode on import)."""
    import app
    return app
