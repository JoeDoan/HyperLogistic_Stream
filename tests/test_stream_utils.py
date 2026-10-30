"""Output-root redirection and the reset safety check in the stream processor."""
from pathlib import Path

CFG = {
    "paths": {
        "bronze": "data/bronze/flights_raw",
        "silver": "data/silver/flights_enriched",
        "gold_predictions": "data/gold/predictions",
        "gold_congestion": "data/gold/hub_congestion",
        "checkpoints": "checkpoints",
        "models": "models",
    },
    "other": 1,
}


def test_output_root_redirects_all_medallion_paths(stream_processor):
    out = stream_processor.apply_output_root(CFG, "data/benchmark_runs/c2")
    root = stream_processor.PROJECT_ROOT / "data" / "benchmark_runs" / "c2"
    for key in ("bronze", "silver", "gold_predictions", "gold_congestion", "checkpoints"):
        assert Path(out["paths"][key]).is_relative_to(root), key


def test_output_root_keeps_models_and_does_not_mutate_input(stream_processor):
    before = dict(CFG["paths"])
    out = stream_processor.apply_output_root(CFG, "data/x")
    assert out["paths"]["models"] == "models"
    assert out["other"] == 1
    assert CFG["paths"] == before


def test_output_root_keeps_absolute_root(stream_processor, tmp_path):
    out = stream_processor.apply_output_root(CFG, str(tmp_path))
    assert out["paths"]["bronze"].startswith(str(tmp_path))


def test_reset_removes_dirs_inside_project(stream_processor):
    target = stream_processor.PROJECT_ROOT / "data" / "_pytest_reset_tmp"
    (target / "sub").mkdir(parents=True, exist_ok=True)
    (target / "sub" / "f.parquet").write_text("x")
    stream_processor.reset_outputs([str(target)])
    assert not target.exists()


def test_reset_refuses_outside_project(stream_processor, tmp_path):
    victim = tmp_path / "keep"
    victim.mkdir()
    (victim / "f.txt").write_text("x")
    stream_processor.reset_outputs([str(victim)])
    assert (victim / "f.txt").exists()


def test_reset_refuses_project_root_itself(stream_processor):
    stream_processor.reset_outputs([str(stream_processor.PROJECT_ROOT)])
    assert (stream_processor.PROJECT_ROOT / "pipeline").exists()
