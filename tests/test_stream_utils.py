"""Output-root redirection and the reset safety check in the stream processor."""
import shutil
from pathlib import Path

import pytest

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
    with pytest.raises(ValueError, match="Refusing unsafe or unauthorized reset target"):
        stream_processor.reset_outputs([str(victim)])
    assert (victim / "f.txt").exists()


def test_reset_refuses_project_root_itself(stream_processor):
    with pytest.raises(ValueError, match="Refusing unsafe or unauthorized reset target"):
        stream_processor.reset_outputs([str(stream_processor.PROJECT_ROOT)])
    assert (stream_processor.PROJECT_ROOT / "pipeline").exists()


def test_reset_allows_explicit_external_configured_paths(stream_processor, tmp_path):
    colab_gold = tmp_path / "drive" / "MyDrive" / "HyperLogistics" / "gold"
    colab_ckpt = tmp_path / "checkpoints" / "c4_sp8_mo5000"
    (colab_gold / "part").mkdir(parents=True)
    (colab_gold / "part" / "f.parquet").write_text("x")
    colab_ckpt.mkdir(parents=True)
    (colab_ckpt / "offsets").write_text("1")

    targets = [str(colab_gold), str(colab_ckpt)]
    stream_processor.reset_outputs(targets, allowed_roots=targets)

    assert not colab_gold.exists()
    assert not colab_ckpt.exists()


def test_reset_refuses_shallow_root_even_if_allowed(stream_processor):
    with pytest.raises(ValueError, match="Refusing unsafe or unauthorized reset target"):
        stream_processor.reset_outputs([str(Path("/tmp"))], allowed_roots=[str(Path("/tmp"))])

    with pytest.raises(ValueError, match="Refusing unsafe or unauthorized reset target"):
        stream_processor.reset_outputs(
            [str(stream_processor.PROJECT_ROOT)],
            allowed_roots=[str(stream_processor.PROJECT_ROOT)],
        )
    assert (stream_processor.PROJECT_ROOT / "pipeline").exists()


def test_reset_validates_all_targets_before_deleting_any(stream_processor, tmp_path):
    valid_inside = stream_processor.PROJECT_ROOT / "data" / "_pytest_atomic_reset_tmp"
    valid_inside.mkdir(parents=True, exist_ok=True)
    (valid_inside / "f.parquet").write_text("x")
    unauthorized_outside = tmp_path / "unauthorized"
    unauthorized_outside.mkdir()
    try:
        with pytest.raises(ValueError, match="Refusing unsafe or unauthorized reset target"):
            stream_processor.reset_outputs([str(valid_inside), str(unauthorized_outside)])
        assert (valid_inside / "f.parquet").exists()
    finally:
        shutil.rmtree(valid_inside, ignore_errors=True)
