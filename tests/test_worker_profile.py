"""Tests for installer/apply_worker_profile.py (the night-worker config profile)."""
import importlib.util
import shutil
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))

yaml = pytest.importorskip("yaml")

_spec = importlib.util.spec_from_file_location(
    "apply_worker_profile", ROOT / "installer" / "apply_worker_profile.py"
)
awp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(awp)


def _fresh_config(tmp_path):
    cfg = tmp_path / "config.yaml"
    shutil.copy(ROOT / "config.example.yaml", cfg)
    return cfg


def test_profile_sets_expected_keys(tmp_path):
    cfg = _fresh_config(tmp_path)
    changed = awp.apply_profile(cfg)
    data = yaml.safe_load(cfg.read_text())
    assert data["encoder_preference"] == "nvenc"
    assert data["availability"]["enabled"] is True
    assert data["availability"]["night_start"] == "18:00"
    assert data["availability"]["night_end"] == "09:00"
    assert data["coordination"]["enabled"] is True
    assert data["coordination"]["claims_folder"] == "/HeavyDrops/_h265_claims"
    assert data["disk_budget"]["max_staging_bytes"] == 150_000_000_000
    assert data["concurrency"]["download_workers"] == 1
    assert data["concurrency"]["transcode_workers"] == 1
    assert "encoder_preference" in changed


def test_profile_is_idempotent(tmp_path):
    cfg = _fresh_config(tmp_path)
    awp.apply_profile(cfg)
    assert awp.apply_profile(cfg) == []  # second run changes nothing


def test_profile_preserves_siblings(tmp_path):
    cfg = _fresh_config(tmp_path)
    before = yaml.safe_load(cfg.read_text())
    audio_workers = before["concurrency"].get("audio_workers")
    awp.apply_profile(cfg)
    after = yaml.safe_load(cfg.read_text())
    # Untouched sibling keys survive the merge.
    assert after["concurrency"].get("audio_workers") == audio_workers
    assert after.get("dropbox_root") == before.get("dropbox_root")


def test_result_loads_as_valid_config(tmp_path, monkeypatch):
    cfg = _fresh_config(tmp_path)
    awp.apply_profile(cfg)
    monkeypatch.chdir(tmp_path)
    from transcoder.config import load_config
    loaded = load_config(str(cfg))
    assert loaded.encoder_preference.value == "nvenc"
    assert loaded.availability.night_start == "18:00"
    assert loaded.coordination.enabled is True
