"""Dashboard settings API: the fleet knobs validate, persist, and round-trip."""
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

_MINIMAL_YAML = (
    "dropbox_root: /Test\n"
    "encoder_preference: auto\n"
    "disk_budget:\n"
    "  enabled: false\n"
    "  max_staging_bytes: 2000000000000\n"
    "  min_free_bytes: 1200000000000\n"
    "availability:\n"
    "  enabled: false\n"
    '  night_start: "20:00"\n'
    '  night_end: "07:00"\n'
    "  pause_when_user_active: true\n"
    "  idle_minutes: 10\n"
    "coordination:\n"
    "  enabled: false\n"
    '  claims_folder: "/_h265_claims"\n'
    "  claim_ttl_minutes: 60\n"
    "  heartbeat_minutes: 10\n"
)


def _load(tmp_path, monkeypatch):
    (tmp_path / "config.yaml").write_text(_MINIMAL_YAML, encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    from transcoder.config import load_config
    return load_config("config.yaml")


def test_fleet_settings_apply_persist_and_roundtrip(tmp_path, monkeypatch):
    from transcoder import api as A
    from transcoder.config import load_config

    cfg = _load(tmp_path, monkeypatch)
    api = SimpleNamespace(config=cfg, daemon=None)
    res = A._apply_settings(api, {
        "encoder_preference": "nvenc",
        "availability_enabled": True,
        "availability_night_start": "18:00",
        "availability_night_end": "9:00",          # normalises to 09:00
        "coordination_enabled": True,
        "coordination_claim_ttl_minutes": 120,
        "disk_budget_max_staging_gb": 150,
    })

    text = (tmp_path / "config.yaml").read_text()
    assert "encoder_preference: nvenc" in text
    assert 'night_start: "18:00"' in text          # quoted so YAML keeps it a string
    assert 'night_end: "09:00"' in text

    # In-memory config mutated correctly.
    assert cfg.encoder_preference.value == "nvenc"
    assert cfg.availability.enabled and cfg.availability.night_start == "18:00"
    assert cfg.coordination.enabled and cfg.coordination.claim_ttl_minutes == 120
    assert cfg.disk_budget.max_staging_bytes == 150_000_000_000
    assert set(res["updated"]) == {
        "encoder_preference", "availability_enabled", "availability_night_start",
        "availability_night_end", "coordination_enabled",
        "coordination_claim_ttl_minutes", "disk_budget_max_staging_gb",
    }

    # Reloading proves the YAML is valid and 18:00 isn't read as sexagesimal.
    cfg2 = load_config("config.yaml")
    assert cfg2.availability.night_start == "18:00"
    assert cfg2.encoder_preference.value == "nvenc"
    assert cfg2.coordination.enabled is True


def test_bad_time_and_choice_are_rejected(tmp_path, monkeypatch):
    from transcoder import api as A
    cfg = _load(tmp_path, monkeypatch)
    api = SimpleNamespace(config=cfg, daemon=None)

    for bad in ({"availability_night_start": "25:99"},
                {"availability_night_start": "banana"},
                {"encoder_preference": "magic"}):
        try:
            A._apply_settings(api, bad)
            assert False, f"should have rejected {bad}"
        except ValueError:
            pass
