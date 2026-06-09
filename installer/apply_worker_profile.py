#!/usr/bin/env python3
"""Apply the production / editor-machine profile to a config.yaml.

Used by the worker bootstrap (HD_WORKER=1) so each editors' machine (Heavy1-6)
comes up ready to chip in overnight without anyone touching settings:
  * NVIDIA NVENC encoder
  * night mode 18:00-09:00, pausing the instant someone uses the machine
  * shared-Dropbox claim ON so machines split the work with no duplicates
  * a low disk ceiling so it never crowds the editors' drive
  * 1/1/1 concurrency (a file or two on disk at a time)

It only sets these keys, preserving everything else (Dropbox auth, paths, the
dedicated box's own settings are never touched — this runs on workers only).
Idempotent: safe to re-run on upgrades.

Usage: python apply_worker_profile.py <path-to-config.yaml>
"""
from __future__ import annotations

import sys
from pathlib import Path

import yaml

# The production-machine overrides. Nested dicts are MERGED into any existing
# block so sibling keys (e.g. concurrency.audio_workers) are preserved.
PROFILE = {
    "encoder_preference": "nvenc",
    "availability": {
        "enabled": True,
        "night_start": "18:00",
        "night_end": "09:00",
        "pause_when_user_active": True,
        "idle_minutes": 10,
        "check_interval_sec": 60,
    },
    "coordination": {
        "enabled": True,
        "claims_folder": "/_h265_claims",
        "claim_ttl_minutes": 120,
        "heartbeat_minutes": 10,
    },
    "disk_budget": {
        "enabled": True,
        "max_staging_bytes": 150_000_000_000,   # ~150 GB scratch ceiling
        "min_free_bytes": 100_000_000_000,       # back off under ~100 GB free
    },
    "concurrency": {
        "download_workers": 1,
        "transcode_workers": 1,
        "upload_workers": 1,
    },
}


def _merge(base: dict, overrides: dict) -> list[str]:
    """Deep-merge overrides into base; return the dotted keys that changed."""
    changed: list[str] = []
    for key, value in overrides.items():
        if isinstance(value, dict):
            sub = base.get(key)
            if not isinstance(sub, dict):
                sub = {}
                base[key] = sub
            for leaf in _merge(sub, value):
                changed.append(f"{key}.{leaf}")
        else:
            if base.get(key) != value:
                changed.append(key)
            base[key] = value
    return changed


def apply_profile(config_path: Path) -> list[str]:
    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    changed = _merge(data, PROFILE)
    config_path.write_text(
        yaml.safe_dump(data, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    return changed


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: apply_worker_profile.py <config.yaml>", file=sys.stderr)
        return 2
    path = Path(sys.argv[1])
    if not path.exists():
        print(f"config not found: {path}", file=sys.stderr)
        return 1
    changed = apply_profile(path)
    if changed:
        print("Applied production-machine profile. Set/updated:")
        for k in changed:
            print(f"  - {k}")
    else:
        print("Production-machine profile already applied; nothing to change.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
