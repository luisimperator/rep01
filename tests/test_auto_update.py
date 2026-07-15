"""Tests for the AutoUpdater self-update loop.

Covers the decision logic of one updater tick: when an update gets applied,
when the daemon restart is requested, and the latches that keep a broken
release from putting the daemon in a pull/restart loop.
"""
import sys
import threading
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from transcoder.updater import (  # noqa: E402
    AutoUpdater,
    UpdateStatus,
    detect_install_dir,
)


def _status(available: bool, tag: str | None = "v9.9.9") -> UpdateStatus:
    return UpdateStatus(
        current_version="8.2.0",
        latest_tag=tag,
        update_available=available,
        checked_at="2026-07-15T00:00:00+00:00",
        error=None,
    )


def _checkout(tmp_path: Path, version: str) -> Path:
    """Fake git checkout with a pyproject.toml at the given version."""
    (tmp_path / ".git").mkdir(exist_ok=True)
    (tmp_path / "pyproject.toml").write_text(
        f'[project]\nname = "x"\nversion = "{version}"\n', encoding="utf-8"
    )
    return tmp_path


def _make(tmp_path, *, available=True, apply_rc=0, auto_apply=True,
          install_dir="auto", pulled_version="9.9.9"):
    """AutoUpdater wired with stubs; returns (updater, apply_fn, restart)."""
    checkout = _checkout(tmp_path, pulled_version)
    apply_fn = MagicMock(return_value=apply_rc)
    restart = MagicMock()
    upd = AutoUpdater(
        MagicMock(),  # db — only the injected check_fn touches it
        "luisimperator/rep01",
        threading.Event(),
        auto_apply=auto_apply,
        install_dir=checkout if install_dir == "auto" else install_dir,
        request_restart=restart,
        check_fn=lambda *a, **k: _status(available),
        apply_fn=apply_fn,
    )
    upd._running_version = "8.2.0"
    return upd, apply_fn, restart


class TestAutoApply:
    def test_newer_release_is_applied_and_daemon_restarts(self, tmp_path):
        upd, apply_fn, restart = _make(tmp_path)
        upd._tick()
        apply_fn.assert_called_once()
        restart.assert_called_once_with("auto-update to v9.9.9")

    def test_no_update_available_does_nothing(self, tmp_path):
        upd, apply_fn, restart = _make(tmp_path, available=False)
        upd._tick()
        apply_fn.assert_not_called()
        restart.assert_not_called()

    def test_auto_apply_off_is_notify_only(self, tmp_path):
        upd, apply_fn, restart = _make(tmp_path, auto_apply=False)
        upd._tick()
        apply_fn.assert_not_called()
        restart.assert_not_called()

    def test_non_git_install_never_applies(self, tmp_path):
        upd, apply_fn, restart = _make(tmp_path, install_dir=None)
        upd._tick()
        apply_fn.assert_not_called()
        restart.assert_not_called()


class TestNoRestartLoops:
    def test_failed_apply_is_not_retried_for_same_tag(self, tmp_path):
        upd, apply_fn, restart = _make(tmp_path, apply_rc=1)
        upd._tick()
        upd._tick()
        apply_fn.assert_called_once()  # second tick skips the latched tag
        restart.assert_not_called()

    def test_newer_tag_resets_the_failure_latch(self, tmp_path):
        upd, apply_fn, restart = _make(tmp_path, apply_rc=1)
        upd._tick()
        apply_fn.assert_called_once()

        # A NEW release supersedes the broken one — try again.
        upd._check_fn = lambda *a, **k: _status(True, tag="v10.0.0")
        upd._apply_fn = MagicMock(return_value=0)
        upd._tick()
        upd._apply_fn.assert_called_once()
        restart.assert_called_once_with("auto-update to v10.0.0")

    def test_pull_that_does_not_advance_version_does_not_restart(self, tmp_path):
        """Tag published but the checkout still holds the running version
        (e.g. pull landed on an unexpected branch) — restarting would loop."""
        upd, apply_fn, restart = _make(tmp_path, pulled_version="8.2.0")
        upd._tick()
        upd._tick()
        apply_fn.assert_called_once()
        restart.assert_not_called()


def test_detect_install_dir_finds_this_checkout():
    found = detect_install_dir()
    assert found is not None
    assert (found / "pyproject.toml").exists()
    assert (found / ".git").exists()
