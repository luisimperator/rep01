"""Tests for the auto-update restart plumbing (v8.3.0 postmortem).

v8.3.0 stayed down after its own update restart because (1) the helper's
creationflags combined DETACHED_PROCESS with CREATE_NO_WINDOW — an invalid
CreateProcess pair, so the helper never spawned — and (2) the fallback
relied on Task Scheduler's RestartOnFailure, which ignores application
exit codes. These tests pin the fixed contract: valid helper flags, a
guaranteed hard exit, and the keepalive task registered at boot.
"""
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from transcoder import main as m  # noqa: E402

DETACHED_PROCESS = 0x00000008
CREATE_NEW_PROCESS_GROUP = 0x00000200
CREATE_NO_WINDOW = 0x08000000


def _daemon() -> m.Daemon:
    d = object.__new__(m.Daemon)
    d.config = MagicMock()
    d.config.updater.windows_task_name = "HeavyDropsDaemon"
    return d


@pytest.fixture
def win32(monkeypatch):
    monkeypatch.setattr(m.sys, "platform", "win32")
    monkeypatch.setattr(subprocess, "DETACHED_PROCESS", DETACHED_PROCESS,
                        raising=False)
    monkeypatch.setattr(subprocess, "CREATE_NEW_PROCESS_GROUP",
                        CREATE_NEW_PROCESS_GROUP, raising=False)
    monkeypatch.setattr(subprocess, "CREATE_NO_WINDOW", CREATE_NO_WINDOW,
                        raising=False)


class TestExitForRestart:
    def test_helper_flags_valid_and_process_hard_exits(self, monkeypatch, win32):
        import transcoder.api as api_mod
        monkeypatch.setattr(api_mod, "_kill_all_ffmpeg", lambda: 0)

        popens = []
        monkeypatch.setattr(
            m.subprocess, "Popen",
            lambda *a, **k: popens.append((a, k)) or MagicMock(),
        )
        exits = []
        monkeypatch.setattr(m.logging, "shutdown", lambda: None)
        monkeypatch.setattr(
            m.os, "_exit",
            lambda code: (_ for _ in ()).throw(SystemExit(code)),
        )

        with pytest.raises(SystemExit) as e:
            _daemon()._exit_for_restart()

        assert e.value.code == m.RESTART_EXIT_CODE
        assert len(popens) == 1
        flags = popens[0][1]["creationflags"]
        # The invalid pair that killed v8.3.0's helper must never return.
        assert flags & CREATE_NO_WINDOW == 0
        assert flags & DETACHED_PROCESS
        assert "schtasks /Run" in popens[0][0][0][-1]

    def test_hard_exit_happens_even_if_helper_spawn_fails(self, monkeypatch, win32):
        import transcoder.api as api_mod
        monkeypatch.setattr(api_mod, "_kill_all_ffmpeg", lambda: 0)
        monkeypatch.setattr(
            m.subprocess, "Popen",
            lambda *a, **k: (_ for _ in ()).throw(OSError("boom")),
        )
        monkeypatch.setattr(m.logging, "shutdown", lambda: None)
        monkeypatch.setattr(
            m.os, "_exit",
            lambda code: (_ for _ in ()).throw(SystemExit(code)),
        )

        with pytest.raises(SystemExit) as e:
            _daemon()._exit_for_restart()
        assert e.value.code == m.RESTART_EXIT_CODE


class TestKeepaliveTask:
    def test_registered_with_30min_schedule(self, monkeypatch, win32):
        runs = []
        monkeypatch.setattr(
            m.subprocess, "run",
            lambda cmd, **k: runs.append(cmd) or MagicMock(returncode=0),
        )
        _daemon()._ensure_keepalive_task()

        assert len(runs) == 1
        cmd = runs[0]
        assert cmd[:3] == ["schtasks", "/Create", "/F"]
        assert "HeavyDropsDaemonKeepalive" in cmd
        i = cmd.index("/SC")
        assert cmd[i + 1] == "MINUTE" and cmd[cmd.index("/MO") + 1] == "30"
        assert 'schtasks /Run /TN "HeavyDropsDaemon"' in cmd

    def test_noop_off_windows(self, monkeypatch):
        monkeypatch.setattr(m.sys, "platform", "linux")
        runs = []
        monkeypatch.setattr(
            m.subprocess, "run",
            lambda cmd, **k: runs.append(cmd) or MagicMock(returncode=0),
        )
        _daemon()._ensure_keepalive_task()
        assert runs == []

    def test_registration_failure_is_non_fatal(self, monkeypatch, win32):
        monkeypatch.setattr(
            m.subprocess, "run",
            lambda cmd, **k: (_ for _ in ()).throw(OSError("no schtasks")),
        )
        _daemon()._ensure_keepalive_task()  # must not raise
