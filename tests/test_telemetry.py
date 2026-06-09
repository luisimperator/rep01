"""Tests for the telemetry status publisher (offline — no network calls)."""
import sys
import threading
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from transcoder.telemetry import StatusPublisher, _tail, _human_bytes


class _FakeDB:
    def get_stats(self):
        return {
            "state_counts": {"done": 12, "downloading": 2, "failed": 1},
            "total_jobs": 15,
        }

    def get_savings_stats(self, since=None):
        return {"jobs": 4, "bytes_saved": 5 * 1024 ** 3}


def _make_publisher(tmp_path, *, enabled=True, token="tok", tail_lines=50):
    tele = SimpleNamespace(
        enabled=enabled,
        github_repo="luisimperator/rep01",
        github_token=token,
        branch="telemetry",
        interval_minutes=30,
        log_tail_lines=tail_lines,
    )
    config = SimpleNamespace(telemetry=tele)
    return StatusPublisher(config, _FakeDB(), threading.Event(), tmp_path, token)


def test_tail_returns_last_lines(tmp_path):
    p = tmp_path / "log.txt"
    p.write_text("\n".join(f"line{i}" for i in range(100)), encoding="utf-8")
    out = _tail(p, 5)
    assert out.splitlines() == ["line95", "line96", "line97", "line98", "line99"]


def test_tail_missing_file_is_empty(tmp_path):
    assert _tail(tmp_path / "nope.log", 10) == ""


def test_human_bytes():
    assert _human_bytes(0) == "0.0 B"
    assert _human_bytes(1536) == "1.5 KB"
    assert _human_bytes(5 * 1024 ** 3) == "5.0 GB"


def test_report_includes_core_sections(tmp_path):
    (tmp_path / "transcoder.log").write_text(
        "2026-06-09 10:00:00 - INFO - Transcode complete: foo.mov\n", encoding="utf-8"
    )
    pub = _make_publisher(tmp_path)
    report = pub._build_report()
    assert "HeavyDrops status" in report
    assert "jobs by state" in report and "done=12" in report
    assert "today" in report and "5.0 GB" in report and "saved" in report
    assert "transcoder.log" in report
    assert "Transcode complete: foo.mov" in report
    # No crash file yet → reported as empty/clean.
    assert "crash.log — empty" in report


def test_report_flags_new_crash(tmp_path):
    (tmp_path / "transcoder.log").write_text("hello\n", encoding="utf-8")
    pub = _make_publisher(tmp_path)
    # Crash appears after the publisher started → must be flagged NEW.
    (tmp_path / "crash.log").write_text(
        "UNHANDLED EXCEPTION (thread 'transcoder-1')\nRuntimeError: boom\n",
        encoding="utf-8",
    )
    report = pub._build_report()
    assert "NEW since last report" in report
    assert "RuntimeError: boom" in report
    # Second report with no further growth → no longer flagged NEW.
    report2 = pub._build_report()
    assert "NEW since last report" not in report2


def test_path_is_per_machine(tmp_path):
    pub = _make_publisher(tmp_path)
    path = pub._path()
    assert path.startswith("telemetry/") and path.endswith("-status.md")


def test_run_exits_when_disabled(tmp_path):
    pub = _make_publisher(tmp_path, enabled=False)
    pub.run()  # returns immediately, no network


def test_run_exits_without_token(tmp_path):
    pub = _make_publisher(tmp_path, token="")
    pub.run()  # returns immediately, no network
