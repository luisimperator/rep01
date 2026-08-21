"""Tests for the timed pause ("Pause 3h" dashboard button).

A timed pause must behave exactly like a manual pause while active, and
undo itself once the deadline passes — the whole point is that a pause
can't be forgotten and leave the machine idle for days.
"""
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from transcoder.api import _pause_duration_sec  # noqa: E402
from transcoder.dispatcher import JobDispatcher  # noqa: E402


def _dispatcher() -> JobDispatcher:
    d = object.__new__(JobDispatcher)
    d._paused = threading.Event()
    d._pause_until = None
    return d


class TestTimedPause:
    def test_timed_pause_pauses_and_reports_remaining(self):
        d = _dispatcher()
        d.pause(3 * 3600)
        assert d.is_paused()
        rem = d.pause_remaining_sec()
        assert rem is not None and 3 * 3600 - 5 < rem <= 3 * 3600

    def test_expires_only_after_deadline(self):
        d = _dispatcher()
        d.pause(3600)
        d._expire_timed_pause()
        assert d.is_paused()  # deadline far away — stays paused

        d._pause_until = time.monotonic() - 1  # deadline passed
        d._expire_timed_pause()
        assert not d.is_paused()
        assert d.pause_remaining_sec() is None

    def test_plain_pause_never_auto_resumes(self):
        d = _dispatcher()
        d.pause()
        assert d.pause_remaining_sec() is None
        d._expire_timed_pause()
        assert d.is_paused()

    def test_plain_pause_overrides_pending_timer(self):
        """Pause 3h followed by a manual Pause = pause forever (the manual
        click is the stronger intent)."""
        d = _dispatcher()
        d.pause(3600)
        d.pause()
        assert d.pause_remaining_sec() is None
        d._pause_until = None
        d._expire_timed_pause()
        assert d.is_paused()

    def test_timed_pauses_stack(self):
        """Clicking Pause 3h while already timed-paused ADDS 3h (3h -> 6h),
        instead of restarting the 3h window."""
        d = _dispatcher()
        d.pause(3 * 3600)
        d.pause(3 * 3600)
        rem = d.pause_remaining_sec()
        assert rem is not None and 6 * 3600 - 5 < rem <= 6 * 3600

        d.pause(3 * 3600)
        rem = d.pause_remaining_sec()
        assert rem is not None and 9 * 3600 - 5 < rem <= 9 * 3600

    def test_timed_pause_on_manual_pause_arms_from_now(self):
        """A timed pause on top of a manual (untimed) pause has no deadline
        to extend — it arms a fresh one from now."""
        d = _dispatcher()
        d.pause()
        d.pause(3 * 3600)
        rem = d.pause_remaining_sec()
        assert rem is not None and 3 * 3600 - 5 < rem <= 3 * 3600

    def test_timed_pause_after_resume_starts_fresh(self):
        """A stale deadline from an earlier pause must not leak into a new
        one: pause 3h, resume, pause 3h again -> 3h, not 6h."""
        d = _dispatcher()
        d.pause(3 * 3600)
        d.resume()
        d.pause(3 * 3600)
        rem = d.pause_remaining_sec()
        assert rem is not None and 3 * 3600 - 5 < rem <= 3 * 3600

    def test_resume_clears_timer(self):
        d = _dispatcher()
        d.pause(3600)
        d.resume()
        assert not d.is_paused()
        assert d.pause_remaining_sec() is None


class TestPauseDurationParsing:
    def test_hours(self):
        assert _pause_duration_sec({"hours": ["3"]}) == 3 * 3600

    def test_seconds_and_fractional_hours(self):
        assert _pause_duration_sec({"seconds": ["90"]}) == 90
        assert _pause_duration_sec({"hours": ["0.5"]}) == 1800

    def test_absent_or_invalid_means_plain_pause(self):
        assert _pause_duration_sec({}) is None
        assert _pause_duration_sec({"hours": ["banana"]}) is None
        assert _pause_duration_sec({"hours": ["0"]}) is None
        assert _pause_duration_sec({"hours": ["-2"]}) is None
