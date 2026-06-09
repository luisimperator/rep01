"""Tests for the night-window + idle availability gate (pure logic)."""
import sys
from datetime import datetime, time as dtime
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from transcoder.availability import within_window, parse_hhmm, AvailabilityGate


def _gate(**kw):
    defaults = dict(
        enabled=True, night_start="20:00", night_end="07:00",
        pause_when_user_active=True, idle_minutes=10, check_interval_sec=60,
    )
    defaults.update(kw)
    return AvailabilityGate(SimpleNamespace(availability=SimpleNamespace(**defaults)))


def test_parse_hhmm():
    assert parse_hhmm("20:00") == dtime(20, 0)
    assert parse_hhmm(" 7:05 ") == dtime(7, 5)


def test_within_window_daytime():
    w = (dtime(9, 0), dtime(17, 0))
    assert within_window(dtime(12, 0), *w)
    assert not within_window(dtime(8, 0), *w)
    assert not within_window(dtime(20, 0), *w)


def test_within_window_overnight():
    w = (dtime(20, 0), dtime(7, 0))
    assert within_window(dtime(23, 0), *w)   # late night
    assert within_window(dtime(3, 0), *w)    # small hours
    assert within_window(dtime(7, 0), *w)    # boundary
    assert not within_window(dtime(12, 0), *w)  # midday → blocked
    assert not within_window(dtime(19, 0), *w)  # just before start


def test_within_window_degenerate_equal_is_always_on():
    assert within_window(dtime(12, 0), dtime(0, 0), dtime(0, 0))


NIGHT = datetime(2026, 1, 15, 23, 0)   # 23:00 — inside 20:00-07:00
DAY = datetime(2026, 1, 15, 12, 0)     # 12:00 — outside


def test_disabled_always_works():
    ok, _ = _gate(enabled=False).should_work(DAY, idle_seconds=0)
    assert ok


def test_outside_window_blocks():
    ok, reason = _gate().should_work(DAY, idle_seconds=10_000)
    assert not ok and "window" in reason


def test_night_and_idle_works():
    ok, reason = _gate().should_work(NIGHT, idle_seconds=15 * 60)
    assert ok and "idle" in reason


def test_night_but_user_active_blocks():
    ok, reason = _gate().should_work(NIGHT, idle_seconds=30)  # 30s < 10min
    assert not ok and "using the machine" in reason


def test_idle_unknown_does_not_block_at_night():
    # Non-Windows / undetectable idle → None → don't gate on idle.
    ok, _ = _gate().should_work(NIGHT, idle_seconds=None)
    assert ok


def test_idle_check_off_ignores_activity():
    ok, _ = _gate(pause_when_user_active=False).should_work(NIGHT, idle_seconds=0)
    assert ok


def test_invalid_window_fails_closed():
    ok, reason = _gate(night_start="banana").should_work(NIGHT, idle_seconds=None)
    assert not ok and "invalid" in reason
