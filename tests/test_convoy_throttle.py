"""Regression tests for convoy/starvation mode (the 'leader' download boost).

When the transcoder queue is empty and ≥2 downloaders are pulling bytes, the
dispatcher elects ONE leader to run full-speed and tells the rest to pause in
their progress callback, so the leader's file finishes first and feeds the
starving transcoder. Before v6.8.5 the "pause" was a fixed 5s/chunk sleep that
still leaked ~1.5 MB/s per non-leader — on a fat pipe three non-leaders ate
more of the WAN than the leader, so the leader never sped up. The fix turns the
sleep into a real pause (poll-until-clear) capped by convoy_keepalive_sec.

These tests pin the dispatcher-side election/clear contract that the worker
pause loop depends on, plus the new keepalive knob.
"""
import sys
import threading
from pathlib import Path
from queue import Queue

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from transcoder.config import DispatcherSettings  # noqa: E402
from transcoder.dispatcher import JobDispatcher  # noqa: E402


def _bare_dispatcher(throttle: float = 5.0) -> JobDispatcher:
    """A JobDispatcher with only the convoy state should_throttle_download needs."""
    d = object.__new__(JobDispatcher)
    d.convoy_throttle_sec = throttle
    d.convoy_keepalive_sec = 20.0
    d.transcode_q = Queue()
    d._convoy_lock = threading.Lock()
    d._convoy_leader = None
    d._download_active = {}
    return d


# --- leader election ----------------------------------------------------------

def test_first_asker_is_leader_rest_are_throttled():
    d = _bare_dispatcher()
    d._download_active = {"downloader-0": 1, "downloader-1": 2}

    # First worker to ask wins the lead and runs full speed.
    assert d.should_throttle_download("downloader-0") is False
    assert d._convoy_leader == "downloader-0"
    # Everyone else pauses.
    assert d.should_throttle_download("downloader-1") is True


def test_single_downloader_is_never_throttled():
    d = _bare_dispatcher()
    d._download_active = {"downloader-0": 1}
    assert d.should_throttle_download("downloader-0") is False
    assert d._convoy_leader is None


def test_disabled_when_throttle_sec_zero():
    d = _bare_dispatcher(throttle=0.0)
    d._download_active = {"downloader-0": 1, "downloader-1": 2}
    assert d.should_throttle_download("downloader-1") is False


# --- convoy clears so non-leaders resume -------------------------------------

def test_convoy_clears_when_transcoder_fed():
    d = _bare_dispatcher()
    d._download_active = {"downloader-0": 1, "downloader-1": 2}
    assert d.should_throttle_download("downloader-0") is False  # elect leader
    assert d.should_throttle_download("downloader-1") is True   # paused

    # Transcoder now has work -> convoy must release immediately so the paused
    # worker's poll loop exits and it resumes at full speed.
    d.transcode_q.put(object())
    assert d.should_throttle_download("downloader-1") is False
    assert d._convoy_leader is None


def test_leader_that_vanished_is_re_elected():
    d = _bare_dispatcher()
    d._download_active = {"downloader-0": 1, "downloader-1": 2, "downloader-2": 3}
    assert d.should_throttle_download("downloader-0") is False
    assert d._convoy_leader == "downloader-0"

    # Leader finished/unregistered; with ≥2 still in flight a fresh leader must
    # be electable so the convoy doesn't deadlock with everyone paused waiting
    # on a dead leader.
    d._download_active.pop("downloader-0")
    assert d.should_throttle_download("downloader-1") is False
    assert d._convoy_leader == "downloader-1"


def test_drops_below_two_active_releases_throttle():
    d = _bare_dispatcher()
    d._download_active = {"downloader-0": 1, "downloader-1": 2}
    d.should_throttle_download("downloader-0")  # elect
    assert d.should_throttle_download("downloader-1") is True

    d._download_active.pop("downloader-0")
    # Only one downloader left -> nothing to throttle.
    assert d.should_throttle_download("downloader-1") is False
    assert d._convoy_leader is None


# --- keepalive knob -----------------------------------------------------------

def test_keepalive_default_and_bounds():
    s = DispatcherSettings()
    assert s.convoy_keepalive_sec == 20.0

    # bounded 1..90
    assert DispatcherSettings(convoy_keepalive_sec=1.0).convoy_keepalive_sec == 1.0
    assert DispatcherSettings(convoy_keepalive_sec=90.0).convoy_keepalive_sec == 90.0
    with pytest.raises(Exception):
        DispatcherSettings(convoy_keepalive_sec=0.0)
    with pytest.raises(Exception):
        DispatcherSettings(convoy_keepalive_sec=120.0)
