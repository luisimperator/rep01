"""Availability gating: only let a production machine encode at night while idle.

HEAVY7 is a dedicated box (gating off → runs 24/7). The editors' machines
(Heavy1-6) are busy by day and idle at night; we want them to chip in on the
transcode pool overnight WITHOUT ever competing with an editor. This module
pauses the pipeline (and frees the GPU) whenever we're outside the configured
night window OR someone is actively using the machine, and resumes when it's
safe again.

It only ever undoes a pause that IT issued, so a manual pause from the
dashboard (/api/pause) is never overridden.
"""

from __future__ import annotations

import logging
import sys
import threading
from datetime import datetime, time as dtime
from typing import Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from .config import Config
    from .dispatcher import JobDispatcher

logger = logging.getLogger(__name__)


def user_idle_seconds() -> float | None:
    """Seconds since the last keyboard/mouse input.

    Windows only (the editor machines are Windows). Returns None where it can't
    be determined, which callers treat as "can't tell → don't block on idle".
    """
    if sys.platform != "win32":
        return None
    try:
        import ctypes

        class _LASTINPUTINFO(ctypes.Structure):
            _fields_ = [("cbSize", ctypes.c_uint), ("dwTime", ctypes.c_uint)]

        info = _LASTINPUTINFO()
        info.cbSize = ctypes.sizeof(_LASTINPUTINFO)
        if not ctypes.windll.user32.GetLastInputInfo(ctypes.byref(info)):
            return None
        millis = ctypes.windll.kernel32.GetTickCount() - info.dwTime
        return max(0.0, millis / 1000.0)
    except Exception:  # pragma: no cover — defensive, never block on failure
        return None


def parse_hhmm(value: str) -> dtime:
    """Parse 'HH:MM' into a time. Raises ValueError on a bad string."""
    hh, mm = value.strip().split(":")
    return dtime(int(hh), int(mm))


def within_window(now_t: dtime, start: dtime, end: dtime) -> bool:
    """Is now_t inside [start, end], handling overnight windows (start > end)?"""
    if start == end:
        return True  # degenerate → treat as always-on
    if start < end:
        return start <= now_t <= end
    # Overnight, e.g. 20:00 → 07:00.
    return now_t >= start or now_t <= end


class AvailabilityGate:
    """Pure decision: may this machine encode right now?"""

    def __init__(self, config: "Config") -> None:
        self.config = config

    def should_work(
        self, now: datetime, idle_seconds: float | None
    ) -> tuple[bool, str]:
        a = self.config.availability
        if not a.enabled:
            return True, "availability gating off (24/7)"

        try:
            start, end = parse_hhmm(a.night_start), parse_hhmm(a.night_end)
        except Exception:
            # Misconfigured window → fail OPEN would risk disrupting editors,
            # so fail CLOSED (don't work) and say why.
            return False, f"invalid night window {a.night_start!r}-{a.night_end!r}"

        if not within_window(now.time(), start, end):
            return False, f"outside work window {a.night_start}-{a.night_end}"

        if a.pause_when_user_active and idle_seconds is not None:
            if idle_seconds < a.idle_minutes * 60:
                return False, f"someone is using the machine ({idle_seconds:.0f}s idle)"

        return True, "inside night window, machine idle"


class AvailabilityWorker(threading.Thread):
    """Supervises the gate: pauses the dispatcher + frees the GPU when we must
    yield, resumes when it's safe. Only undoes its own pauses."""

    def __init__(
        self,
        config: "Config",
        dispatcher: "JobDispatcher",
        stop_event: threading.Event,
        kill_ffmpeg: Callable[[], int],
    ) -> None:
        super().__init__(name="availability", daemon=True)
        self.config = config
        self.dispatcher = dispatcher
        self.stop_event = stop_event
        self.kill_ffmpeg = kill_ffmpeg
        self.gate = AvailabilityGate(config)
        self._paused_by_us = False

    def run(self) -> None:
        a = self.config.availability
        if not a.enabled:
            logger.info("availability: gating disabled; machine runs 24/7")
            return
        logger.info(
            "availability: night window %s-%s, pause when active=%s (idle>%dm)",
            a.night_start, a.night_end, a.pause_when_user_active, a.idle_minutes,
        )
        interval = max(5, int(a.check_interval_sec))
        last_reason = None
        while not self.stop_event.is_set():
            try:
                should, reason = self.gate.should_work(
                    datetime.now(), user_idle_seconds()
                )
                if should:
                    if self._paused_by_us and self.dispatcher.is_paused():
                        self.dispatcher.resume()
                        self._paused_by_us = False
                        logger.info("availability: RESUMING — %s", reason)
                else:
                    if not self.dispatcher.is_paused():
                        self.dispatcher.pause()
                        self._paused_by_us = True
                        killed = 0
                        try:
                            killed = self.kill_ffmpeg()
                        except Exception:
                            logger.warning("availability: GPU free failed", exc_info=True)
                        logger.info(
                            "availability: YIELDING — %s (paused pipeline, "
                            "killed %d ffmpeg to free the GPU)", reason, killed,
                        )
                if reason != last_reason:
                    logger.debug("availability: %s → work=%s", reason, should)
                    last_reason = reason
            except Exception:
                logger.warning("availability: check failed", exc_info=True)
            if self.stop_event.wait(interval):
                break
        logger.info("availability: supervisor stopped")
