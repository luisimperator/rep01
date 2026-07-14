"""Tests for watchdog kill-on-timeout, the state_changed_at timeout clock,
and continuous disk-reservation reconciliation.

Regression suite for the v7.9 HEAVY7 incident: two ffmpeg processes ran for
6+ days (a near-frozen transcode trickled row updates, and the trigger-
refreshed updated_at meant the timeout never fired), their jobs pinned the
entire 1.82TB staging budget, and every downloader stalled on
"staging budget exhausted" for 3 days straight.
"""
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from transcoder.database import ACTIVE_STATES, Database, Job, JobState  # noqa: E402
from transcoder.watchdog import Watchdog  # noqa: E402


@pytest.fixture
def db(tmp_path):
    d = Database(tmp_path / "test.db")
    d.initialize()
    yield d
    d.close()


def _add_job(db: Database, state: JobState, path: str = "/videos/a.mp4") -> int:
    job = db.create_job(
        dropbox_path=path,
        dropbox_rev="rev1",
        dropbox_size=1000,
        output_path=path.rsplit("/", 1)[0] + "/h265/" + path.rsplit("/", 1)[1],
    )
    db.update_job_state(job.id, state)
    return job.id


def _get_job(db: Database, job_id: int) -> Job:
    conn = db._get_connection()
    row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    return Job.from_row(row)


def _backdate_state_change(db: Database, job_id: int, hours: float) -> None:
    """Move state_changed_at into the past without touching state."""
    conn = db._get_connection()
    past = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    conn.execute(
        "UPDATE jobs SET state_changed_at = ? WHERE id = ?", (past, job_id)
    )
    conn.commit()


def _make_watchdog(db: Database, workers=None, disk_budget=None) -> Watchdog:
    config = MagicMock()
    config.watchdog.download_timeout_sec = 7200
    config.watchdog.transcode_timeout_sec = 86400
    config.watchdog.upload_timeout_sec = 7200
    config.watchdog.max_retries = 10
    config.watchdog.failed_revive_cooldown_sec = 600.0
    return Watchdog(
        config, db, threading.Event(),
        workers=workers or [], disk_budget=disk_budget,
    )


class TestStateChangedAtClock:
    """state_changed_at moves only on state transitions."""

    def test_stamped_on_state_transition(self, db):
        job_id = _add_job(db, JobState.TRANSCODING)
        job = _get_job(db, job_id)
        assert job.state_changed_at is not None
        age = (datetime.now(timezone.utc) - job.state_changed_at).total_seconds()
        assert age < 60

    def test_not_refreshed_by_non_state_row_updates(self, db):
        """The jobs_updated_at trigger refreshes updated_at on ANY row update;
        state_changed_at must survive that (this is the 6-day-zombie bug)."""
        job_id = _add_job(db, JobState.TRANSCODING)
        _backdate_state_change(db, job_id, hours=48)

        # A non-transition row update — e.g. a retry bump — fires the trigger.
        time.sleep(1.1)  # datetime('now') has 1s resolution
        db.increment_retry(job_id, max_retries=10)

        job = _get_job(db, job_id)
        # updated_at got refreshed by the trigger…
        assert (datetime.now(timezone.utc) - job.updated_at).total_seconds() < 60
        # …but the timeout clock did not move.
        age_h = (
            datetime.now(timezone.utc) - job.state_changed_at
        ).total_seconds() / 3600
        assert age_h > 47

    def test_zombie_times_out_despite_fresh_updated_at(self, db):
        """A transcode 48h in-state whose row was just touched must time out."""
        job_id = _add_job(db, JobState.TRANSCODING)
        _backdate_state_change(db, job_id, hours=48)
        db.increment_retry(job_id, max_retries=10)  # refreshes updated_at

        wd = _make_watchdog(db)
        job = _get_job(db, job_id)
        assert wd._is_job_timed_out(job, 86400, datetime.now(timezone.utc))

    def test_recover_active_jobs_restamps_clock(self, db):
        job_id = _add_job(db, JobState.TRANSCODING)
        _backdate_state_change(db, job_id, hours=48)
        db.recover_active_jobs()
        job = _get_job(db, job_id)
        assert job.state == JobState.RETRY_WAIT
        age = (datetime.now(timezone.utc) - job.state_changed_at).total_seconds()
        assert age < 60


class TestKillOnTimeout:
    """Timeout kills the in-flight process through the owning worker."""

    def test_worker_kill_claims_job_and_watchdog_stays_out_of_db(self, db):
        job_id = _add_job(db, JobState.TRANSCODING)
        _backdate_state_change(db, job_id, hours=48)
        job = _get_job(db, job_id)

        worker = MagicMock()
        worker.abort_job.return_value = True
        wd = _make_watchdog(db, workers=[worker])

        wd._handle_timeout(job, "Transcode timeout")

        worker.abort_job.assert_called_once()
        # State untouched — the killed worker's own failure path owns cleanup.
        assert _get_job(db, job_id).state == JobState.TRANSCODING
        assert _get_job(db, job_id).retry_count == 0

    def test_orphan_job_is_requeued_and_reservation_released(self, db):
        """No worker owns the job: watchdog flips state AND frees the budget."""
        job_id = _add_job(db, JobState.TRANSCODING)
        _backdate_state_change(db, job_id, hours=48)
        db.reserve_disk(job_id, 10_000_000_000)
        job = _get_job(db, job_id)

        budget = MagicMock()
        wd = _make_watchdog(db, workers=[], disk_budget=budget)

        wd._handle_timeout(job, "Transcode timeout")

        assert _get_job(db, job_id).state == JobState.RETRY_WAIT
        assert _get_job(db, job_id).retry_count == 1
        budget.release.assert_called_once_with(job_id)

    def test_stuck_worker_falls_through_after_two_kill_attempts(self, db):
        """abort_job returns True but never unsticks — attempt 3 takes over."""
        job_id = _add_job(db, JobState.TRANSCODING)
        _backdate_state_change(db, job_id, hours=48)
        job = _get_job(db, job_id)

        worker = MagicMock()
        worker.abort_job.return_value = True
        budget = MagicMock()
        wd = _make_watchdog(db, workers=[worker], disk_budget=budget)

        wd._handle_timeout(job, "Transcode timeout")  # kill attempt 1
        wd._handle_timeout(job, "Transcode timeout")  # kill attempt 2
        assert _get_job(db, job_id).state == JobState.TRANSCODING

        wd._handle_timeout(job, "Transcode timeout")  # watchdog takes over
        assert _get_job(db, job_id).state == JobState.RETRY_WAIT
        budget.release.assert_called_once_with(job_id)

    def test_workers_without_abort_hook_are_skipped(self, db):
        job_id = _add_job(db, JobState.UPLOADING)
        _backdate_state_change(db, job_id, hours=48)
        job = _get_job(db, job_id)

        not_a_worker = object()  # e.g. the Watchdog itself in main.workers
        wd = _make_watchdog(db, workers=[not_a_worker])

        wd._handle_timeout(job, "Upload timeout")
        assert _get_job(db, job_id).state == JobState.RETRY_WAIT


class TestStaleReservationReconcile:
    """Leaked reservations are reclaimed continuously, not just at startup."""

    def test_reservation_of_terminal_job_is_pruned(self, db):
        done_id = _add_job(db, JobState.DONE, path="/videos/done.mp4")
        active_id = _add_job(db, JobState.DOWNLOADING, path="/videos/dl.mp4")
        db.reserve_disk(done_id, 5_000_000_000)
        db.reserve_disk(active_id, 7_000_000_000)
        assert db.total_reserved_bytes() == 12_000_000_000

        wd = _make_watchdog(db)
        wd._check_stale_reservations()

        # DONE job's leak reclaimed; the active job's reservation survives.
        assert db.total_reserved_bytes() == 7_000_000_000

    def test_noop_when_nothing_leaked(self, db):
        active_id = _add_job(db, JobState.TRANSCODING)
        db.reserve_disk(active_id, 1_000)
        wd = _make_watchdog(db)
        wd._check_stale_reservations()
        assert db.total_reserved_bytes() == 1_000
