"""
Watchdog module for job timeout monitoring.

Monitors running jobs and kills/requeues them if they exceed timeouts.
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from .database import ACTIVE_STATES, Database, Job, JobState
from .utils import SUBPROCESS_FLAGS

if TYPE_CHECKING:
    from .config import Config

logger = logging.getLogger(__name__)


class Watchdog(threading.Thread):
    """
    Monitors jobs for timeouts and handles stale jobs.

    Checks periodically for:
    - Jobs stuck in active states too long
    - Jobs that need retry after backoff period
    """

    def __init__(
        self,
        config: Config,
        db: Database,
        stop_event: threading.Event,
        check_interval: int = 60,
        workers: list | None = None,
        disk_budget=None,
    ):
        """
        Initialize watchdog.

        Args:
            config: Application configuration.
            db: Database instance.
            stop_event: Event to signal shutdown.
            check_interval: Seconds between checks.
            workers: Pipeline workers (anything exposing abort_job). On a
                timeout the watchdog first kills the offending job's in-flight
                process through its worker, so the stuck thread unblocks and
                runs its own cleanup, instead of just flipping DB state under
                a still-running ffmpeg.
            disk_budget: DiskBudget, so orphaned timeouts release their
                staging reservation instead of deadlocking the budget.
        """
        super().__init__(name="watchdog", daemon=True)
        self.config = config
        self.db = db
        self.stop_event = stop_event
        self.check_interval = check_interval
        self.workers = workers if workers is not None else []
        self.disk_budget = disk_budget
        # job_id -> soft-kill attempts. After 2 ticks where the kill didn't
        # unstick the worker, the watchdog takes over the DB transition itself.
        self._kill_attempts: dict[int, int] = {}
        # Failed-revive cooldown: when the download pipeline goes idle we
        # promote every FAILED job back to RETRY_WAIT to give it another
        # shot. Cooldown prevents thrashing if the same jobs immediately
        # re-fail on a flaky network minute.
        self._last_failed_revive_at: float = 0.0
        self._failed_revive_cooldown_sec: float = float(
            getattr(config.watchdog, "failed_revive_cooldown_sec", 600.0)
        )

    def run(self) -> None:
        """Main watchdog loop."""
        logger.info("Watchdog started")

        while not self.stop_event.is_set():
            try:
                self._check_timeouts()
                self._check_retry_ready()
                self._check_failed_revive()
                self._check_stale_reservations()
            except Exception as e:
                logger.error(f"Watchdog error: {e}")

            # Wait for next check
            for _ in range(self.check_interval):
                if self.stop_event.is_set():
                    break
                time.sleep(1)

        logger.info("Watchdog stopped")

    def _check_timeouts(self) -> None:
        """Check for jobs that have exceeded timeout."""
        now = datetime.now(timezone.utc)
        active_ids: set[int] = set()

        # Check downloading jobs
        for job in self.db.get_jobs_by_state(JobState.DOWNLOADING, limit=100):
            active_ids.add(job.id)
            if self._is_job_timed_out(job, self.config.watchdog.download_timeout_sec, now):
                logger.warning(f"Job {job.id} download timeout, requeueing")
                self._handle_timeout(job, "Download timeout")

        # Check transcoding jobs
        for job in self.db.get_jobs_by_state(JobState.TRANSCODING, limit=100):
            active_ids.add(job.id)
            if self._is_job_timed_out(job, self.config.watchdog.transcode_timeout_sec, now):
                logger.warning(f"Job {job.id} transcode timeout, requeueing")
                self._handle_timeout(job, "Transcode timeout")

        # Check uploading jobs
        for job in self.db.get_jobs_by_state(JobState.UPLOADING, limit=100):
            active_ids.add(job.id)
            if self._is_job_timed_out(job, self.config.watchdog.upload_timeout_sec, now):
                logger.warning(f"Job {job.id} upload timeout, requeueing")
                self._handle_timeout(job, "Upload timeout")

        # Kill-attempt counters for jobs that left the active states (the
        # worker's cleanup ran, or an operator intervened) are done with.
        self._kill_attempts = {
            jid: n for jid, n in self._kill_attempts.items() if jid in active_ids
        }

    @staticmethod
    def _state_clock(job: Job) -> datetime | None:
        """Timestamp the job entered its current state.

        Prefer state_changed_at: updated_at is refreshed by the
        jobs_updated_at trigger on EVERY row update (progress, retry bumps),
        so a near-frozen ffmpeg that trickles updates resets updated_at
        forever and its job never times out (the 6-day zombie transcode).
        Fall back to updated_at only for rows read mid-migration.
        """
        return job.state_changed_at or job.updated_at

    def _is_job_timed_out(
        self,
        job: Job,
        timeout_sec: int,
        now: datetime,
    ) -> bool:
        """Check if job has exceeded timeout."""
        entered = self._state_clock(job)
        if not entered:
            return False

        elapsed = (now - entered).total_seconds()
        return elapsed > timeout_sec

    def _handle_timeout(self, job: Job, reason: str) -> None:
        """Handle a timed-out job.

        First try to kill the in-flight process (ffmpeg / download stream)
        through the worker that owns the job. The kill unblocks the stuck
        worker thread, whose own failure path then runs the full cleanup —
        retry increment, RETRY_WAIT/FAILED, disk-budget release, staging rm —
        with no state race against the watchdog.

        Only when no worker owns the job (orphaned state after a crash), or
        the kill failed to unstick it after 2 ticks, does the watchdog take
        over: flip the DB state and reclaim the staging reservation itself.
        """
        attempts = self._kill_attempts.get(job.id, 0)
        if attempts < 2 and self._abort_in_flight(job, reason):
            self._kill_attempts[job.id] = attempts + 1
            return

        self._kill_attempts.pop(job.id, None)
        retry_count, should_fail = self.db.increment_retry(
            job.id,
            self.config.watchdog.max_retries,
        )

        if should_fail:
            self.db.update_job_state(
                job.id,
                JobState.FAILED,
                error_message=f"{reason} (max retries exceeded)",
            )
            logger.error(f"Job {job.id} failed permanently: {reason}")
        else:
            self.db.update_job_state(
                job.id,
                JobState.RETRY_WAIT,
                error_message=f"{reason} (retry {retry_count})",
            )

        # The owning worker is gone (or hopelessly stuck) — nobody else will
        # ever release this job's staging reservation. Reclaim it here or the
        # budget stays exhausted and every downloader stalls forever.
        if self.disk_budget is not None:
            self.disk_budget.release(job.id)

    def _abort_in_flight(self, job: Job, reason: str) -> bool:
        """Ask each pipeline worker to kill its in-flight process for `job`.

        Returns True when a worker owned the job AND actually delivered a
        kill (ffmpeg terminated / download stream aborted).
        """
        for worker in self.workers:
            abort = getattr(worker, "abort_job", None)
            if abort is None:
                continue
            try:
                if abort(job.id, reason):
                    return True
            except Exception:
                logger.exception(
                    "abort_job(%s) failed on worker %s", job.id, worker
                )
        return False

    def _check_stale_reservations(self) -> None:
        """Reclaim disk reservations whose jobs are no longer active.

        Startup already prunes these, but a daemon that runs for weeks needs
        the same self-healing continuously: one leaked reservation (worker
        crash, kill -9, code bug) otherwise pins the budget until the next
        restart — the 3-day 'staging budget exhausted' deadlock of v7.9.
        """
        try:
            pruned = self.db.prune_stale_disk_reservations(ACTIVE_STATES)
        except Exception:
            logger.exception("prune_stale_disk_reservations failed")
            return
        if pruned:
            logger.warning(
                "watchdog: reclaimed %d stale disk reservation(s) held by "
                "non-active jobs — staging budget was leaking",
                pruned,
            )

    def _check_retry_ready(self) -> None:
        """Check for RETRY_WAIT jobs ready to be retried."""
        now = datetime.now(timezone.utc)

        for job in self.db.get_jobs_by_state(JobState.RETRY_WAIT, limit=100):
            # Calculate backoff delay
            delay = min(300, 5 * (2 ** job.retry_count))

            entered = self._state_clock(job)
            if entered:
                elapsed = (now - entered).total_seconds()
                if elapsed >= delay:
                    logger.info(f"Job {job.id} ready for retry after {elapsed:.0f}s backoff")
                    self.db.update_job_state(job.id, JobState.NEW)

    def _check_failed_revive(self) -> None:
        """When the download pipeline is idle, give every FAILED job another
        shot. Idle = no jobs in NEW, RETRY_WAIT, or DOWNLOADING state.
        Otherwise the downloaders are either busy or about to be busy and
        we don't want to pile FAILED retries on top of pending work.

        Cooldown (default 10 min) prevents thrashing on a flaky network
        where the same jobs would re-fail immediately after each revive.
        """
        if self._failed_revive_cooldown_sec <= 0:
            return

        now_mono = time.monotonic()
        if (now_mono - self._last_failed_revive_at) < self._failed_revive_cooldown_sec:
            return

        # Pipeline-busy guards — scoped to current watch_folder so stale
        # NEW/RETRY_WAIT jobs from an old dropbox_root (which the
        # dispatcher already ignores, v6.8.1) don't permanently block
        # the revive of in-scope FAILED jobs.
        watch_root = getattr(self.config, "dropbox_root", None) or None
        if self.db.count_jobs(JobState.DOWNLOADING, path_prefix=watch_root) > 0:
            return
        if self.db.count_jobs(JobState.NEW, path_prefix=watch_root) > 0:
            return
        if self.db.count_jobs(JobState.RETRY_WAIT, path_prefix=watch_root) > 0:
            return

        failed_count = self.db.count_jobs(JobState.FAILED, path_prefix=watch_root)
        if failed_count == 0:
            return

        reset = self.db.reset_failed_jobs(path_prefix=watch_root)
        self._last_failed_revive_at = now_mono
        scope = f" under {watch_root}" if watch_root else ""
        logger.info(
            "watchdog: download pipeline idle — revived %d FAILED job(s)%s "
            "back to RETRY_WAIT for another attempt (next cooldown %.0fs)",
            reset,
            scope,
            self._failed_revive_cooldown_sec,
        )


class HealthChecker:
    """
    Checks system health for the doctor command.

    Verifies:
    - FFmpeg/FFprobe availability
    - Encoder availability
    - Dropbox authentication
    - Disk space
    - Database connectivity
    """

    def __init__(self, config: Config):
        """Initialize health checker."""
        self.config = config

    def run_all_checks(self) -> dict[str, dict]:
        """
        Run all health checks.

        Returns:
            Dict mapping check name to result dict with 'ok' and 'message' keys.
        """
        results = {}

        results['ffmpeg'] = self._check_ffmpeg()
        results['ffprobe'] = self._check_ffprobe()
        results['encoders'] = self._check_encoders()
        results['dropbox'] = self._check_dropbox()
        results['staging_disk'] = self._check_disk_space(self.config.local_staging_dir)
        results['database'] = self._check_database()

        return results

    def _check_ffmpeg(self) -> dict:
        """Check FFmpeg availability."""
        import subprocess
        try:
            result = subprocess.run(
                [self.config.ffmpeg_path, "-version"],
                capture_output=True,
                text=True,
                timeout=10,
                **SUBPROCESS_FLAGS,
            )
            if result.returncode == 0:
                # Extract version
                version_line = result.stdout.split('\n')[0]
                return {'ok': True, 'message': version_line}
            return {'ok': False, 'message': f"FFmpeg error: {result.stderr}"}
        except FileNotFoundError:
            return {'ok': False, 'message': f"FFmpeg not found at: {self.config.ffmpeg_path}"}
        except Exception as e:
            return {'ok': False, 'message': f"FFmpeg check failed: {e}"}

    def _check_ffprobe(self) -> dict:
        """Check FFprobe availability."""
        import subprocess
        try:
            result = subprocess.run(
                [self.config.ffprobe_path, "-version"],
                capture_output=True,
                text=True,
                timeout=10,
                **SUBPROCESS_FLAGS,
            )
            if result.returncode == 0:
                version_line = result.stdout.split('\n')[0]
                return {'ok': True, 'message': version_line}
            return {'ok': False, 'message': f"FFprobe error: {result.stderr}"}
        except FileNotFoundError:
            return {'ok': False, 'message': f"FFprobe not found at: {self.config.ffprobe_path}"}
        except Exception as e:
            return {'ok': False, 'message': f"FFprobe check failed: {e}"}

    def _check_encoders(self) -> dict:
        """Check available encoders."""
        from .encoder_detect import detect_available_encoders, EncoderType

        encoders = detect_available_encoders(self.config.ffmpeg_path)
        available = [e.name for e in encoders.values() if e.available]

        if not available:
            return {'ok': False, 'message': "No HEVC encoders available"}

        return {'ok': True, 'message': f"Available: {', '.join(available)}"}

    def _check_dropbox(self) -> dict:
        """Check Dropbox authentication."""
        if not self.config.dropbox_token:
            return {'ok': False, 'message': "No Dropbox token configured"}

        try:
            from .dropbox_client import DropboxClient
            client = DropboxClient(self.config.dropbox_token)
            if client.check_connection():
                account = client.get_account_info()
                return {'ok': True, 'message': f"Connected as: {account['email']}"}
            return {'ok': False, 'message': "Connection check failed"}
        except Exception as e:
            return {'ok': False, 'message': f"Dropbox error: {e}"}

    def _check_disk_space(self, path: Path) -> dict:
        """Check disk space at path."""
        import shutil
        from pathlib import Path

        try:
            path = Path(path)
            path.mkdir(parents=True, exist_ok=True)

            usage = shutil.disk_usage(path)
            free_gb = usage.free / (1024 ** 3)
            total_gb = usage.total / (1024 ** 3)
            pct_free = (usage.free / usage.total) * 100

            if free_gb < 50:
                return {
                    'ok': False,
                    'message': f"Low disk space: {free_gb:.1f} GB free ({pct_free:.1f}%)"
                }

            return {
                'ok': True,
                'message': f"{free_gb:.1f} GB free / {total_gb:.1f} GB total ({pct_free:.1f}%)"
            }
        except Exception as e:
            return {'ok': False, 'message': f"Disk check failed: {e}"}

    def _check_database(self) -> dict:
        """Check database connectivity."""
        try:
            db = Database(self.config.database_path)
            db.initialize()
            stats = db.get_stats()
            db.close()
            return {
                'ok': True,
                'message': f"Connected, {stats['total_jobs']} jobs in database"
            }
        except Exception as e:
            return {'ok': False, 'message': f"Database error: {e}"}


# Expose Path for type hints
from pathlib import Path
