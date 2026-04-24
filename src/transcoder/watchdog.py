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
    ):
        """
        Initialize watchdog.

        Args:
            config: Application configuration.
            db: Database instance.
            stop_event: Event to signal shutdown.
            check_interval: Seconds between checks.
        """
        super().__init__(name="watchdog", daemon=True)
        self.config = config
        self.db = db
        self.stop_event = stop_event
        self.check_interval = check_interval

    def run(self) -> None:
        """Main watchdog loop."""
        logger.info("Watchdog started")

        while not self.stop_event.is_set():
            try:
                self._check_timeouts()
                self._check_retry_ready()
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

        # Check downloading jobs
        for job in self.db.get_jobs_by_state(JobState.DOWNLOADING, limit=100):
            if self._is_job_timed_out(job, self.config.watchdog.download_timeout_sec, now):
                logger.warning(f"Job {job.id} download timeout, requeueing")
                self._handle_timeout(job, "Download timeout")

        # Check transcoding jobs
        for job in self.db.get_jobs_by_state(JobState.TRANSCODING, limit=100):
            if self._is_job_timed_out(job, self.config.watchdog.transcode_timeout_sec, now):
                logger.warning(f"Job {job.id} transcode timeout, requeueing")
                self._handle_timeout(job, "Transcode timeout")

        # Check uploading jobs
        for job in self.db.get_jobs_by_state(JobState.UPLOADING, limit=100):
            if self._is_job_timed_out(job, self.config.watchdog.upload_timeout_sec, now):
                logger.warning(f"Job {job.id} upload timeout, requeueing")
                self._handle_timeout(job, "Upload timeout")

    def _is_job_timed_out(
        self,
        job: Job,
        timeout_sec: int,
        now: datetime,
    ) -> bool:
        """Check if job has exceeded timeout."""
        # Use updated_at as the state entry time
        if not job.updated_at:
            return False

        elapsed = (now - job.updated_at).total_seconds()
        return elapsed > timeout_sec

    def _handle_timeout(self, job: Job, reason: str) -> None:
        """Handle a timed-out job."""
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

    def _check_retry_ready(self) -> None:
        """Check for RETRY_WAIT jobs ready to be retried."""
        now = datetime.now(timezone.utc)

        for job in self.db.get_jobs_by_state(JobState.RETRY_WAIT, limit=100):
            # Calculate backoff delay
            delay = min(300, 5 * (2 ** job.retry_count))

            if job.updated_at:
                elapsed = (now - job.updated_at).total_seconds()
                if elapsed >= delay:
                    logger.info(f"Job {job.id} ready for retry after {elapsed:.0f}s backoff")
                    self.db.update_job_state(job.id, JobState.NEW)


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
