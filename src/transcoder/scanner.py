"""
Dropbox folder scanner with file stability detection.

Implements R2 (stability checks) and R4/R5 (filtering).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from .database import Database, JobState
from .dropbox_client import DropboxClient, DropboxFileInfo
from .utils import (
    get_h265_log_path,
    get_output_path,
    is_in_h265_folder,
    is_partial_file,
    is_video_file,
    is_youtube_download,
    matches_exclude_pattern,
)

if TYPE_CHECKING:
    from .config import Config

logger = logging.getLogger(__name__)


class StabilityResult:
    """Result of stability check."""
    STABLE = "stable"
    WAITING = "waiting"
    CHANGED = "changed"


class Scanner:
    """
    Scans Dropbox folder for eligible video files.

    Implements:
    - R2: File stability detection (multiple checks over time)
    - R4: Exclude h265 output folders
    - R5: Minimum file size filter
    """

    def __init__(
        self,
        config: Config,
        db: Database,
        dropbox_client: DropboxClient,
    ):
        """
        Initialize scanner.

        Args:
            config: Application configuration.
            db: Database instance.
            dropbox_client: Dropbox client instance.
        """
        self.config = config
        self.db = db
        self.dropbox = dropbox_client
        # Cache for h265 feito.txt logs (cleared at start of each scan)
        self._h265_log_cache: dict[str, set[str]] = {}

    def scan(self, dry_run: bool = False) -> dict[str, int]:
        """
        Scan Dropbox folder for eligible files.

        Args:
            dry_run: If True, don't create jobs.

        Returns:
            Dict with counts: new, skipped_small, skipped_hevc, skipped_exists,
                             skipped_excluded, waiting_stable
        """
        stats = {
            'scanned': 0,
            'new': 0,
            'skipped_small': 0,
            'skipped_excluded': 0,
            'skipped_exists': 0,
            'skipped_h265_log': 0,
            'skipped_youtube': 0,
            'waiting_stable': 0,
            'already_queued': 0,
            'errors': 0,
        }

        # Clear h265 log cache at start of each scan
        self._h265_log_cache.clear()

        logger.info(f"Starting scan of {self.config.dropbox_root}")

        try:
            for file_info in self.dropbox.list_folder(
                self.config.dropbox_root,
                recursive=True,
            ):
                stats['scanned'] += 1

                try:
                    result = self._process_file(file_info, dry_run)
                    if result:
                        stats[result] += 1
                except Exception as e:
                    logger.error(f"Error processing {file_info.path}: {e}")
                    stats['errors'] += 1

        except Exception as e:
            logger.error(f"Scan failed: {e}")
            raise

        logger.info(
            f"Scan complete: {stats['scanned']} files scanned, "
            f"{stats['new']} new jobs, "
            f"{stats['waiting_stable']} waiting for stability, "
            f"{stats['skipped_h265_log']} already in h265 feito.txt, "
            f"{stats['skipped_youtube']} YouTube downloads skipped"
        )

        return stats

    def _process_file(
        self,
        file_info: DropboxFileInfo,
        dry_run: bool,
    ) -> str | None:
        """
        Process a single file from scan.

        Returns:
            Result category string or None.
        """
        path = file_info.path

        # Skip non-video files
        if not is_video_file(path, self.config.video_extensions):
            return None

        # R4: Skip h265 output folders
        if is_in_h265_folder(path):
            logger.debug(f"Skipping (in h265 folder): {path}")
            return 'skipped_excluded'

        # Skip partial files
        if is_partial_file(path):
            logger.debug(f"Skipping (partial file): {path}")
            return 'skipped_excluded'

        # Check exclude patterns
        if matches_exclude_pattern(path, self.config.exclude_patterns):
            logger.debug(f"Skipping (excluded pattern): {path}")
            return 'skipped_excluded'

        # Skip YouTube downloads (already well compressed)
        if is_youtube_download(path):
            logger.debug(f"Skipping (YouTube download): {path}")
            return 'skipped_youtube'

        # R5: Minimum size filter
        min_bytes = self.config.min_size_bytes()
        if file_info.size < min_bytes:
            logger.debug(
                f"Skipping (too small: {file_info.size / (1024**3):.2f} GB < "
                f"{self.config.min_size_gb} GB): {path}"
            )
            if not dry_run:
                self._create_skipped_job(file_info, JobState.SKIPPED_TOO_SMALL)
            return 'skipped_small'

        # Check if already in h265 feito.txt log (avoids unnecessary downloads)
        if self._is_in_h265_feito_log(file_info):
            logger.debug(f"Skipping (in h265 feito.txt): {path}")
            return 'skipped_h265_log'

        # Check if output already exists
        output_path = get_output_path(path)
        if self.dropbox.file_exists(output_path):
            logger.debug(f"Skipping (output exists): {path}")
            if not dry_run:
                self._create_skipped_job(file_info, JobState.SKIPPED_ALREADY_EXISTS)
            return 'skipped_exists'

        # Check if already in queue (by path+rev)
        existing_job = self.db.get_job_by_path(path)
        if existing_job:
            if existing_job.dropbox_rev == file_info.rev:
                # Same revision, already queued
                if existing_job.state not in {JobState.FAILED, JobState.RETRY_WAIT}:
                    logger.debug(f"Skipping (already queued): {path}")
                    return 'already_queued'
            else:
                # Revision changed, file was modified
                logger.info(f"File modified since last job: {path}")

        # R2: Check stability
        stability = self._check_stability(file_info)

        if stability == StabilityResult.STABLE:
            if not dry_run:
                self._create_new_job(file_info)
            logger.info(f"New job created: {path}")
            return 'new'
        else:
            logger.debug(f"Waiting for stability: {path}")
            return 'waiting_stable'

    def _is_in_h265_feito_log(self, file_info: DropboxFileInfo) -> bool:
        """
        Check if file is already logged in h265 feito.txt.

        Uses caching to avoid repeated Dropbox API calls for the same log file.

        Args:
            file_info: File metadata.

        Returns:
            True if file is in the log, False otherwise.
        """
        from pathlib import PurePosixPath

        log_path = get_h265_log_path(file_info.path)
        filename = PurePosixPath(file_info.path).name

        # Check cache first
        if log_path in self._h265_log_cache:
            return filename in self._h265_log_cache[log_path]

        # Read log file from Dropbox
        log_content = self.dropbox.read_text_file(log_path)
        if log_content is None:
            # Log file doesn't exist, cache empty set
            self._h265_log_cache[log_path] = set()
            return False

        # Parse log to extract filenames
        # Format: "2024-01-27 15:30:45 | video001.mp4 | 8500.5MB -> 2100.3MB (75.3% menor)"
        filenames = set()
        for line in log_content.splitlines():
            parts = line.split('|')
            if len(parts) >= 2:
                logged_filename = parts[1].strip()
                filenames.add(logged_filename)

        self._h265_log_cache[log_path] = filenames
        return filename in filenames

    def _check_stability(self, file_info: DropboxFileInfo) -> str:
        """
        Check if file is stable (R2).

        File is stable when:
        - Size, rev, and server_modified are the same for N consecutive checks
        - Minimum age since first check in sequence has passed

        Args:
            file_info: Current file metadata.

        Returns:
            StabilityResult value.
        """
        path = file_info.path
        checks_required = self.config.stability.checks_required
        min_age_sec = self.config.stability.min_age_sec

        # Get recent stability checks
        recent_checks = self.db.get_recent_stability_checks(path, limit=checks_required)

        # Record current check
        self.db.add_stability_check(
            dropbox_path=path,
            size=file_info.size,
            rev=file_info.rev,
            server_modified=file_info.server_modified.isoformat(),
            content_hash=file_info.content_hash,
        )

        # Not enough checks yet
        if len(recent_checks) < checks_required - 1:
            logger.debug(f"Stability: not enough checks ({len(recent_checks) + 1}/{checks_required}): {path}")
            return StabilityResult.WAITING

        # Check if all checks match current values
        for check in recent_checks:
            if (check.size != file_info.size or
                check.rev != file_info.rev or
                check.server_modified != file_info.server_modified.isoformat()):
                # Values changed, clear old checks and start over
                self.db.clear_stability_checks(path)
                self.db.add_stability_check(
                    dropbox_path=path,
                    size=file_info.size,
                    rev=file_info.rev,
                    server_modified=file_info.server_modified.isoformat(),
                    content_hash=file_info.content_hash,
                )
                logger.debug(f"Stability: values changed, resetting: {path}")
                return StabilityResult.CHANGED

        # All checks match, check minimum age
        oldest_check = recent_checks[-1] if recent_checks else None
        if oldest_check:
            age = (datetime.now(timezone.utc) - oldest_check.check_time).total_seconds()
            if age < min_age_sec:
                logger.debug(
                    f"Stability: waiting for age ({age:.0f}/{min_age_sec}s): {path}"
                )
                return StabilityResult.WAITING

        logger.debug(f"Stability: file is stable: {path}")
        return StabilityResult.STABLE

    def _create_new_job(self, file_info: DropboxFileInfo) -> None:
        """Create a new job for the file."""
        output_path = get_output_path(file_info.path)

        self.db.create_job(
            dropbox_path=file_info.path,
            dropbox_rev=file_info.rev,
            dropbox_size=file_info.size,
            output_path=output_path,
            state=JobState.NEW,
        )

        # Clear stability checks after job creation
        self.db.clear_stability_checks(file_info.path)

    def _create_skipped_job(
        self,
        file_info: DropboxFileInfo,
        state: JobState,
    ) -> None:
        """Create a job marked as skipped (for tracking)."""
        output_path = get_output_path(file_info.path)

        self.db.create_job(
            dropbox_path=file_info.path,
            dropbox_rev=file_info.rev,
            dropbox_size=file_info.size,
            output_path=output_path,
            state=state,
        )

    def check_file_stability(self, dropbox_path: str) -> tuple[str, DropboxFileInfo | None]:
        """
        Check stability of a specific file.

        Returns:
            Tuple of (stability_result, file_info or None if not found).
        """
        file_info = self.dropbox.get_metadata(dropbox_path)
        if not file_info:
            return (StabilityResult.CHANGED, None)

        stability = self._check_stability(file_info)
        return (stability, file_info)

    def verify_job_rev(self, job_id: int, dropbox_path: str, expected_rev: str) -> bool:
        """
        Verify that file revision hasn't changed.

        Used during download to detect changes.

        Returns:
            True if rev matches, False if changed.
        """
        file_info = self.dropbox.get_metadata(dropbox_path)
        if not file_info:
            logger.warning(f"File not found during rev check: {dropbox_path}")
            return False

        if file_info.rev != expected_rev:
            logger.warning(
                f"Rev changed during job {job_id}: {expected_rev} -> {file_info.rev}"
            )
            # Reset job to STABLE_WAIT
            self.db.update_job_state(
                job_id,
                JobState.STABLE_WAIT,
                error_message="File revision changed during processing",
                dropbox_rev=file_info.rev,
            )
            # Clear stability checks to restart
            self.db.clear_stability_checks(dropbox_path)
            return False

        return True
