"""
Dropbox folder scanner with file stability detection.

Phases:
- BULK: first pass over a brand-new archive. Enumerates the full tree via
  list_folder(root, recursive=True), checkpoints the cursor after every N
  entries so restarts resume. Uses the aggressive stability profile because
  the archive is effectively frozen.
- DELTA: once the bulk pass has completed, subsequent scans call
  list_folder_continue(cursor) and only see added/modified/deleted entries.
  Uses the conservative stability profile.

Implements R2 (stability), R4 (h265 exclusion), R5 (min size).
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from pathlib import PurePosixPath
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
    from .config import Config, StabilitySettings

logger = logging.getLogger(__name__)


class StabilityResult:
    """Result of stability check."""
    STABLE = "stable"
    WAITING = "waiting"
    CHANGED = "changed"


class Scanner:
    """
    Scans Dropbox folder for eligible video files in two modes (BULK, DELTA).

    Implements:
    - R2: File stability detection (two profiles: bulk aggressive / steady)
    - R4: Exclude h265 output folders
    - R5: Minimum file size filter
    """

    def __init__(
        self,
        config: Config,
        db: Database,
        dropbox_client: DropboxClient,
        stop_event: threading.Event | None = None,
    ):
        self.config = config
        self.db = db
        self.dropbox = dropbox_client
        # stop_event lets a long-running bulk pass bail cleanly on shutdown.
        self.stop_event = stop_event or threading.Event()
        # In-process memoisation of feito.txt reads within a single scan call,
        # backed by a persistent DB cache across scans.
        self._feito_memo: dict[str, set[str]] = {}

    # ------------------------------------------------------------------ public

    def scan(self, dry_run: bool = False) -> dict[str, int]:
        """
        Run one scan pass. In bulk mode this may walk the whole tree (with
        resumable cursor checkpoints); in delta mode it processes only changed
        entries.
        """
        stats = self._empty_stats()
        self._feito_memo.clear()

        state = self.db.get_scan_state(self.config.dropbox_root)
        logger.info(
            "scan: root=%s bulk_pass_complete=%s cursor=%s entries_seen=%d",
            state.dropbox_root,
            state.bulk_pass_complete,
            _cursor_preview(state.cursor),
            state.entries_seen,
        )

        try:
            if not state.bulk_pass_complete:
                stats['mode'] = 1  # 1 = bulk, 0 = delta (kept as int for table printing)
                self._run_bulk(state, stats, dry_run)
            else:
                stats['mode'] = 0
                self._run_delta(state, stats, dry_run)
        except _StopScan:
            logger.info("scan: stop_event signalled, exiting early")

        logger.info(
            "scan complete: scanned=%d new=%d waiting=%d skipped_h265_log=%d youtube=%d",
            stats['scanned'],
            stats['new'],
            stats['waiting_stable'],
            stats['skipped_h265_log'],
            stats['skipped_youtube'],
        )
        return stats

    def check_file_stability(
        self,
        dropbox_path: str,
    ) -> tuple[str, DropboxFileInfo | None]:
        """Check stability of a specific file (used by watchdog/doctor)."""
        file_info = self.dropbox.get_metadata(dropbox_path)
        if not file_info:
            return (StabilityResult.CHANGED, None)
        stability = self._check_stability(file_info, self._stability_settings())
        return (stability, file_info)

    def verify_job_rev(self, job_id: int, dropbox_path: str, expected_rev: str) -> bool:
        """Verify that file revision hasn't changed during a download."""
        file_info = self.dropbox.get_metadata(dropbox_path)
        if not file_info:
            logger.warning(f"File not found during rev check: {dropbox_path}")
            return False

        if file_info.rev != expected_rev:
            logger.warning(
                f"Rev changed during job {job_id}: {expected_rev} -> {file_info.rev}"
            )
            self.db.update_job_state(
                job_id,
                JobState.STABLE_WAIT,
                error_message="File revision changed during processing",
                dropbox_rev=file_info.rev,
            )
            self.db.clear_stability_checks(dropbox_path)
            return False

        return True

    # ---------------------------------------------------------------- BULK mode

    def _run_bulk(
        self,
        state,  # ScanState; avoid circular imports
        stats: dict[str, int],
        dry_run: bool,
    ) -> None:
        """First-pass enumeration of the configured Dropbox root."""
        self.db.mark_bulk_started()

        checkpoint_every = self.config.scanner.cursor_checkpoint_entries
        entries_seen = state.entries_seen
        entries_since_checkpoint = 0
        stability_cfg = self.config.stability_profiles.bulk

        if state.cursor:
            logger.info("bulk: resuming from saved cursor at %d entries", entries_seen)
            iterator = self.dropbox.list_folder_delta(state.cursor)
        else:
            iterator = self.dropbox.list_folder_entries(
                self.config.dropbox_root, recursive=True
            )

        for item in iterator:
            self._raise_if_stopped()

            if isinstance(item, str):
                # Page boundary: persist cursor so we can resume after crashes.
                cursor = item
                entries_since_checkpoint = 0
                self.db.save_scan_cursor(cursor, entries_seen)
                continue

            # Could be DropboxFileInfo (from list_folder_entries)
            # or ("file"|"deleted", payload) (from list_folder_delta when resuming).
            file_info = self._coerce_entry(item, stats)
            if file_info is None:
                continue

            entries_seen += 1
            entries_since_checkpoint += 1
            self._handle_file(file_info, stats, dry_run, stability_cfg)

            if entries_since_checkpoint >= checkpoint_every:
                # Cursor hasn't updated yet in mid-page iteration, but we bump
                # entries_seen so a crash+restart preserves progress visibility.
                self.db.save_scan_cursor(state.cursor or "", entries_seen)
                entries_since_checkpoint = 0

        # Finished the whole tree: flip the flag and persist.
        self.db.mark_bulk_complete()
        logger.info("bulk pass complete: entries_seen=%d", entries_seen)

    # --------------------------------------------------------------- DELTA mode

    def _run_delta(
        self,
        state,
        stats: dict[str, int],
        dry_run: bool,
    ) -> None:
        """Incremental scan: consume only changes since the saved cursor."""
        if not state.cursor:
            # Shouldn't happen (bulk_pass_complete without a cursor), but guard:
            logger.warning("delta: no cursor saved; falling back to bulk")
            self._run_bulk(state, stats, dry_run)
            return

        stability_cfg = self.config.stability_profiles.steady
        entries_seen = state.entries_seen

        for item in self.dropbox.list_folder_delta(state.cursor):
            self._raise_if_stopped()

            if isinstance(item, str):
                # Cursor persists after each delta page so we never re-deliver.
                self.db.save_scan_cursor(item, entries_seen)
                continue

            kind, payload = item
            if kind == "deleted":
                folder = str(PurePosixPath(payload).parent)
                self.db.invalidate_feito_cache(folder)
                continue

            file_info = payload  # type: ignore[assignment]
            if not isinstance(file_info, DropboxFileInfo):
                continue

            # A new/updated feito.txt invalidates the parent folder's cache
            if PurePosixPath(file_info.path).name.lower() == "h265 feito.txt":
                self.db.invalidate_feito_cache(str(PurePosixPath(file_info.path).parent))

            entries_seen += 1
            self._handle_file(file_info, stats, dry_run, stability_cfg)

        self.db.mark_delta_pass()

    # ---------------------------------------------------------------- per-file

    def _handle_file(
        self,
        file_info: DropboxFileInfo,
        stats: dict[str, int],
        dry_run: bool,
        stability_cfg: "StabilitySettings",
    ) -> None:
        stats['scanned'] += 1
        try:
            result = self._process_file(file_info, dry_run, stability_cfg)
            if result:
                stats[result] += 1
        except Exception as e:
            logger.error(f"Error processing {file_info.path}: {e}")
            stats['errors'] += 1

    def _process_file(
        self,
        file_info: DropboxFileInfo,
        dry_run: bool,
        stability_cfg: "StabilitySettings",
    ) -> str | None:
        path = file_info.path

        if not is_video_file(path, self.config.video_extensions):
            return None

        if is_in_h265_folder(path, mirror_root=self.config.output_mirror_root):
            logger.debug(f"Skipping (in h265 folder): {path}")
            return 'skipped_excluded'

        if is_partial_file(path):
            logger.debug(f"Skipping (partial file): {path}")
            return 'skipped_excluded'

        if matches_exclude_pattern(path, self.config.exclude_patterns):
            logger.debug(f"Skipping (excluded pattern): {path}")
            return 'skipped_excluded'

        if is_youtube_download(path):
            logger.debug(f"Skipping (YouTube download): {path}")
            return 'skipped_youtube'

        min_bytes = self.config.min_size_bytes()
        if file_info.size < min_bytes:
            if not dry_run:
                self._create_skipped_job(file_info, JobState.SKIPPED_TOO_SMALL)
            return 'skipped_small'

        if self._is_in_h265_feito_log(file_info):
            logger.debug(f"Skipping (in h265 feito.txt): {path}")
            return 'skipped_h265_log'

        output_path = self._output_path(path)
        if self.dropbox.file_exists(output_path):
            if not dry_run:
                self._create_skipped_job(file_info, JobState.SKIPPED_ALREADY_EXISTS)
            return 'skipped_exists'

        existing_job = self.db.get_job_by_path(path)
        if existing_job:
            if existing_job.dropbox_rev == file_info.rev:
                if existing_job.state not in {JobState.FAILED, JobState.RETRY_WAIT}:
                    return 'already_queued'
            else:
                logger.info(f"File modified since last job: {path}")

        stability = self._check_stability(file_info, stability_cfg)
        if stability == StabilityResult.STABLE:
            if not dry_run:
                self._create_new_job(file_info)
            logger.info(f"New job created: {path}")
            return 'new'
        return 'waiting_stable'

    # ------------------------------------------------------------- feito cache

    def _is_in_h265_feito_log(self, file_info: DropboxFileInfo) -> bool:
        """
        Return True if the file is already recorded in its folder's
        `h265 feito.txt`. Uses the in-process scan memo, the persistent DB
        cache (TTL-bounded), and falls back to reading from Dropbox.
        """
        log_path = get_h265_log_path(
            file_info.path,
            layout=self.config.output_layout.value,
            dropbox_root=self.config.dropbox_root,
            mirror_root=self.config.output_mirror_root,
        )
        filename = PurePosixPath(file_info.path).name

        # In-scan memo
        filenames = self._feito_memo.get(log_path)
        if filenames is not None:
            return filename in filenames

        # Persistent DB cache
        ttl = self.config.scanner.feito_cache_ttl_sec
        cached = self.db.get_feito_cache(log_path, ttl_sec=ttl)
        if cached is not None:
            self._feito_memo[log_path] = cached
            return filename in cached

        # Cold read from Dropbox
        log_content = self.dropbox.read_text_file(log_path)
        if log_content is None:
            empty: set[str] = set()
            self._feito_memo[log_path] = empty
            self.db.put_feito_cache(log_path, empty)
            return False

        filenames = set()
        for line in log_content.splitlines():
            parts = line.split('|')
            if len(parts) >= 2:
                filenames.add(parts[1].strip())

        self._feito_memo[log_path] = filenames
        self.db.put_feito_cache(log_path, filenames)
        return filename in filenames

    # ---------------------------------------------------------------- stability

    def _stability_settings(self) -> "StabilitySettings":
        """
        Default profile used by ad-hoc helpers (watchdog etc.): picks the
        steady-state profile once bulk is done, bulk otherwise.
        """
        state = self.db.get_scan_state(self.config.dropbox_root)
        if state.bulk_pass_complete:
            return self.config.stability_profiles.steady
        return self.config.stability_profiles.bulk

    def _check_stability(
        self,
        file_info: DropboxFileInfo,
        cfg: "StabilitySettings",
    ) -> str:
        """Stability gate using the supplied profile (bulk or steady)."""
        path = file_info.path
        checks_required = cfg.checks_required
        min_age_sec = cfg.min_age_sec

        # Fast path for bulk: one-check trust, no age requirement.
        if checks_required <= 1 and min_age_sec == 0:
            return StabilityResult.STABLE

        recent_checks = self.db.get_recent_stability_checks(path, limit=checks_required)

        self.db.add_stability_check(
            dropbox_path=path,
            size=file_info.size,
            rev=file_info.rev,
            server_modified=file_info.server_modified.isoformat(),
            content_hash=file_info.content_hash,
        )

        if len(recent_checks) < checks_required - 1:
            return StabilityResult.WAITING

        for check in recent_checks:
            if (check.size != file_info.size or
                check.rev != file_info.rev or
                check.server_modified != file_info.server_modified.isoformat()):
                self.db.clear_stability_checks(path)
                self.db.add_stability_check(
                    dropbox_path=path,
                    size=file_info.size,
                    rev=file_info.rev,
                    server_modified=file_info.server_modified.isoformat(),
                    content_hash=file_info.content_hash,
                )
                return StabilityResult.CHANGED

        oldest_check = recent_checks[-1] if recent_checks else None
        if oldest_check:
            age = (datetime.now(timezone.utc) - oldest_check.check_time).total_seconds()
            if age < min_age_sec:
                return StabilityResult.WAITING

        return StabilityResult.STABLE

    # ----------------------------------------------------------- job creation

    def _output_path(self, dropbox_path: str) -> str:
        return get_output_path(
            dropbox_path,
            layout=self.config.output_layout.value,
            dropbox_root=self.config.dropbox_root,
            mirror_root=self.config.output_mirror_root,
        )

    def _create_new_job(self, file_info: DropboxFileInfo) -> None:
        self.db.create_job(
            dropbox_path=file_info.path,
            dropbox_rev=file_info.rev,
            dropbox_size=file_info.size,
            output_path=self._output_path(file_info.path),
            state=JobState.NEW,
        )
        self.db.clear_stability_checks(file_info.path)

    def _create_skipped_job(
        self,
        file_info: DropboxFileInfo,
        state: JobState,
    ) -> None:
        self.db.create_job(
            dropbox_path=file_info.path,
            dropbox_rev=file_info.rev,
            dropbox_size=file_info.size,
            output_path=self._output_path(file_info.path),
            state=state,
        )

    # -------------------------------------------------------------- internals

    @staticmethod
    def _empty_stats() -> dict[str, int]:
        return {
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
            'mode': 0,
        }

    @staticmethod
    def _coerce_entry(item, stats: dict[str, int]) -> DropboxFileInfo | None:
        """Unwrap an iterator entry into a DropboxFileInfo, or None to skip."""
        if isinstance(item, DropboxFileInfo):
            return item
        if isinstance(item, tuple) and len(item) == 2:
            kind, payload = item
            if kind == "file" and isinstance(payload, DropboxFileInfo):
                return payload
        return None

    def _raise_if_stopped(self) -> None:
        if self.stop_event.is_set():
            raise _StopScan()


class _StopScan(Exception):
    """Signals the outer scan() to bail cleanly on shutdown."""


def _cursor_preview(cursor: str | None) -> str:
    if not cursor:
        return "<none>"
    return cursor[:16] + "..."
