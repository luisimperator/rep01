"""
Central job dispatcher.

A single thread reads candidate jobs from the database every few seconds and
fills three bounded queues — one per pipeline stage. Workers consume from these
queues instead of polling the DB themselves, which gives:

- One DB reader instead of N pollers (less SQLite contention)
- Bounded look-ahead per stage (ready jobs sit in memory, not in the DB churn)
- A single source of truth (`active_set`) for "which jobs are already in flight"
  so nothing gets dispatched twice.
"""

from __future__ import annotations

import logging
import threading
import time
from pathlib import PurePosixPath
from queue import Empty, Full, Queue
from typing import TYPE_CHECKING

from .database import Database, Job, JobState

if TYPE_CHECKING:
    from .config import Config

logger = logging.getLogger(__name__)


# Which DB states feed which worker queue.
DOWNLOAD_STATES = {JobState.NEW, JobState.RETRY_WAIT}
TRANSCODE_STATES = {JobState.DOWNLOADED}
UPLOAD_STATES = {JobState.UPLOADING}


class JobDispatcher(threading.Thread):
    """Owns the in-memory job queues for the three pipeline stages."""

    def __init__(
        self,
        config: Config,
        db: Database,
        stop_event: threading.Event,
    ) -> None:
        super().__init__(name="dispatcher", daemon=True)
        self.config = config
        self.db = db
        self.stop_event = stop_event

        mult = config.dispatcher.queue_multiplier
        self.download_q: Queue[Job] = Queue(
            maxsize=max(1, config.concurrency.download_workers) * mult
        )
        self.transcode_q: Queue[Job] = Queue(
            maxsize=max(1, config.concurrency.transcode_workers) * mult
        )
        self.audio_transcode_q: Queue[Job] = Queue(
            maxsize=max(1, config.concurrency.audio_workers) * mult
        )
        self.upload_q: Queue[Job] = Queue(
            maxsize=max(1, config.concurrency.upload_workers) * mult
        )

        self._active_lock = threading.Lock()
        self._active_set: set[int] = set()

        self.poll_interval = config.dispatcher.poll_interval_sec

        # Pause flag: when set, the dispatcher keeps running but stops refilling
        # queues. Workers drain whatever is already in flight and then idle.
        self._paused = threading.Event()

        # Per-folder pending bytes, refreshed periodically from folder_census.
        # Used to bias the download queue toward whichever folder still has
        # the largest backlog of H.264 to chew through (folder-drain priority).
        # Empty until the first census run completes.
        self._folder_priority: dict[str, int] = {}
        self._folder_priority_at: float = 0.0
        self._folder_priority_ttl_sec: float = 60.0

    # ------------------------------------------------------------------ public

    def mark_done(self, job_id: int) -> None:
        """Worker calls this in its `finally:` block to release the slot."""
        with self._active_lock:
            self._active_set.discard(job_id)

    def pause(self) -> None:
        """Stop enqueuing new jobs. Workers drain what's already in queues."""
        self._paused.set()

    def resume(self) -> None:
        """Resume enqueueing new jobs."""
        self._paused.clear()

    def is_paused(self) -> bool:
        return self._paused.is_set()

    def queue_for_stage(self, stage: str) -> Queue[Job]:
        """Look up a worker queue by stage name."""
        if stage == "download":
            return self.download_q
        if stage == "transcode":
            return self.transcode_q
        if stage == "audio_transcode":
            return self.audio_transcode_q
        if stage == "upload":
            return self.upload_q
        raise ValueError(f"unknown stage: {stage}")

    def queue_depths(self) -> dict[str, int]:
        """For diagnostics: current depth of each queue."""
        return {
            "download": self.download_q.qsize(),
            "transcode": self.transcode_q.qsize(),
            "audio_transcode": self.audio_transcode_q.qsize(),
            "upload": self.upload_q.qsize(),
            "active": len(self._active_set),
        }

    # ------------------------------------------------------------------ thread

    def run(self) -> None:
        logger.info(
            "dispatcher started: queues=download(%d)/transcode(%d)/audio(%d)/upload(%d), poll=%.1fs",
            self.download_q.maxsize,
            self.transcode_q.maxsize,
            self.audio_transcode_q.maxsize,
            self.upload_q.maxsize,
            self.poll_interval,
        )
        while not self.stop_event.is_set():
            try:
                if not self._paused.is_set():
                    # Download queue is the entry point — that's where folder
                    # priority matters. Once a job is past download, the
                    # downstream stages process whatever lands their way.
                    self._refill(self.download_q, DOWNLOAD_STATES, prioritize_folder=True)
                    # Video jobs only — audio jobs in DOWNLOADED state route
                    # to audio_transcode_q below.
                    self._refill(self.transcode_q, TRANSCODE_STATES, kind="video")
                    self._refill(self.audio_transcode_q, TRANSCODE_STATES, kind="audio")
                    self._refill(self.upload_q, UPLOAD_STATES)
            except Exception:
                logger.exception("dispatcher refill error")

            # Wake up early on stop
            if self.stop_event.wait(self.poll_interval):
                break

        logger.info("dispatcher stopped")

    # ----------------------------------------------------------------- private

    def _refill(
        self,
        q: Queue[Job],
        states: set[JobState],
        kind: str | None = None,
        prioritize_folder: bool = False,
    ) -> None:
        free = q.maxsize - q.qsize()
        if free <= 0:
            return

        # Over-fetch: dispatchable jobs may already be in flight (active_set).
        # If a kind filter is set we over-fetch more aggressively because the
        # candidate list may contain mostly the wrong kind for this queue.
        # When folder-prioritizing, fetch a wider net so we have room to
        # reorder before truncating to `free`.
        if prioritize_folder:
            fetch_limit = max(free * 8, 500)
        else:
            fetch_limit = free * (4 if kind else 2)
        candidates = self.db.get_dispatchable_jobs(states, limit=fetch_limit)
        if not candidates:
            return

        if prioritize_folder:
            candidates = self._sort_by_folder_priority(candidates)

        with self._active_lock:
            for job in candidates:
                if free <= 0:
                    break
                if kind is not None and job.kind != kind:
                    continue
                if job.id in self._active_set:
                    continue
                try:
                    q.put_nowait(job)
                except Full:
                    break
                self._active_set.add(job.id)
                free -= 1

    def _sort_by_folder_priority(self, jobs: list[Job]) -> list[Job]:
        """Stable sort: largest folder backlog first, then FIFO by created_at.

        Folders with no census data (or zero pending bytes) sink to the end.
        This is best-effort — once a job leaves NEW state the downstream
        stages don't re-prioritize.
        """
        priority = self._get_folder_priority()
        if not priority:
            return jobs  # no census yet → fall back to created_at order

        def key(job: Job) -> tuple:
            parent = str(PurePosixPath(job.dropbox_path).parent)
            # Negate for descending; jobs with no folder-priority entry
            # get 0 → sorted last but still in created_at order among
            # themselves.
            return (-priority.get(parent, 0), job.created_at)

        return sorted(jobs, key=key)

    def _get_folder_priority(self) -> dict[str, int]:
        """Cache folder_census reads — refresh once a minute.

        Census runs at most once a day; refreshing more often than every
        minute is wasted SQLite work. The dispatcher polls every 2s by
        default so even a 60s TTL is well within the cadence.
        """
        now = time.monotonic()
        if (now - self._folder_priority_at) > self._folder_priority_ttl_sec:
            try:
                self._folder_priority = self.db.get_folder_pending_bytes_map()
            except Exception:
                logger.exception("dispatcher: folder priority refresh failed")
                self._folder_priority = {}
            self._folder_priority_at = now
        return self._folder_priority
