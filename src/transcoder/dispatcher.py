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
        self.upload_q: Queue[Job] = Queue(
            maxsize=max(1, config.concurrency.upload_workers) * mult
        )

        self._active_lock = threading.Lock()
        self._active_set: set[int] = set()

        self.poll_interval = config.dispatcher.poll_interval_sec

    # ------------------------------------------------------------------ public

    def mark_done(self, job_id: int) -> None:
        """Worker calls this in its `finally:` block to release the slot."""
        with self._active_lock:
            self._active_set.discard(job_id)

    def queue_for_stage(self, stage: str) -> Queue[Job]:
        """Look up a worker queue by stage name."""
        if stage == "download":
            return self.download_q
        if stage == "transcode":
            return self.transcode_q
        if stage == "upload":
            return self.upload_q
        raise ValueError(f"unknown stage: {stage}")

    def queue_depths(self) -> dict[str, int]:
        """For diagnostics: current depth of each queue."""
        return {
            "download": self.download_q.qsize(),
            "transcode": self.transcode_q.qsize(),
            "upload": self.upload_q.qsize(),
            "active": len(self._active_set),
        }

    # ------------------------------------------------------------------ thread

    def run(self) -> None:
        logger.info(
            "dispatcher started: queues=download(%d)/transcode(%d)/upload(%d), poll=%.1fs",
            self.download_q.maxsize,
            self.transcode_q.maxsize,
            self.upload_q.maxsize,
            self.poll_interval,
        )
        while not self.stop_event.is_set():
            try:
                self._refill(self.download_q, DOWNLOAD_STATES)
                self._refill(self.transcode_q, TRANSCODE_STATES)
                self._refill(self.upload_q, UPLOAD_STATES)
            except Exception:
                logger.exception("dispatcher refill error")

            # Wake up early on stop
            if self.stop_event.wait(self.poll_interval):
                break

        logger.info("dispatcher stopped")

    # ----------------------------------------------------------------- private

    def _refill(self, q: Queue[Job], states: set[JobState]) -> None:
        free = q.maxsize - q.qsize()
        if free <= 0:
            return

        # Over-fetch: dispatchable jobs may already be in flight (active_set).
        candidates = self.db.get_dispatchable_jobs(states, limit=free * 2)
        if not candidates:
            return

        with self._active_lock:
            for job in candidates:
                if free <= 0:
                    break
                if job.id in self._active_set:
                    continue
                try:
                    q.put_nowait(job)
                except Full:
                    break
                self._active_set.add(job.id)
                free -= 1
