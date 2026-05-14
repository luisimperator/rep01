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

        # Convoy mode: when transcode_q is empty AND ≥2 download workers are
        # actively pulling bytes, all 4 workers split the WAN four ways and
        # the transcoder waits ~45 min for any of them to finish. Convoy mode
        # picks one worker as "leader" (first one to ask) and tells the
        # others to sleep N seconds per chunk in the progress callback. Leader
        # gets effectively all the bandwidth and finishes ~4x faster — the
        # transcoder gets fed in ~10 min instead of 45.
        self._convoy_lock = threading.Lock()
        self._download_active: dict[str, int] = {}   # worker_name -> job_id
        self._convoy_leader: str | None = None
        self.convoy_throttle_sec: float = float(
            getattr(config.dispatcher, "convoy_throttle_sec", 5.0)
        )

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

    # -- convoy mode -----------------------------------------------------

    def register_download_active(self, worker_name: str, job_id: int) -> None:
        """A DownloadWorker is now actively pulling bytes from Dropbox."""
        with self._convoy_lock:
            self._download_active[worker_name] = job_id

    def unregister_download_active(self, worker_name: str) -> None:
        """A DownloadWorker is done (success, failure, or aborted)."""
        with self._convoy_lock:
            self._download_active.pop(worker_name, None)
            if self._convoy_leader == worker_name:
                self._convoy_leader = None

    def should_throttle_download(self, worker_name: str) -> bool:
        """True if this worker is a non-leader during active convoy mode.

        Called from the DownloadWorker progress callback after each chunk.
        Convoy mode activates when ALL of:
          - convoy_throttle_sec > 0
          - transcode_q is empty (no work waiting for transcoder)
          - ≥2 download workers are actively pulling bytes

        First worker to ask becomes leader and runs at full speed; the rest
        sleep `convoy_throttle_sec` per chunk so the leader's file finishes
        first and feeds the transcoder. As soon as transcode_q has work,
        convoy clears and everyone runs full-speed.
        """
        if self.convoy_throttle_sec <= 0:
            return False

        # Once the transcoder has work, drop convoy state and let everyone
        # rip — leaders included. Cheap fast path that runs every chunk.
        if self.transcode_q.qsize() > 0:
            if self._convoy_leader is not None:
                with self._convoy_lock:
                    if self._convoy_leader is not None:
                        logger.info(
                            "dispatcher: convoy mode cleared — transcoder fed"
                        )
                        self._convoy_leader = None
            return False

        with self._convoy_lock:
            # Leader must still be a registered active worker; if it
            # finished/failed without unregistering for any reason, drop it
            # so a fresh leader can be elected on the next callback.
            if (
                self._convoy_leader is not None
                and self._convoy_leader not in self._download_active
            ):
                self._convoy_leader = None

            # Need at least two active downloaders for convoy to make sense.
            # With one downloader there's nothing to throttle.
            if len(self._download_active) < 2:
                self._convoy_leader = None
                return False

            # Elect: first asker wins. The user explicitly said "doesn't
            # matter who, just pick any one."
            if self._convoy_leader is None:
                self._convoy_leader = worker_name
                logger.info(
                    "dispatcher: convoy mode engaged — transcode_q empty, %d "
                    "downloaders active; leader=%s, others throttled to "
                    "%.1fs/chunk to free WAN for the leader",
                    len(self._download_active),
                    worker_name,
                    self.convoy_throttle_sec,
                )

            return self._convoy_leader != worker_name

    # ---------------------------------------------------------------- queues

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
        # Restrict to the current watch_folder so stale jobs queued under a
        # previous dropbox_root don't tie up worker slots after the user
        # switches folders.
        watch_root = getattr(self.config, "dropbox_root", None) or None
        candidates = self.db.get_dispatchable_jobs(
            states, limit=fetch_limit, path_prefix=watch_root,
        )
        if not candidates:
            return

        if prioritize_folder:
            candidates = self._sort_by_folder_priority(candidates)
            # Anti-starvation: when the transcode queue is empty, push the
            # first reasonably-small job to the front so the transcoder
            # gets fed quickly instead of waiting hours for one of N huge
            # downloads to finish. Folder priority still applies — we only
            # reorder within the already-prioritised list.
            if self.transcode_q.qsize() == 0:
                candidates = self._anti_starvation_reorder(candidates)

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

    # Threshold below which a job counts as "small enough to feed the
    # transcoder fast". 20 GB is roughly a 4K 4:2:0 source — downloads
    # in ~5–10 min on a decent uplink, so the transcoder gets work soon.
    _ANTI_STARVATION_SMALL_BYTES = 20 * 1024 ** 3

    def _anti_starvation_reorder(self, jobs: list[Job]) -> list[Job]:
        """Move the first sub-threshold job to the head of the list.

        Called only when the transcode queue is empty. Picks the first
        "small" job in the (already folder-prioritised) candidate list
        and lifts it to position 0, leaving the rest of the order
        intact. This guarantees that the next download admission picks
        something the transcoder can chew on within minutes, even when
        all four download workers would otherwise grab 70+ GB monsters
        from the priority folder.

        If no small job exists in the candidate window, returns the
        list unchanged (folder priority wins by default).
        """
        if not jobs:
            return jobs
        # If the head of the list is already small, no reorder needed.
        first_size = int(jobs[0].dropbox_size or 0)
        if 0 < first_size < self._ANTI_STARVATION_SMALL_BYTES:
            return jobs
        # Otherwise, scan for the first small job further down and lift
        # it to position 0.
        for i in range(1, len(jobs)):
            size = int(jobs[i].dropbox_size or 0)
            if 0 < size < self._ANTI_STARVATION_SMALL_BYTES:
                logger.info(
                    "dispatcher: transcode queue empty — promoting small job "
                    "%s (%.2f GB) ahead of folder priority to feed transcoder",
                    jobs[i].id, size / (1024 ** 3),
                )
                small = jobs.pop(i)
                jobs.insert(0, small)
                return jobs
        return jobs

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
