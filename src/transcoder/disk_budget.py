"""
Staging-disk budget for the download pipeline.

When the daemon points at a 200TB Dropbox but has only a few TB of local disk,
uncontrolled parallel downloads fill the staging directory in minutes and
every worker starts failing with ENOSPC. The DiskBudget class hands out
reservations: a download worker must acquire one before it fetches, and
releases it once the job has been uploaded and the staging files deleted.

Disabled by default; flip `config.disk_budget.enabled` to turn it on.
"""

from __future__ import annotations

import logging
import shutil
import threading
import time
from pathlib import Path

from .database import Database

logger = logging.getLogger(__name__)


class DiskBudget:
    """Thread-safe admission control over `local_staging_dir`."""

    def __init__(
        self,
        staging_dir: Path,
        db: Database,
        max_staging_bytes: int,
        min_free_bytes: int,
        poll_interval_sec: int = 30,
        enabled: bool = True,
    ) -> None:
        self.staging_dir = Path(staging_dir)
        self.db = db
        self.max_staging_bytes = int(max_staging_bytes)
        self.min_free_bytes = int(min_free_bytes)
        self.poll_interval_sec = max(1, int(poll_interval_sec))
        self.enabled = bool(enabled)
        # One Python-level lock in front of the SQL read+write; SQLite's own
        # transactions would also work but the lock makes the admit-then-reserve
        # sequence atomic across multiple threads in this process.
        self._lock = threading.Lock()

    # --------------------------------------------------------------- admission

    def _free_on_staging(self) -> int:
        try:
            return shutil.disk_usage(self.staging_dir).free
        except (FileNotFoundError, OSError):
            # Staging dir may not exist yet at startup; treat as abundant.
            return max(self.max_staging_bytes, self.min_free_bytes) * 2

    def can_admit(self, size_bytes: int) -> tuple[bool, str | None]:
        """
        Check whether `size_bytes` can be safely reserved right now.

        Returns (True, None) when admission is allowed, or (False, reason)
        when the caller must wait.
        """
        if not self.enabled:
            return True, None
        reserved = self.db.total_reserved_bytes()
        if reserved + size_bytes > self.max_staging_bytes:
            return False, (
                f"staging budget exhausted: reserved={reserved}B, "
                f"incoming={size_bytes}B, cap={self.max_staging_bytes}B"
            )
        free_after = self._free_on_staging() - size_bytes
        if free_after < self.min_free_bytes:
            return False, (
                f"insufficient free space: after={free_after}B < "
                f"min_free={self.min_free_bytes}B"
            )
        return True, None

    def try_reserve(self, job_id: int, size_bytes: int) -> tuple[bool, str | None]:
        """Atomic admit + reserve. Returns (granted, reason_if_denied)."""
        if not self.enabled:
            return True, None
        with self._lock:
            ok, reason = self.can_admit(size_bytes)
            if not ok:
                return False, reason
            self.db.reserve_disk(job_id, size_bytes)
            return True, None

    def release(self, job_id: int) -> None:
        if not self.enabled:
            return
        try:
            self.db.release_disk(job_id)
        except Exception:
            logger.exception("release_disk(%s) failed", job_id)

    def wait_for_slot(
        self,
        job_id: int,
        size_bytes: int,
        stop_event: threading.Event,
    ) -> bool:
        """
        Block until `size_bytes` can be reserved for this job_id.

        Logs the first stall and then every 5 minutes while the stall lasts,
        to keep operator logs readable for a daemon that might wait hours
        for staging to drain.

        Returns True when the reservation was granted, or False if stop_event
        was signalled while waiting.
        """
        if not self.enabled:
            return True

        first_stall = True
        last_log = 0.0

        while not stop_event.is_set():
            granted, reason = self.try_reserve(job_id, size_bytes)
            if granted:
                if not first_stall:
                    logger.info(
                        "disk_budget: admitted job %s (%.2f GB) after waiting",
                        job_id,
                        size_bytes / (1024 ** 3),
                    )
                return True

            now = time.monotonic()
            if first_stall or (now - last_log) > 300.0:
                logger.info(
                    "disk_budget: job %s (%.2f GB) stalled — %s",
                    job_id,
                    size_bytes / (1024 ** 3),
                    reason,
                )
                last_log = now
                first_stall = False

            if stop_event.wait(self.poll_interval_sec):
                return False

        return False
