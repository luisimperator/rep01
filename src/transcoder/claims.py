"""Cross-machine claim coordination over Dropbox.

Several machines can share ONE Dropbox pool and divide the work without two of
them processing the same video. Each machine keeps its own local DB, so the
only shared medium is Dropbox itself: before a machine commits to a file it
atomically creates a small claim file under a shared folder. The atomic arbiter
is `DropboxClient.claim_create` (add-mode upload, rename-on-conflict).

Lifecycle (decoupled from the multi-stage pipeline):
  * The DownloadWorker calls `try_claim()` before starting a file. If it loses,
    it skips the file (another machine owns it).
  * A background `ClaimReconciler` heartbeats the claims for every job still in
    flight on this machine and releases the claims of jobs that have finished.
  * A claim with no heartbeat for `claim_ttl_minutes` is treated as abandoned
    (the owner crashed / powered off) and can be stolen, so work is never lost.

Known limit (acceptable for v1): after an outage longer than the TTL, a machine
may still believe it holds a claim that was meanwhile stolen, so the same file
could be processed twice. The downstream output-exists / rev checks dedupe that
rare case; keep the TTL well above your largest job's download+transcode time.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Iterable

if TYPE_CHECKING:
    from .config import Config
    from .database import Database, JobState
    from .dropbox_client import DropboxClient

logger = logging.getLogger(__name__)


def _as_utc(dt: datetime) -> datetime:
    """Dropbox returns naive UTC timestamps; make them tz-aware."""
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


class ClaimStore:
    """Thread-safe Dropbox-backed claims. Tracks the keys this machine holds."""

    def __init__(
        self,
        client: "DropboxClient",
        *,
        folder: str,
        pc_name: str,
        ttl_minutes: int,
    ) -> None:
        self.client = client
        self.folder = "/" + folder.strip("/")
        self.pc_name = pc_name
        self.ttl = timedelta(minutes=ttl_minutes)
        self._held: set[str] = set()
        self._lock = threading.Lock()

    # ---- key / path / payload helpers --------------------------------------

    def _key(self, dropbox_path: str) -> str:
        return hashlib.sha1(dropbox_path.encode("utf-8")).hexdigest()

    def _claim_path(self, key: str) -> str:
        return f"{self.folder}/{key}.json"

    def _payload(self, dropbox_path: str) -> str:
        return json.dumps(
            {
                "pc": self.pc_name,
                "path": dropbox_path,
                "ts": datetime.now(timezone.utc).isoformat(),
            }
        )

    # ---- public API --------------------------------------------------------

    def seed_held(self, dropbox_paths: Iterable[str]) -> None:
        """Re-adopt claims for jobs already in flight (e.g. after a restart)."""
        with self._lock:
            for p in dropbox_paths:
                self._held.add(self._key(p))

    def try_claim(self, dropbox_path: str) -> bool:
        """Attempt to claim a file. True → this machine may process it."""
        key = self._key(dropbox_path)
        with self._lock:
            if key in self._held:
                return True

        claim_path = self._claim_path(key)
        if self.client.claim_create(claim_path, self._payload(dropbox_path)):
            with self._lock:
                self._held.add(key)
            return True

        # We lost the race — inspect the existing claim for staleness.
        meta = self.client.get_metadata(claim_path)
        if meta is None:
            return False  # vanished underneath us; retry on the next pass
        age = datetime.now(timezone.utc) - _as_utc(meta.server_modified)
        if age <= self.ttl:
            return False  # a live claim held by another machine

        # Abandoned claim → steal it.
        self.client.delete_file(claim_path)
        if self.client.claim_create(claim_path, self._payload(dropbox_path)):
            with self._lock:
                self._held.add(key)
            logger.info("claims: stole abandoned claim (age %s) for %s", age, dropbox_path)
            return True
        return False

    def heartbeat(self, dropbox_path: str) -> None:
        """Refresh the claim's timestamp so it isn't seen as abandoned."""
        key = self._key(dropbox_path)
        with self._lock:
            if key not in self._held:
                return
        self.client.write_text_file(self._claim_path(key), self._payload(dropbox_path))

    def release(self, dropbox_path: str) -> None:
        self._release_key(self._key(dropbox_path))

    def _release_key(self, key: str) -> None:
        try:
            self.client.delete_file(self._claim_path(key))
        finally:
            with self._lock:
                self._held.discard(key)

    def held_keys(self) -> set[str]:
        with self._lock:
            return set(self._held)


class ClaimReconciler(threading.Thread):
    """Heartbeats claims for in-flight jobs; releases them when jobs finish."""

    def __init__(
        self,
        store: ClaimStore,
        db: "Database",
        active_states: set["JobState"],
        stop_event: threading.Event,
        interval_sec: int,
    ) -> None:
        super().__init__(name="claim-reconciler", daemon=True)
        self.store = store
        self.db = db
        self.active_states = active_states
        self.stop_event = stop_event
        self.interval_sec = max(30, int(interval_sec))

    def _active_paths(self) -> list[str]:
        jobs = self.db.get_jobs_by_states(self.active_states, limit=100_000)
        return [j.dropbox_path for j in jobs]

    def reconcile_once(self) -> None:
        """One pass: heartbeat in-flight claims, release finished ones."""
        active = self._active_paths()
        # Adopt + heartbeat the claims for everything still in flight.
        self.store.seed_held(active)
        for path in active:
            self.store.heartbeat(path)
        # Release claims for jobs that have left the active set.
        active_keys = {self.store._key(p) for p in active}
        for key in self.store.held_keys() - active_keys:
            self.store._release_key(key)

    def run(self) -> None:
        logger.info(
            "claims: reconciler started (folder=%s, heartbeat every %ds)",
            self.store.folder, self.interval_sec,
        )
        while not self.stop_event.is_set():
            try:
                self.reconcile_once()
            except Exception:
                logger.warning("claims: reconcile pass failed", exc_info=True)
            if self.stop_event.wait(self.interval_sec):
                break
        logger.info("claims: reconciler stopped")
