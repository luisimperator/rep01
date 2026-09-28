"""
Daily sweep of projects that went cold (v8.5.0).

Several things are deferred while an edit is still "hot" and, before this
sweep, never came back on their own:

  * The post-upload swap (H.265 into the original's spot, H.264 backed up to
    h264/) re-fires only on the NEXT upload in that folder — a folder whose
    last upload happened while hot stayed unswapped until someone ran
    `hd reorganize-existing` by hand. Fast-lane (proxy_only) folders never
    swap at upload at all.
  * Proxies/ trees and Premiere preview caches are deleted by the scanner
    only when Dropbox delivers one of their files — in delta mode that is
    only when something CHANGES, so a Proxies/ seen hot once was never
    revisited.

Once a day this sweep lists the tree ONCE and, per folder whose project is
settled (reorganize.is_folder_settled with legacy_reorganize_min_age_days):
  a) swaps every pending pair (video h264/h265 + audio wav/mp3);
  b) deletes Proxies/ roots and Premiere Video/Audio Previews roots
     (only with scanner.delete_throwaway_files);
  c) runs the ._ quarantine over the same listing.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Callable

from .database import TERMINAL_STATES, JobState
from .reorganize import (
    AUDIO_LAYOUT,
    VIDEO_LAYOUT,
    _audio_successor_name,
    _video_successor_name,
    find_unreorganized_pairs_in_entries,
    index_entries_by_parent,
    is_folder_settled,
    reorganize_pair,
    schedule_h264_delete,
    sweep_dot_underscore_in_entries,
)
from .utils import (
    path_has_assets_segment,
    premiere_preview_root,
    proxies_folder_root,
)

if TYPE_CHECKING:
    from .config import Config
    from .database import Database
    from .dispatcher import JobDispatcher
    from .dropbox_client import DropboxClient, DropboxFileInfo

logger = logging.getLogger(__name__)

# Per-list cap on what the dashboard keeps (counts stay exact).
_MAX_LISTED = 200


@dataclass
class ColdSweepResult:
    trigger: str
    started_at: float
    finished_at: float | None = None
    status: str = "running"            # running | done | aborted | error
    abort_reason: str | None = None
    error: str | None = None
    threshold_days: int = 0
    entries_listed: int = 0
    swapped_pairs: int = 0
    failed_pairs: int = 0
    swapped_folders: list[str] = field(default_factory=list)
    deleted_count: int = 0
    deleted: list[dict] = field(default_factory=list)
    deferred_count: int = 0
    deferred: list[dict] = field(default_factory=list)
    dot_underscore_quarantined: int = 0

    def add_deferred(self, path: str, what: str, reason: str) -> None:
        self.deferred_count += 1
        if len(self.deferred) < _MAX_LISTED:
            self.deferred.append({"path": path, "what": what, "reason": reason})

    def add_deleted(self, path: str, kind: str) -> None:
        self.deleted_count += 1
        if len(self.deleted) < _MAX_LISTED:
            self.deleted.append({"path": path, "kind": kind})

    def to_dict(self) -> dict:
        return asdict(self)


def _real_date(entry: "DropboxFileInfo") -> datetime | None:
    dt = entry.client_modified or entry.server_modified
    if dt is None:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _days_str(days: float | None) -> str:
    return f"{days:.1f}d" if days is not None else "?d"


def _throwaway_roots(
    entries: list["DropboxFileInfo"],
) -> dict[str, tuple[str, datetime | None]]:
    """{root: (kind, newest real date of any file under it)}.

    Proxy roots win over preview roots (a preview cache inside a Proxies
    tree goes with the tree). /assets/ is never touched, same as the
    scanner.
    """
    roots: dict[str, tuple[str, datetime | None]] = {}
    for e in entries:
        if path_has_assets_segment(e.path):
            continue
        root = proxies_folder_root(e.path)
        kind = "camera/NLE proxy"
        if root is None:
            root = premiere_preview_root(e.path)
            kind = "Premiere preview"
        if root is None:
            continue
        prev_kind, prev_newest = roots.get(root, (kind, None))
        when = _real_date(e)
        newest = prev_newest if when is None or (prev_newest and prev_newest >= when) else when
        roots[root] = (prev_kind, newest)
    return roots


def _folder_busy(db: "Database", parent: str) -> str | None:
    """Reason string when a job in `parent` is still in flight, else None."""
    try:
        jobs = db.get_jobs_in_folder(parent)
    except Exception:
        return None
    busy = [j for j in jobs
            if j.state not in TERMINAL_STATES and j.state != JobState.FAILED]
    if busy:
        return f"{len(busy)} job(s) still in flight"
    return None


def run_cold_sweep(
    config: "Config",
    db: "Database",
    dropbox: "DropboxClient",
    *,
    should_continue: Callable[[], bool] = lambda: True,
    trigger: str = "manual",
    entries: list["DropboxFileInfo"] | None = None,
    result: ColdSweepResult | None = None,
) -> ColdSweepResult:
    """One pass over the tree. Never raises; failures land in the result."""
    min_age = int(getattr(config, "legacy_reorganize_min_age_days", 0) or 0)
    res = result or ColdSweepResult(trigger=trigger, started_at=time.time())
    res.threshold_days = min_age
    root = config.dropbox_root
    settled_cache: dict = {}

    def stop_requested() -> bool:
        if should_continue():
            return False
        res.status = "aborted"
        res.abort_reason = res.abort_reason or "pipeline paused or shutting down"
        return True

    try:
        if entries is None:
            logger.info(f"cold sweep: listing {root} ({trigger})")
            entries = list(dropbox.list_folder(root, recursive=True))
        res.entries_listed = len(entries)
        if stop_requested():
            return res

        # c) ._ forks — same rules as the periodic sweep, same listing.
        if config.cleanup_dot_underscore:
            try:
                dotu = sweep_dot_underscore_in_entries(
                    dropbox, entries,
                    config.cleanup_dot_underscore_delete_after_seconds,
                    config.dot_underscore_target_folder_names,
                    max_size_bytes=config.dot_underscore_max_size_bytes,
                )
                res.dot_underscore_quarantined = sum(dotu.values())
            except Exception as e:
                logger.warning(f"cold sweep: ._ pass failed: {e}")

        # a) deferred swaps, both layouts, grouped per folder.
        by_parent = index_entries_by_parent(entries)
        grouped: dict[str, dict] = {}
        for layout, key in ((VIDEO_LAYOUT, "video"), (AUDIO_LAYOUT, "audio")):
            for cand in find_unreorganized_pairs_in_entries(by_parent, layout):
                # Nothing inside Proxies/ (Conformer's "H265 Prod" included)
                # or /assets/ is ever swapped.
                probe = cand.parent.rstrip("/") + "/x"
                if proxies_folder_root(probe) or path_has_assets_segment(probe):
                    continue
                grouped.setdefault(cand.parent, {"video": [], "audio": []})[key] = cand.pairs

        for parent in sorted(grouped):
            if stop_requested():
                return res
            slots = grouped[parent]
            reason = _swap_block_reason(config, db, dropbox, parent, min_age, settled_cache)
            if reason is not None:
                res.add_deferred(parent, "swap", reason)
                continue
            _swap_folder(config, db, dropbox, parent, slots, res)

        # b) throwaway trees: Proxies/ + Premiere Video/Audio Previews.
        if getattr(config.scanner, "delete_throwaway_files", False):
            for troot, (kind, newest) in sorted(_throwaway_roots(entries).items()):
                if stop_requested():
                    return res
                _maybe_delete_throwaway(
                    config, dropbox, troot, kind, newest, min_age, settled_cache, res,
                )

        res.status = "done"
    except Exception as e:
        logger.exception("cold sweep crashed")
        res.status = "error"
        res.error = str(e)
    finally:
        res.finished_at = time.time()
        logger.info(
            "cold sweep %s (%s): %d pair(s) swapped in %d folder(s), %d failed; "
            "%d throwaway folder(s) deleted; %d deferred; %d ._ quarantined",
            res.status, res.trigger, res.swapped_pairs, len(res.swapped_folders),
            res.failed_pairs, res.deleted_count, res.deferred_count,
            res.dot_underscore_quarantined,
        )
    return res


def _swap_block_reason(config, db, dropbox, parent, min_age, cache) -> str | None:
    """Why `parent` must not be swapped now, or None when it's good to go."""
    busy = _folder_busy(db, parent)
    if busy:
        return busy
    # A fast-lane folder's H.265 is the editors' live proxy: never swap it
    # without a real age threshold, even if someone set 0 ("immediately").
    prio = getattr(config, "priority", None)
    fast_lane = prio is not None and prio.proxy_only and config.is_priority_path(parent + "/x")
    if fast_lane and min_age <= 0:
        return "fast-lane proxy folder and legacy_reorganize_min_age_days is 0"
    try:
        activity = is_folder_settled(
            dropbox, parent, min_age, dropbox_root=config.dropbox_root, cache=cache,
        )
    except Exception as e:
        return f"settled check failed: {e}"
    if not activity.settled:
        what = ".prproj saved" if activity.source == "prproj" else "footage"
        return (f"{what} {_days_str(activity.days_since_newest)} ago "
                f"(< {activity.threshold_days}d — project still active)")
    return None


def _swap_folder(config, db, dropbox, parent, slots, res: ColdSweepResult) -> None:
    v_pairs, a_pairs = slots["video"], slots["audio"]
    done = {"video": 0, "audio": 0}
    for layout, pairs, label in (
        (VIDEO_LAYOUT, v_pairs, "video"),
        (AUDIO_LAYOUT, a_pairs, "audio"),
    ):
        for pair in pairs:
            try:
                new_path = reorganize_pair(
                    dropbox, parent, pair.name,
                    int(pair.original.size or 0), int(pair.h265.size or 0),
                    layout=layout,
                )
            except Exception as e:
                res.failed_pairs += 1
                logger.warning(f"cold sweep: swap failed for {parent}/{pair.name}: {e}")
                continue
            done[label] += 1
            res.swapped_pairs += 1
            # Keep the DB's output_path in line with the new canonical spot.
            try:
                original_path = (parent.rstrip('/') + '/' + pair.name) if parent else '/' + pair.name
                job = db.get_job_by_path(original_path)
                if job is not None:
                    db.update_job_state(job.id, JobState.DONE, output_path=new_path)
            except Exception:
                pass

    if done["video"] or done["audio"]:
        if len(res.swapped_folders) < _MAX_LISTED:
            res.swapped_folders.append(parent)
    # Backup cleanup only when the WHOLE layout batch in this folder landed
    # (same safety as the live pipeline). Delays unchanged.
    base = parent.rstrip('/') if parent else ''
    if v_pairs and done["video"] == len(v_pairs):
        delay = config.legacy_reorganize_delete_h264_after_seconds
        if delay > 0:
            schedule_h264_delete(dropbox, base + '/h264', delay,
                                 successor_resolver=_video_successor_name)
    if a_pairs and done["audio"] == len(a_pairs):
        delay = config.legacy_reorganize_delete_wav_after_seconds
        if delay > 0:
            schedule_h264_delete(dropbox, base + '/wav', delay,
                                 successor_resolver=_audio_successor_name)


def _maybe_delete_throwaway(config, dropbox, root, kind, newest, min_age, cache, res) -> None:
    try:
        activity = is_folder_settled(
            dropbox, root, min_age, dropbox_root=config.dropbox_root, cache=cache,
        )
    except Exception as e:
        res.add_deferred(root, kind, f"folder-age check failed: {e}")
        return
    settled, days = activity.settled, activity.days_since_newest
    # A Proxies/ or preview root often holds only subfolders (Premiere nests
    # by resolution / .PRV), so "no files directly here" must not read as
    # "nothing to protect": fall back to its newest file anywhere below.
    if activity.source == "empty" and min_age > 0 and newest is not None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        days = (now - newest).total_seconds() / 86400.0
        settled = newest < now - timedelta(days=min_age)
    if not settled:
        logger.debug(
            f"cold sweep: skipping (throwaway {kind}, folder still hot — newest "
            f"activity {_days_str(days)} ago, threshold {min_age}d): {root}"
        )
        res.add_deferred(root, kind, f"still hot ({_days_str(days)} < {min_age}d)")
        return
    try:
        deleted = dropbox.delete_file(root)
    except Exception as e:
        logger.warning(f"cold sweep: delete failed for {root}: {e}")
        res.add_deferred(root, kind, f"delete failed: {e}")
        return
    if deleted:
        logger.info(
            f"Deleted throwaway {kind} from Dropbox: {root} "
            f"(recoverable via version history for ~30 days)"
        )
        res.add_deleted(root, kind)


# ---------------------------------------------------------------- scheduler


class ColdSweepWorker(threading.Thread):
    """Runs run_cold_sweep once a day at cold_sweep.daily_run_at (+ on demand)."""

    def __init__(
        self,
        config: "Config",
        db: "Database",
        dropbox: "DropboxClient",
        stop_event: threading.Event,
        dispatcher: "JobDispatcher | None" = None,
        state_path: Path | None = None,
    ) -> None:
        super().__init__(name="cold-sweep", daemon=True)
        self.config = config
        self.db = db
        self.dropbox = dropbox
        self.stop_event = stop_event
        self.dispatcher = dispatcher
        self.state_path = state_path or Path(config.database_path).with_name(
            "cold_sweep_last.json"
        )
        self._wake = threading.Event()
        self._lock = threading.Lock()
        self._current: ColdSweepResult | None = None
        self._last: dict | None = None
        self._last_scheduled_date: str | None = None
        self._load_state()

    # --------------- dashboard API ---------------------------------------

    def is_paused(self) -> bool:
        d = self.dispatcher
        return bool(d is not None and d.is_paused())

    def trigger_now(self) -> tuple[bool, str]:
        if self.is_paused():
            return False, "pipeline is paused — resume it to run the sweep"
        with self._lock:
            if self._current is not None:
                return False, "a cold sweep is already running"
        self._wake.set()
        return True, ""

    def status(self) -> dict:
        with self._lock:
            running = self._current.to_dict() if self._current else None
            return {
                "enabled": self.config.cold_sweep.enabled,
                "daily_run_at": self.config.cold_sweep.daily_run_at,
                "running": running,
                "last": self._last,
                "last_scheduled_date": self._last_scheduled_date,
            }

    # --------------- thread loop ------------------------------------------

    def run(self) -> None:
        logger.info(
            "cold sweep: scheduled daily at %s (catch-up %.0fh); "
            "use /api/cold-sweep/run to run now",
            self.config.cold_sweep.daily_run_at, self.config.cold_sweep.catch_up_hours,
        )
        while not self.stop_event.is_set():
            manual = self._wake.wait(timeout=30.0)
            if self.stop_event.is_set():
                break
            if manual:
                self._wake.clear()
                self._run("manual")
                continue
            if self._due(datetime.now()) and not self.is_paused():
                self._run("scheduled")

    def _due(self, now: datetime) -> bool:
        cfg = self.config.cold_sweep
        hh, mm = (int(x) for x in cfg.daily_run_at.split(":"))
        target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if now < target:
            # Before today's slot: yesterday's catch-up window may still be open.
            target -= timedelta(days=1)
        if self._last_scheduled_date == target.date().isoformat():
            return False
        return now <= target + timedelta(hours=cfg.catch_up_hours)

    def _run(self, trigger: str) -> None:
        res = ColdSweepResult(trigger=trigger, started_at=time.time())
        with self._lock:
            self._current = res
        slot_date = None
        if trigger == "scheduled":
            now = datetime.now()
            hh, mm = (int(x) for x in self.config.cold_sweep.daily_run_at.split(":"))
            target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
            if now < target:
                target -= timedelta(days=1)
            slot_date = target.date().isoformat()
        try:
            run_cold_sweep(
                self.config, self.db, self.dropbox,
                should_continue=lambda: not self.stop_event.is_set() and not self.is_paused(),
                trigger=trigger, result=res,
            )
        finally:
            with self._lock:
                self._current = None
                self._last = res.to_dict()
                # An aborted scheduled run (paused) retries within the
                # catch-up window once the pipeline resumes.
                if slot_date and res.status in ("done", "error"):
                    self._last_scheduled_date = slot_date
            self._save_state()

    # --------------- persistence ------------------------------------------

    def _load_state(self) -> None:
        try:
            data = json.loads(self.state_path.read_text(encoding="utf-8"))
        except Exception:
            return
        self._last = data.get("last")
        self._last_scheduled_date = data.get("last_scheduled_date")

    def _save_state(self) -> None:
        try:
            with self._lock:
                data = {"last": self._last,
                        "last_scheduled_date": self._last_scheduled_date}
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            self.state_path.write_text(json.dumps(data), encoding="utf-8")
        except Exception as e:
            logger.debug(f"cold sweep: state save failed: {e}")
