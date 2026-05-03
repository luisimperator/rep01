"""
Reduction-map census worker + deep-scan worker.

The census walks the entire Dropbox tree under config.dropbox_root once a
day, classifies every file into one of three buckets:

  - pending     — H.264 candidate that the daemon will (eventually) transcode
  - done        — already H.265 in some form (sibling /h265 file, mirror layout
                  output, DB record, probe_cache codec match, or filename hint)
  - ineligible  — filtered out by static rules (in /assets, partial, YouTube
                  download, below min_size, image-codec by extension, etc)

Per-folder rollups land in `folder_census`. The dashboard's "Reduction map"
tree renders directly from those rows; the dispatcher uses
`pending_bytes` per folder as a priority signal so the largest backlog
gets drained first.

The deep-scan worker probes "unknown" files (passed every static filter
but the daemon hasn't seen them yet) via a Dropbox temporary CDN link
plus ffprobe with `-probesize` capped low — reads ~1-5 MB instead of the
full file. Result lands in `probe_cache`. Catches H.265-natively-encoded
files that would otherwise look pending until the main pipeline finally
reaches them.
"""

from __future__ import annotations

import json
import logging
import re
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, time as dt_time, timedelta
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Iterable

from .database import Database
from .utils import (
    is_image_codec,
    is_in_h265_folder,
    is_partial_file,
    is_video_file,
    is_youtube_download,
    matches_exclude_pattern,
    path_has_assets_segment,
)

if TYPE_CHECKING:
    from .config import Config
    from .dropbox_client import DropboxClient

logger = logging.getLogger(__name__)


# Filename patterns hinting "this file is already HEVC".
# Matching means we trust the name and skip the file as 'done'.
_HEVC_NAME_PATTERNS = (
    re.compile(r'(?:^|[^a-z0-9])h265(?:[^a-z0-9]|$)', re.IGNORECASE),
    re.compile(r'(?:^|[^a-z0-9])hevc(?:[^a-z0-9]|$)', re.IGNORECASE),
    re.compile(r'\(\d+p_\d+fps_HEVC-', re.IGNORECASE),  # yt-dlp HEVC pattern
    re.compile(r'\(\d+p_\d+fps_AV1-', re.IGNORECASE),   # AV1 too — already efficient
)

_HEVC_CODEC_NAMES = frozenset({"hevc", "h265", "hev1", "hvc1"})


def filename_hints_hevc(path: str) -> bool:
    """True when the filename contains a strong H.265/HEVC hint."""
    name = PurePosixPath(path).name
    return any(p.search(name) for p in _HEVC_NAME_PATTERNS)


# -------- output-path helpers (mirror of utils.get_output_path but local) ----

def _h265_sibling_path(path: str) -> str:
    """Sibling-layout output: /A/B/clip.mp4 -> /A/B/h265/clip.mp4."""
    p = PurePosixPath(path)
    return str(p.parent / "h265" / p.name)


def _mirror_output_path(path: str, dropbox_root: str, mirror_root: str) -> str | None:
    """Mirror-layout output: /A/B/clip.mp4 -> /<root>/<mirror>/A/B/clip.mp4.

    Returns None if the file isn't under dropbox_root (defensive — happens
    on weird absolute paths from cross-namespace symlinks).
    """
    p = PurePosixPath(path)
    root = PurePosixPath(dropbox_root or "/")
    try:
        rel = p.relative_to(root)
    except ValueError:
        return None
    return str(root / mirror_root / rel)


# ---------- classification ---------------------------------------------------

@dataclass
class FolderStats:
    pending_count: int = 0
    pending_bytes: int = 0
    done_count: int = 0
    done_bytes: int = 0
    ineligible_count: int = 0
    ineligible_bytes: int = 0


def classify_file(
    path: str,
    size: int,
    config: "Config",
    all_paths: set[str],
    done_paths: set[str],
    hevc_paths: set[str],
    probe_codecs: dict[str, str],
) -> str | None:
    """Classify a single file into 'pending' | 'done' | 'ineligible' | None.

    None means the file doesn't belong to any video bucket (non-video
    extension, audio file, etc) and the census just ignores it.

    Order of checks matters: the "done" signals win over ineligible
    signals because once a file is converted, no one cares why it
    might also have been ineligible.
    """
    # Not a video extension → not interesting for the reduction map at all.
    if not is_video_file(path, config.video_extensions):
        return None

    # 1) Sibling /h265/<name> exists in the tree?
    if _h265_sibling_path(path) in all_paths:
        return "done"

    # 2) Mirror layout output exists?
    if config.output_layout.value == "mirror":
        mp = _mirror_output_path(path, config.dropbox_root, config.output_mirror_root)
        if mp and mp in all_paths:
            return "done"

    # 3) DB knows the file finished
    if path in done_paths:
        return "done"

    # 4) DB classified the file as already-HEVC during preflight probe
    if path in hevc_paths:
        return "done"

    # 5) probe_cache says the codec is HEVC
    codec = probe_codecs.get(path)
    if codec and codec in _HEVC_CODEC_NAMES:
        return "done"

    # 6) Filename hint
    if filename_hints_hevc(path):
        return "done"

    # File lives inside the h265 output folder — already converted, treat as done.
    if is_in_h265_folder(path, mirror_root=config.output_mirror_root):
        return "done"

    # ---- ineligible signals ----
    if path_has_assets_segment(path):
        return "ineligible"
    if is_partial_file(path):
        return "ineligible"
    if matches_exclude_pattern(path, config.exclude_patterns):
        return "ineligible"
    if is_youtube_download(path):
        return "ineligible"
    if size < config.min_size_bytes():
        return "ineligible"
    # Probe cache says it's an image codec wrapped in a video container?
    if codec and is_image_codec(codec):
        return "ineligible"

    # Default: H.264 (or unknown) of decent size → will be transcoded.
    return "pending"


def _bump(stats: FolderStats, bucket: str, size: int) -> None:
    if bucket == "pending":
        stats.pending_count += 1
        stats.pending_bytes += size
    elif bucket == "done":
        stats.done_count += 1
        stats.done_bytes += size
    elif bucket == "ineligible":
        stats.ineligible_count += 1
        stats.ineligible_bytes += size


@dataclass
class CensusResult:
    folders: dict[str, FolderStats]
    files_classified: int
    started_at: float
    finished_at: float

    def totals(self) -> dict:
        t = FolderStats()
        for s in self.folders.values():
            t.pending_count += s.pending_count
            t.pending_bytes += s.pending_bytes
            t.done_count += s.done_count
            t.done_bytes += s.done_bytes
            t.ineligible_count += s.ineligible_count
            t.ineligible_bytes += s.ineligible_bytes
        return {
            "pending_count": t.pending_count,
            "pending_bytes": t.pending_bytes,
            "done_count": t.done_count,
            "done_bytes": t.done_bytes,
            "ineligible_count": t.ineligible_count,
            "ineligible_bytes": t.ineligible_bytes,
        }


def run_census(
    config: "Config",
    db: Database,
    dropbox: "DropboxClient",
    stop_event: threading.Event,
    progress_cb=None,
) -> CensusResult:
    """Walk Dropbox once, classify everything, return the per-folder rollup.

    Two-pass over the tree:
      1) List all entries (path + size) into memory. Build a path-set so
         h265-sibling and mirror-output lookups are O(1).
      2) Classify each path against the 5 done-signals + 6 ineligible-signals.

    Memory cost: ~150 bytes per file × N files. For 100k files that's
    ~15 MB — comfortably affordable.
    """
    started = time.time()

    # Snapshot DB knowledge BEFORE the walk so the census reflects a
    # consistent moment in time.
    done_paths = db.get_done_paths()
    hevc_paths = db.get_skipped_hevc_paths()
    probe_codecs = db.get_probe_cache_codecs()

    # ---- pass 1: enumerate the whole tree ---------------------------------
    all_files: list[tuple[str, int]] = []
    all_paths: set[str] = set()
    yielded = 0
    for entry in dropbox.list_folder_entries(config.dropbox_root, recursive=True):
        if stop_event.is_set():
            raise _CensusStopped()
        if isinstance(entry, str):
            # Page-boundary cursor; ignored — we don't checkpoint census.
            continue
        # entry is a DropboxFileInfo
        all_files.append((entry.path, int(entry.size)))
        all_paths.add(entry.path)
        yielded += 1
        if progress_cb is not None and (yielded % 1000) == 0:
            progress_cb({"phase": "list", "files_seen": yielded})

    if progress_cb is not None:
        progress_cb({"phase": "classify", "total": len(all_files), "done": 0})

    # ---- pass 2: classify --------------------------------------------------
    folders: dict[str, FolderStats] = {}
    classified = 0
    for path, size in all_files:
        if stop_event.is_set():
            raise _CensusStopped()
        bucket = classify_file(
            path, size, config,
            all_paths=all_paths,
            done_paths=done_paths,
            hevc_paths=hevc_paths,
            probe_codecs=probe_codecs,
        )
        if bucket is None:
            continue
        parent = str(PurePosixPath(path).parent)
        stats = folders.get(parent)
        if stats is None:
            stats = FolderStats()
            folders[parent] = stats
        _bump(stats, bucket, size)
        classified += 1
        if progress_cb is not None and (classified % 5000) == 0:
            progress_cb({"phase": "classify", "total": len(all_files), "done": classified})

    finished = time.time()
    return CensusResult(
        folders=folders,
        files_classified=classified,
        started_at=started,
        finished_at=finished,
    )


def persist_census(db: Database, run_id: int, result: CensusResult) -> None:
    rows = [
        {
            "path": folder,
            "pending_count": s.pending_count,
            "pending_bytes": s.pending_bytes,
            "done_count": s.done_count,
            "done_bytes": s.done_bytes,
            "ineligible_count": s.ineligible_count,
            "ineligible_bytes": s.ineligible_bytes,
        }
        for folder, s in result.folders.items()
    ]
    db.replace_folder_census(rows)
    db.census_run_finished(
        run_id,
        folders_scanned=len(result.folders),
        files_classified=result.files_classified,
        totals=result.totals(),
    )


# ---------- the daemon thread ------------------------------------------------

class _CensusStopped(Exception):
    """Internal signal for clean shutdown / cancellation."""


@dataclass
class CensusProgress:
    """Last-known progress of an in-flight (or recent) census run."""
    phase: str = "idle"   # idle | listing | classifying | persisting
    files_seen: int = 0
    total: int = 0
    done: int = 0
    started_at: float = 0.0
    finished_at: float = 0.0
    error: str | None = None


class CensusWorker(threading.Thread):
    """Daily reduction-map walk + on-demand triggering."""

    def __init__(
        self,
        config: "Config",
        db: Database,
        dropbox: "DropboxClient",
        stop_event: threading.Event,
    ) -> None:
        super().__init__(name="census-worker", daemon=True)
        self.config = config
        self.db = db
        self.dropbox = dropbox
        self.stop_event = stop_event
        self._wake = threading.Event()
        self._lock = threading.Lock()
        self.progress = CensusProgress()

    # --------------- public API for the dashboard -----------------------

    def trigger_now(self) -> None:
        """Wake the worker so it runs a census immediately on its next loop tick."""
        self._wake.set()

    def status(self) -> dict:
        with self._lock:
            return {
                "phase": self.progress.phase,
                "files_seen": self.progress.files_seen,
                "total": self.progress.total,
                "done": self.progress.done,
                "started_at": self.progress.started_at,
                "finished_at": self.progress.finished_at,
                "error": self.progress.error,
            }

    # --------------- thread loop ----------------------------------------

    def run(self) -> None:
        if not self.config.census.enabled:
            logger.info("census: disabled in config; worker exiting")
            return

        # Initial run a few minutes after startup so the dashboard isn't
        # blank on first launch.
        if self.config.census.initial_run_on_startup:
            initial_delay = max(0, int(self.config.census.initial_run_delay_sec))
            logger.info(f"census: initial run scheduled in {initial_delay}s")
            self._sleep_or_trigger(initial_delay)
            if not self.stop_event.is_set():
                self._do_run("initial")

        # Main loop: sleep until next daily_run_at or until trigger_now wakes us.
        while not self.stop_event.is_set():
            seconds_to_target = self._seconds_until_daily_target()
            logger.info(
                f"census: next scheduled run at {self.config.census.daily_run_at} "
                f"(in {int(seconds_to_target)}s); use /api/census-now to override"
            )
            self._sleep_or_trigger(seconds_to_target)
            if self.stop_event.is_set():
                break
            self._do_run("scheduled")

    # --------------- helpers ---------------------------------------------

    def _sleep_or_trigger(self, seconds: float) -> None:
        """Wait for either the timeout, a manual trigger, or shutdown."""
        deadline = time.monotonic() + max(1.0, seconds)
        self._wake.clear()
        while not self.stop_event.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            # Wake on either trigger or end of slice
            if self._wake.wait(timeout=min(remaining, 5.0)):
                self._wake.clear()
                return

    def _seconds_until_daily_target(self) -> float:
        """Seconds from now until the next daily_run_at clock time (local)."""
        try:
            hh_str, mm_str = self.config.census.daily_run_at.split(":")
            target_h = int(hh_str)
            target_m = int(mm_str)
        except (ValueError, AttributeError):
            target_h, target_m = 8, 30
        now = datetime.now()
        target = now.replace(hour=target_h, minute=target_m, second=0, microsecond=0)
        if target <= now:
            target = target + timedelta(days=1)
        return max(60.0, (target - now).total_seconds())

    def _do_run(self, reason: str) -> None:
        run_id = self.db.census_run_started()
        with self._lock:
            self.progress = CensusProgress(
                phase="listing",
                started_at=time.time(),
            )

        def cb(p: dict) -> None:
            with self._lock:
                if p.get("phase") == "list":
                    self.progress.phase = "listing"
                    self.progress.files_seen = int(p.get("files_seen", 0))
                elif p.get("phase") == "classify":
                    self.progress.phase = "classifying"
                    self.progress.total = int(p.get("total", 0))
                    self.progress.done = int(p.get("done", 0))

        try:
            logger.info(f"census: starting ({reason})")
            result = run_census(
                self.config, self.db, self.dropbox,
                stop_event=self.stop_event, progress_cb=cb,
            )
            with self._lock:
                self.progress.phase = "persisting"
            persist_census(self.db, run_id, result)
            with self._lock:
                self.progress.phase = "idle"
                self.progress.finished_at = time.time()
                self.progress.error = None
            totals = result.totals()
            logger.info(
                f"census: done — {result.files_classified} files in "
                f"{len(result.folders)} folders · "
                f"{totals['pending_count']} pending "
                f"({totals['pending_bytes']/1e12:.2f}TB) · "
                f"{totals['done_count']} done · "
                f"{totals['ineligible_count']} ineligible · "
                f"{result.finished_at - result.started_at:.0f}s"
            )
        except _CensusStopped:
            logger.info("census: stopped (shutdown)")
            self.db.census_run_finished(
                run_id, folders_scanned=0, files_classified=0,
                totals={}, error="cancelled",
            )
            with self._lock:
                self.progress.phase = "idle"
                self.progress.error = "cancelled"
        except Exception as e:
            logger.exception(f"census: FAILED — {e}")
            self.db.census_run_finished(
                run_id, folders_scanned=0, files_classified=0,
                totals={}, error=str(e),
            )
            with self._lock:
                self.progress.phase = "idle"
                self.progress.error = str(e)


# ============================================================================
# Deep scan — header-only ffprobe over Dropbox temp links
# ============================================================================


@dataclass
class DeepScanProgress:
    state: str = "idle"        # idle | running | cancelling | finished | error
    total: int = 0
    done: int = 0
    probed: int = 0
    skipped: int = 0
    failed: int = 0
    started_at: float = 0.0
    finished_at: float = 0.0
    current_path: str = ""
    error: str | None = None


class DeepScanWorker:
    """Header-only probe of every "unknown" file, populating probe_cache.

    Not a long-lived thread — spun up on demand by /api/deep-scan/start.
    Iterates every video file in the configured root that we don't already
    know the codec for (no probe_cache row, no DONE/SKIPPED_HEVC job, no
    h265 sibling, no filename hint), pulls a temporary CDN URL, and runs
    `ffprobe -probesize 5M` on it. Codec + bitrate land in probe_cache so
    the next census reflects truth.

    Bandwidth priority: while deep scan runs, we activate the global
    BandwidthGovernor in dropbox_client which caps the main pipeline's
    download/upload chunk rate to ~1 MB/s. The pipeline keeps moving
    (no pause), just slowly — so the deep scan probes get the lion's
    share of the WAN.
    """

    def __init__(
        self,
        config: "Config",
        db: Database,
        dropbox: "DropboxClient",
        stop_event: threading.Event,
        dispatcher=None,
    ) -> None:
        self.config = config
        self.db = db
        self.dropbox = dropbox
        self.stop_event = stop_event
        # Dispatcher kept for symmetry with future features; deep scan
        # no longer pauses it (we throttle instead).
        self.dispatcher = dispatcher
        self._cancel = threading.Event()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self.progress = DeepScanProgress()

    def is_running(self) -> bool:
        with self._lock:
            return self.progress.state in ("running", "cancelling")

    def status(self) -> dict:
        with self._lock:
            return {
                "state": self.progress.state,
                "total": self.progress.total,
                "done": self.progress.done,
                "probed": self.progress.probed,
                "skipped": self.progress.skipped,
                "failed": self.progress.failed,
                "started_at": self.progress.started_at,
                "finished_at": self.progress.finished_at,
                "current_path": self.progress.current_path,
                "error": self.progress.error,
            }

    def start(self) -> bool:
        with self._lock:
            if self.progress.state in ("running", "cancelling"):
                return False
            self._cancel.clear()
            self.progress = DeepScanProgress(
                state="running",
                started_at=time.time(),
            )
        self._thread = threading.Thread(
            target=self._run, name="deep-scan", daemon=True
        )
        self._thread.start()
        return True

    def cancel(self) -> None:
        self._cancel.set()
        with self._lock:
            if self.progress.state == "running":
                self.progress.state = "cancelling"

    # --------------- worker body ---------------------------------------

    def _run(self) -> None:
        # Claim bandwidth priority: throttle the main pipeline's chunk
        # rate via the global BandwidthGovernor. Pipeline keeps running
        # but its downloads/uploads now creep at the configured cap,
        # leaving the WAN free for our deep-scan probes. throttle_mbps
        # of 0 disables the throttle entirely (pipeline runs full speed).
        from .dropbox_client import GOVERNOR
        throttle_mbps = float(getattr(self.config.census, "deep_scan_pipeline_throttle_mbps", 1.0))
        if throttle_mbps > 0:
            GOVERNOR.set_throttle(True, max_mbps=throttle_mbps)
            logger.info(f"deep-scan: pipeline throttled to {throttle_mbps} MB/s for bandwidth priority")
        try:
            candidates = self._collect_candidates()
            with self._lock:
                self.progress.total = len(candidates)
            concurrency = max(1, int(self.config.census.deep_scan_concurrency))
            logger.info(
                f"deep-scan: probing {len(candidates)} unknown files "
                f"with {concurrency} parallel workers"
            )
            self._probe_all_parallel(candidates, concurrency)

            with self._lock:
                self.progress.state = "finished"
                self.progress.current_path = ""
                self.progress.finished_at = time.time()
            logger.info(
                f"deep-scan: finished — probed={self.progress.probed} "
                f"skipped={self.progress.skipped} failed={self.progress.failed}"
            )
        except Exception as e:
            logger.exception(f"deep-scan: FAILED — {e}")
            with self._lock:
                self.progress.state = "error"
                self.progress.error = str(e)
                self.progress.finished_at = time.time()
        finally:
            # Always release the throttle so the pipeline returns to
            # full speed whether the scan succeeded, failed, or was
            # cancelled.
            if throttle_mbps > 0:
                GOVERNOR.set_throttle(False)
                logger.info("deep-scan: pipeline throttle released")

    def _probe_all_parallel(self, candidates: list[tuple[str, int]], concurrency: int) -> None:
        """Probe candidates in parallel via a ThreadPoolExecutor.

        Each worker holds the GIL only briefly per probe — the bulk of
        the time is in subprocess.run waiting on ffprobe (which spawns
        its own process and does network I/O). So real parallelism is
        achieved despite the GIL.

        Cancel handling: when self._cancel fires we stop submitting new
        work and let in-flight probes finish on their own (ffprobe
        timeout caps them at 60s anyway). The executor is shut down
        without wait once the loop exits, so the worker thread can
        return promptly while leftover probes finish in the background.
        """
        if not candidates:
            return
        executor = ThreadPoolExecutor(
            max_workers=concurrency,
            thread_name_prefix="deep-scan-probe",
        )
        try:
            futures = {}
            for path, size in candidates:
                if self._cancel.is_set() or self.stop_event.is_set():
                    break
                futures[executor.submit(self._probe_with_check, path)] = path
            for fut in as_completed(futures):
                path = futures[fut]
                try:
                    outcome = fut.result()
                except Exception as e:
                    logger.warning(f"deep-scan: probe error for {path}: {e}")
                    outcome = "failed"
                with self._lock:
                    self.progress.done += 1
                    self.progress.current_path = path
                    if outcome == "probed":
                        self.progress.probed += 1
                    elif outcome == "skipped":
                        self.progress.skipped += 1
                    else:
                        self.progress.failed += 1
        finally:
            # cancel_futures=True (3.9+) cancels not-yet-started work
            # so a cancel button takes effect quickly even if hundreds
            # of probes were queued.
            executor.shutdown(wait=False, cancel_futures=True)

    def _probe_with_check(self, path: str) -> str:
        """Probe wrapper that bails early when cancellation is requested."""
        if self._cancel.is_set() or self.stop_event.is_set():
            return "skipped"
        return self._probe_one(path)

    def _collect_candidates(self) -> list[tuple[str, int]]:
        """List files that pass static eligibility filters and we don't already know about."""
        done = self.db.get_done_paths()
        hevc = self.db.get_skipped_hevc_paths()
        cached = set(self.db.get_probe_cache_codecs().keys())

        all_paths: set[str] = set()
        all_files: list[tuple[str, int]] = []
        for entry in self.dropbox.list_folder_entries(self.config.dropbox_root, recursive=True):
            if self._cancel.is_set() or self.stop_event.is_set():
                return []
            if isinstance(entry, str):
                continue
            all_files.append((entry.path, int(entry.size)))
            all_paths.add(entry.path)

        candidates: list[tuple[str, int]] = []
        for path, size in all_files:
            if not is_video_file(path, self.config.video_extensions):
                continue
            # Skip already-known-done files
            if path in done or path in hevc or path in cached:
                continue
            # Skip if h265 sibling exists OR mirror exists
            if _h265_sibling_path(path) in all_paths:
                continue
            if self.config.output_layout.value == "mirror":
                mp = _mirror_output_path(path, self.config.dropbox_root, self.config.output_mirror_root)
                if mp and mp in all_paths:
                    continue
            # Skip filename-hinted HEVC
            if filename_hints_hevc(path):
                continue
            # Skip the obvious ineligibles to save API calls (deep scan
            # doesn't help classify a /assets file).
            if path_has_assets_segment(path):
                continue
            if is_in_h265_folder(path, mirror_root=self.config.output_mirror_root):
                continue
            if is_partial_file(path):
                continue
            if matches_exclude_pattern(path, self.config.exclude_patterns):
                continue
            if is_youtube_download(path):
                continue
            if size < self.config.min_size_bytes():
                continue
            candidates.append((path, size))
        return candidates

    def _probe_one(self, path: str) -> str:
        """ffprobe via temp link. Returns 'probed' | 'skipped' | 'failed'."""
        url = self.dropbox.get_temporary_link(path)
        if not url:
            return "skipped"

        cmd = [
            self.config.ffprobe_path,
            "-v", "quiet",
            "-print_format", "json",
            "-show_streams",
            "-show_format",
            "-probesize", "5M",
            "-analyzeduration", "5M",
            "-i", url,
        ]
        try:
            r = subprocess.run(
                cmd, capture_output=True, text=True, timeout=60,
            )
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as e:
            logger.debug(f"deep-scan: ffprobe failed for {path}: {e}")
            return "failed"
        if r.returncode != 0 or not r.stdout.strip():
            return "failed"

        try:
            data = json.loads(r.stdout)
        except json.JSONDecodeError:
            return "failed"

        codec = None
        bitrate_kbps = None
        pix_fmt = None
        width = None
        height = None
        duration = None
        for stream in data.get("streams", []):
            if stream.get("codec_type") == "video":
                codec = (stream.get("codec_name") or "").lower() or None
                pix_fmt = stream.get("pix_fmt") or None
                try:
                    width = int(stream.get("width") or 0) or None
                    height = int(stream.get("height") or 0) or None
                except (ValueError, TypeError):
                    pass
                br = stream.get("bit_rate") or data.get("format", {}).get("bit_rate")
                try:
                    if br:
                        bitrate_kbps = int(int(br) / 1000)
                except (ValueError, TypeError):
                    pass
                dur = stream.get("duration") or data.get("format", {}).get("duration")
                try:
                    if dur:
                        duration = float(dur)
                except (ValueError, TypeError):
                    pass
                break

        if codec is None:
            return "failed"

        self.db.put_probe_cache(
            path,
            codec=codec,
            bitrate_kbps=bitrate_kbps,
            pix_fmt=pix_fmt,
            width=width,
            height=height,
            duration_sec=duration,
        )
        return "probed"
