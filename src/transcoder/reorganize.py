"""
Post-upload Dropbox reorganization helpers.

Replicates the legacy GUI's "swap H.265 into the original's spot, back the
H.264 up under h264/, journal it in h265 feito.txt" workflow. Used both by
the live UploadWorker and by the `hd reorganize-existing` retroactive sweep.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Iterable

from .dropbox_client import DropboxClient, DropboxFileInfo

if TYPE_CHECKING:
    from .database import Database

logger = logging.getLogger(__name__)


@dataclass
class FolderActivity:
    """Result of an is_folder_settled check."""
    settled: bool
    newest_modified: datetime | None
    days_since_newest: float | None
    threshold_days: int


def is_folder_settled(
    dropbox: DropboxClient,
    parent: str,
    min_age_days: int,
) -> FolderActivity:
    """
    Check whether `parent` has had user activity in the last `min_age_days`.

    "Activity" = any file directly in `parent` (non-recursive — files in
    /h264 and /h265 subfolders don't count, since those are the daemon's
    own outputs/backups). Returns settled=True when the newest such file is
    older than the threshold (or when the folder has no files at all).

    A min_age_days of 0 short-circuits to settled=True.
    """
    if min_age_days <= 0:
        return FolderActivity(True, None, None, min_age_days)

    threshold = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=min_age_days)
    newest: datetime | None = None

    for entry in dropbox.list_folder(parent, recursive=False):
        if entry.name.lower() == "h265 feito.txt":
            # Daemon-written journal, not user activity.
            continue
        modified = entry.server_modified
        if modified.tzinfo is not None:
            modified = modified.astimezone(timezone.utc).replace(tzinfo=None)
        if newest is None or modified > newest:
            newest = modified

    if newest is None:
        return FolderActivity(True, None, None, min_age_days)

    days_since = (datetime.now(timezone.utc).replace(tzinfo=None) - newest).total_seconds() / 86400.0
    return FolderActivity(newest < threshold, newest, days_since, min_age_days)


@dataclass
class ReorganizeLayout:
    """Defines the directory + extension scheme for one reorganize batch.

    The video pipeline backs <name>.mp4 up to h264/<name>.mp4 and lifts
    h265/<name>.mp4 into the original's spot. The audio pipeline does the
    same dance with wav/ and mp3/ — but the output extension differs from
    the input, so output_ext is what the swapped file ends up named with
    (instead of preserving the original ext).
    """
    backup_subdir: str       # 'h264' or 'wav'
    output_subdir: str       # 'h265' or 'mp3'
    output_ext: str | None   # None = keep original extension; else '.mp3' etc.
    feito_filename: str      # journal file written inside <parent>/<output_subdir>/

    def output_name(self, original_name: str) -> str:
        """The filename the swapped file should have under <parent>/."""
        if self.output_ext is None:
            return original_name
        return PurePosixPath(original_name).stem + self.output_ext


VIDEO_LAYOUT = ReorganizeLayout(
    backup_subdir="h264",
    output_subdir="h265",
    output_ext=None,                  # H.265 keeps the .mp4 extension
    feito_filename="h265 feito.txt",
)

AUDIO_LAYOUT = ReorganizeLayout(
    backup_subdir="wav",
    output_subdir="mp3",
    output_ext=".mp3",
    feito_filename="mp3 feito.txt",
)


def reorganize_pair(
    dropbox: DropboxClient,
    parent: str,
    name: str,
    original_size: int,
    output_size: int,
    layout: ReorganizeLayout = VIDEO_LAYOUT,
) -> str:
    """
    Atomically swap the transcoded output into the original's spot.

    Steps (illustrated for the video layout — audio is identical with
    h264→wav, h265→mp3, and an extension change):
      1. Ensure /<parent>/h264/ exists.
      2. Move /<parent>/<name> (the H.264 original) to /<parent>/h264/<name>.
      3. Move /<parent>/h265/<name> (the H.265) to /<parent>/<name>.
         On failure, rolls step 2 back so the original stays at its
         canonical path.
      4. Append a line to /<parent>/h265/h265 feito.txt in the legacy
         GUI's format.

    Returns the final Dropbox path of the swapped file. Raises on
    unrecoverable failure.
    """
    parent = parent.rstrip('/') if parent != '/' else ''
    backup_dir = parent + '/' + layout.backup_subdir
    backup_path = backup_dir + '/' + name
    output_name = layout.output_name(name)
    output_temp = parent + '/' + layout.output_subdir + '/' + output_name
    final_path = parent + '/' + output_name
    feito_path = parent + '/' + layout.output_subdir + '/' + layout.feito_filename
    original_path = parent + '/' + name

    logger.info(f"reorganize: backing original up to {backup_path}")
    dropbox.create_folder(backup_dir)
    dropbox.move_file(original_path, backup_path, allow_overwrite=True)

    try:
        logger.info(f"reorganize: swapping output into {final_path}")
        dropbox.move_file(output_temp, final_path, allow_overwrite=False)
    except Exception:
        logger.warning(
            f"reorganize: swap failed; rolling back backup "
            f"from {backup_path} to {original_path}"
        )
        try:
            dropbox.move_file(backup_path, original_path, allow_overwrite=False)
        except Exception as rollback_err:
            logger.error(
                f"reorganize: rollback also failed: {rollback_err}. "
                f"Original is at {backup_path}; output at {output_temp}."
            )
        raise

    try:
        _append_feito_log(dropbox, feito_path, name, original_size, output_size)
    except Exception as log_err:
        logger.warning(f"reorganize: feito.txt append failed: {log_err}")

    logger.info(
        f"reorganize complete: original at {backup_path}, output at {final_path}"
    )
    return final_path


def _append_feito_log(
    dropbox: DropboxClient,
    feito_path: str,
    name: str,
    input_size: int,
    output_size: int,
) -> None:
    input_mb = input_size / (1024 ** 2)
    output_mb = output_size / (1024 ** 2)
    reduction = (1 - output_size / input_size) * 100 if input_size > 0 else 0.0
    line = (
        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
        f"{name} | "
        f"{input_mb:.1f}MB -> {output_mb:.1f}MB ({reduction:.1f}% menor)\n"
    )
    existing = dropbox.read_text_file(feito_path) or ""
    if existing and not existing.endswith("\n"):
        existing += "\n"
    dropbox.write_text_file(feito_path, existing + line)


# --------------------------------------------------------------------------
# Retroactive sweep: find pairs (original at /<parent>/<name>, H.265 at
# /<parent>/h265/<name>, no /<parent>/h264/<name> yet) and reorganize them.
# --------------------------------------------------------------------------


@dataclass
class PairCandidate:
    parent: str
    name: str
    original: DropboxFileInfo
    h265: DropboxFileInfo


@dataclass
class FolderCandidate:
    parent: str
    pairs: list[PairCandidate]
    activity: FolderActivity | None = None
    skip_reason: str | None = None


def find_unreorganized_pairs(
    dropbox: DropboxClient,
    dropbox_root: str,
    layout: ReorganizeLayout = VIDEO_LAYOUT,
) -> list[FolderCandidate]:
    """
    Walk `dropbox_root` recursively and find folders with outputs of the
    given layout that haven't been reorganized into the legacy spot yet.

    For VIDEO_LAYOUT a folder is a candidate when it contains BOTH:
      - /<parent>/<name>           (original, presumed H.264)
      - /<parent>/h265/<name>      (H.265 output)
    AND does NOT already contain:
      - /<parent>/h264/<name>      (would mean already reorganized)

    For AUDIO_LAYOUT the same idea applies with wav/mp3 subfolder names
    AND the output extension differs (.wav -> .mp3); we look up the
    expected output filename via layout.output_name().

    The case of the subfolder names is normalized when matching, but the
    actual move uses the path Dropbox returned.
    """
    # Index every file by its parent directory and by lowercase basename.
    # Keys are case-insensitive parent paths so we can match siblings.
    by_parent: dict[str, dict[str, DropboxFileInfo]] = {}

    for entry in dropbox.list_folder(dropbox_root, recursive=True):
        parent_path = str(PurePosixPath(entry.path).parent)
        # Normalize but keep original-cased keys so we can reconstruct paths
        by_parent.setdefault(parent_path, {})[entry.name] = entry

    # Helper: case-insensitive subfolder lookup.
    def find_subfolder_files(parent: str, sub: str) -> dict[str, DropboxFileInfo]:
        wanted = parent.rstrip('/') + '/' + sub
        wanted_lower = wanted.lower()
        for p, files in by_parent.items():
            if p.lower() == wanted_lower:
                return files
        return {}

    candidates: list[FolderCandidate] = []
    seen_parents = set()
    skip_dirs_lower = {layout.backup_subdir.lower(), layout.output_subdir.lower()}

    for parent, files in by_parent.items():
        # Skip the layout's own subfolder names — they're not target parents.
        last = PurePosixPath(parent).name.lower()
        if last in skip_dirs_lower:
            continue
        if parent in seen_parents:
            continue
        seen_parents.add(parent)

        out_files = find_subfolder_files(parent, layout.output_subdir)
        if not out_files:
            continue
        backup_files = find_subfolder_files(parent, layout.backup_subdir)

        pairs: list[PairCandidate] = []
        for name, original in files.items():
            if name.lower() == layout.feito_filename.lower():
                continue
            # The output file may have a different extension (audio: .wav -> .mp3).
            expected_output = layout.output_name(name)
            out_match = None
            for out_name, out_info in out_files.items():
                if out_name.lower() == expected_output.lower():
                    out_match = out_info
                    break
            if out_match is None:
                continue
            # Already-reorganized check: if a file with the same ORIGINAL name
            # exists under the backup subfolder, the swap was already done.
            already = any(n.lower() == name.lower() for n in backup_files)
            if already:
                continue
            pairs.append(PairCandidate(
                parent=parent,
                name=name,
                original=original,
                h265=out_match,
            ))

        if pairs:
            candidates.append(FolderCandidate(parent=parent, pairs=pairs))

    return candidates


# --------------------------------------------------------------------------
# Per-folder gating: only reorganize when every job in a folder is finished.
# Used by the live UploadWorker so the swap happens once per folder instead
# of once per file.
# --------------------------------------------------------------------------


@dataclass
class FolderCompletion:
    """Result of an is_folder_complete check."""
    complete: bool
    total_jobs: int
    pending_states: dict[str, int]  # state name -> count of jobs blocking us
    reason: str


def is_folder_complete(db: "Database", parent: str) -> FolderCompletion:
    """
    Check whether every job whose dropbox_path lives directly in `parent` has
    reached a terminal state (DONE or any SKIPPED_*). FAILED and RETRY_WAIT
    block — the user opted into "all-or-nothing": one bad job freezes the
    folder until they intervene. In-progress states (DOWNLOADING etc.) also
    block.

    Returns FolderCompletion(False, ...) when there are no jobs at all — a
    folder we've never seen isn't a folder we should reorganize.
    """
    from .database import TERMINAL_STATES
    jobs = db.get_jobs_in_folder(parent)
    if not jobs:
        return FolderCompletion(False, 0, {}, "no jobs tracked in this folder")
    pending: dict[str, int] = {}
    for j in jobs:
        if j.state not in TERMINAL_STATES:
            pending[j.state.value] = pending.get(j.state.value, 0) + 1
    if pending:
        summary = ", ".join(f"{n}×{s}" for s, n in sorted(pending.items()))
        return FolderCompletion(False, len(jobs), pending, summary)
    return FolderCompletion(True, len(jobs), {}, "")


def find_unreorganized_pairs_in_folder(
    dropbox: DropboxClient,
    parent: str,
    layout: ReorganizeLayout = VIDEO_LAYOUT,
) -> list[PairCandidate]:
    """
    Single-folder version of find_unreorganized_pairs.

    Lists `<parent>/`, `<parent>/<output_subdir>/` and
    `<parent>/<backup_subdir>/` (each non-recursive) and returns the pairs
    where:
      - `<parent>/<name>`                       (original) still exists
      - `<parent>/<output_subdir>/<output>`    (transcode output) exists
      - `<parent>/<backup_subdir>/<name>`       (already-reorganized backup)
                                                 does NOT exist

    For audio the output filename has a different extension than the
    original (.wav → .mp3); we look it up via layout.output_name().

    Survives missing subfolders by treating them as empty.
    """
    parent = parent.rstrip('/') if parent != '/' else ''

    def _safe_list(path: str) -> list[DropboxFileInfo]:
        try:
            return list(dropbox.list_folder(path or '/', recursive=False))
        except Exception:
            return []

    parent_entries = _safe_list(parent)
    out_entries = _safe_list(parent + '/' + layout.output_subdir)
    if not out_entries:
        return []
    backup_entries = _safe_list(parent + '/' + layout.backup_subdir)

    out_by_lower = {e.name.lower(): e for e in out_entries}
    backup_lower = {e.name.lower() for e in backup_entries}

    pairs: list[PairCandidate] = []
    for entry in parent_entries:
        if entry.name.lower() == layout.feito_filename.lower():
            continue
        # Map the original name to its expected output name (extension may
        # differ for audio). Match in the output subfolder.
        expected_output = layout.output_name(entry.name)
        out_match = out_by_lower.get(expected_output.lower())
        if out_match is None:
            continue
        if entry.name.lower() in backup_lower:
            # Already reorganized in a previous run.
            continue
        pairs.append(PairCandidate(
            parent=parent,
            name=entry.name,
            original=entry,
            h265=out_match,
        ))
    return pairs


def schedule_h264_delete(
    dropbox: DropboxClient,
    dir_path: str,
    delay_seconds: int,
) -> None:
    """
    Spawn a background daemon thread that sleeps `delay_seconds` and then
    deletes every file inside `dir_path` while keeping the folder itself.
    Works for any backup/quarantine subfolder — h264/, wav/, ponto tracinho/.

    A `<basename> deletado.txt` audit log is written INSIDE the same folder
    (so the parent stays clean) listing every file that was removed plus the
    deletion timestamp. Subsequent batches append a new section to the same
    file. Originals are recoverable via Dropbox version history.

    Errors are logged, never raised.
    """
    if delay_seconds <= 0:
        return

    folder_basename = PurePosixPath(dir_path).name
    audit_log_filename = f"{folder_basename} deletado.txt"
    audit_log_path = dir_path.rstrip('/') + '/' + audit_log_filename

    def _worker() -> None:
        time.sleep(delay_seconds)

        try:
            entries = list(dropbox.list_folder(dir_path, recursive=False))
        except Exception as e:
            logger.warning(f"reorganize: list {dir_path} for cleanup failed: {e}")
            return

        # Skip the audit log itself so we never delete our own paper trail.
        files_to_delete = [
            e for e in entries
            if e.name.lower() != audit_log_filename.lower()
        ]
        if not files_to_delete:
            logger.info(
                f"reorganize: {dir_path} already empty (or only audit log), "
                f"nothing to delete"
            )
            return

        # Write the log BEFORE deletion so the file list is captured even if
        # the per-file deletes fail halfway.
        try:
            _write_h264_deletion_log(
                dropbox, dir_path, audit_log_path,
                files_to_delete, delay_seconds, folder_basename,
            )
        except Exception as log_err:
            logger.warning(
                f"reorganize: failed to write deletion log {audit_log_path}: {log_err}"
            )

        deleted = 0
        for entry in files_to_delete:
            file_path = dir_path.rstrip('/') + '/' + entry.name
            try:
                dropbox.delete_file(file_path)
                deleted += 1
            except Exception as e:
                logger.warning(f"reorganize: delete {file_path} failed: {e}")

        logger.info(
            f"reorganize: cleaned {deleted}/{len(files_to_delete)} file(s) "
            f"inside {dir_path} after {delay_seconds}s "
            f"(folder kept; audit log at {audit_log_path}; "
            f"recoverable via Dropbox history)"
        )

    t = threading.Thread(
        target=_worker,
        name=f"backup-cleanup-{dir_path}",
        daemon=True,
    )
    t.start()


def _write_h264_deletion_log(
    dropbox: DropboxClient,
    dir_path: str,
    audit_log_path: str,
    files: list,
    delay_seconds: int,
    folder_basename: str,
) -> None:
    """Append a deletion record to the audit log inside `dir_path`. Receives
    the pre-computed file list so we don't list twice — the caller already
    excluded the audit log itself from the deletion set."""
    if not files:
        return

    files_sorted = sorted(files, key=lambda e: e.name.lower())
    total_bytes = sum(e.size for e in files_sorted)

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    name_width = max((len(e.name) for e in files_sorted), default=4)
    name_width = max(name_width, len("FILENAME"))

    lines = [
        "================================================================",
        f"DELETED AT: {now_str}",
        f"FOLDER:     {dir_path}",
        f"REASON:     scheduled cleanup after {delay_seconds}s",
        f"RECOVERY:   Dropbox version history (Plus 30d / Business 180d)",
        "",
        f"{'FILENAME'.ljust(name_width)}    SIZE",
    ]
    for e in files_sorted:
        lines.append(f"{e.name.ljust(name_width)}    {_format_size(e.size)}")
    lines.append("")
    lines.append(f"Total: {len(files_sorted)} file(s), {_format_size(total_bytes)} freed")
    lines.append("")
    lines.append("")
    new_section = "\n".join(lines)

    existing = dropbox.read_text_file(audit_log_path) or ""
    if not existing:
        # First time — add a header.
        header = (
            f"HeavyDrops Transcoder — {folder_basename}/ cleanup log\n"
            f"{'=' * (40 + len(folder_basename))}\n"
            "\n"
            f"Each section below records one batch cleanup of the {folder_basename}/\n"
            "folder. The folder itself is preserved; only the files inside are\n"
            "removed. Files are recoverable via Dropbox version history for the\n"
            "timeframe shown.\n"
            "\n"
            "\n"
        )
        existing = header
    elif not existing.endswith("\n"):
        existing += "\n"

    dropbox.write_text_file(audit_log_path, existing + new_section)


def _format_size(num_bytes: int) -> str:
    """Human-readable size (binary units)."""
    for unit in ('B', 'KB', 'MB', 'GB', 'TB'):
        if num_bytes < 1024 or unit == 'TB':
            if unit == 'B':
                return f"{num_bytes} {unit}"
            return f"{num_bytes:.2f} {unit}"
        num_bytes /= 1024
    return f"{num_bytes:.2f} PB"


# --------------------------------------------------------------------------
# ATEM cleanup: ._ macOS resource forks (4 KB Finder metadata files) that
# get scattered everywhere when ATEM writes media. We move them into a
# 'ponto tracinho' subfolder, then schedule deletion so Dropbox version
# history still has them in case anyone needs the metadata.
# --------------------------------------------------------------------------


PONTO_TRACINHO_SUBDIR = "ponto tracinho"


def sweep_dot_underscore_under_root(
    dropbox: DropboxClient,
    dropbox_root: str,
    delete_after_seconds: int,
    target_folder_names: list[str],
    max_size_bytes: int = 10240,
) -> dict[str, int]:
    """Walk `dropbox_root` recursively and clean ._ files in every folder
    whose name matches one of `target_folder_names` (case-insensitive).

    Designed to be called on a periodic schedule from the scan loop AND
    on demand from the dashboard ('Sweep now' button). Either way, this
    closes the gap where ._ files arrive AFTER the per-folder reorganize
    batch already ran — those files would otherwise stay orphaned because
    cleanup only fires at batch end.

    Returns {parent_path: count_quarantined} for every folder we touched.
    Empty dict if nothing was found. Errors per-folder are logged but
    don't abort the sweep.
    """
    targets_lower = {n.lower() for n in target_folder_names}
    seen_parents: set[str] = set()
    results: dict[str, int] = {}

    try:
        entries = list(dropbox.list_folder(dropbox_root, recursive=True))
    except Exception as e:
        logger.warning(f"sweep_dot_underscore: list root failed: {e}")
        return results

    # Group entries by parent so we only call cleanup once per matching folder.
    for entry in entries:
        parent = str(PurePosixPath(entry.path).parent)
        if parent in seen_parents:
            continue
        if PurePosixPath(parent).name.lower() not in targets_lower:
            continue
        seen_parents.add(parent)
        try:
            moved = cleanup_dot_underscore_files(
                dropbox,
                parent,
                delete_after_seconds,
                target_folder_names=target_folder_names,
                max_size_bytes=max_size_bytes,
            )
            if moved > 0:
                results[parent] = moved
        except Exception as e:
            logger.warning(f"sweep_dot_underscore: {parent} failed: {e}")

    return results


def cleanup_dot_underscore_files(
    dropbox: DropboxClient,
    parent: str,
    delete_after_seconds: int,
    target_folder_names: list[str] | None = None,
    max_size_bytes: int = 10240,
) -> int:
    """Sweep `._*` resource forks out of `parent` (non-recursive).

    Only runs when:
      (a) `parent`'s last segment matches one of `target_folder_names`
          (case-insensitive) — typically the ATEM "Video ISO Files" and
          "Audio Source Files" folders, the only places these resource
          forks belong; and
      (b) the file is smaller than `max_size_bytes` — real macOS
          resource forks are ~4 KB, the limit catches anomalies.

    Files matching both criteria are moved into
    `<parent>/ponto tracinho/<name>` and the subfolder is queued for
    cleanup via schedule_h264_delete (folder kept, files deleted, audit
    log left inside).

    Returns the number of files actually moved.
    """
    parent = parent.rstrip('/') if parent != '/' else ''

    if target_folder_names is None:
        target_folder_names = ["Video ISO Files", "Audio Source Files"]
    targets_lower = {name.lower() for name in target_folder_names}

    # Gate (a): must be one of the configured ATEM folders. Anything else
    # we leave alone, even if it has tiny ._ files in it.
    if PurePosixPath(parent).name.lower() not in targets_lower:
        return 0

    try:
        entries = list(dropbox.list_folder(parent or '/', recursive=False))
    except Exception as e:
        logger.warning(f"cleanup_dot_underscore: list {parent} failed: {e}")
        return 0

    targets = [
        e for e in entries
        if e.name.startswith('._') and e.size < max_size_bytes
    ]
    if not targets:
        return 0

    quarantine_dir = parent + '/' + PONTO_TRACINHO_SUBDIR
    try:
        dropbox.create_folder(quarantine_dir)
    except Exception as e:
        logger.warning(
            f"cleanup_dot_underscore: create {quarantine_dir} failed: {e}; "
            f"skipping cleanup for this folder"
        )
        return 0

    moved = 0
    for entry in targets:
        src = parent + '/' + entry.name
        dst = quarantine_dir + '/' + entry.name
        try:
            dropbox.move_file(src, dst, allow_overwrite=True)
            moved += 1
        except Exception as e:
            logger.warning(f"cleanup_dot_underscore: move {src} -> {dst} failed: {e}")

    if moved == 0:
        return 0

    logger.info(
        f"cleanup_dot_underscore: quarantined {moved} ._ file(s) "
        f"into {quarantine_dir}"
    )
    if delete_after_seconds > 0:
        schedule_h264_delete(dropbox, quarantine_dir, delete_after_seconds)

    return moved
