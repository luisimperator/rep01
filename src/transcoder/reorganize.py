"""
Post-upload Dropbox reorganization helpers.

Replicates the legacy GUI's "swap H.265 into the original's spot, back the
H.264 up under h264/, journal it in h265 feito.txt" workflow. Used both by
the live UploadWorker and by the `hd reorganize-existing` retroactive sweep.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import PurePosixPath
from typing import Iterable

from .dropbox_client import DropboxClient, DropboxFileInfo

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


def reorganize_pair(
    dropbox: DropboxClient,
    parent: str,
    name: str,
    original_size: int,
    h265_size: int,
) -> str:
    """
    Atomically swap the H.265 into the original's spot.

    Steps:
      1. Ensure /<parent>/h264/ exists.
      2. Move /<parent>/<name> (the H.264 original) to /<parent>/h264/<name>.
      3. Move /<parent>/h265/<name> (the H.265) to /<parent>/<name>.
         On failure, rolls step 2 back so the original stays at its
         canonical path.
      4. Append a line to /<parent>/h265/h265 feito.txt in the legacy
         GUI's format.

    Returns the final Dropbox path of the H.265 file (== /<parent>/<name>).
    Raises on unrecoverable failure.
    """
    parent = parent.rstrip('/') if parent != '/' else ''
    h264_dir = parent + '/h264'
    h264_backup = h264_dir + '/' + name
    h265_temp = parent + '/h265/' + name
    original = parent + '/' + name
    feito_path = parent + '/h265/h265 feito.txt'

    logger.info(f"reorganize: backing original up to {h264_backup}")
    dropbox.create_folder(h264_dir)
    dropbox.move_file(original, h264_backup, allow_overwrite=True)

    try:
        logger.info(f"reorganize: swapping H.265 into {original}")
        dropbox.move_file(h265_temp, original, allow_overwrite=False)
    except Exception:
        logger.warning(
            f"reorganize: swap failed; rolling back backup "
            f"from {h264_backup} to {original}"
        )
        try:
            dropbox.move_file(h264_backup, original, allow_overwrite=False)
        except Exception as rollback_err:
            logger.error(
                f"reorganize: rollback also failed: {rollback_err}. "
                f"Original is at {h264_backup}; H.265 at {h265_temp}."
            )
        raise

    try:
        _append_feito_log(dropbox, feito_path, name, original_size, h265_size)
    except Exception as log_err:
        logger.warning(f"reorganize: feito.txt append failed: {log_err}")

    logger.info(
        f"reorganize complete: original at {h264_backup}, H.265 at {original}"
    )
    return original


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
) -> list[FolderCandidate]:
    """
    Walk `dropbox_root` recursively and find folders with H.265 outputs that
    haven't been reorganized into the legacy layout yet.

    A folder is a candidate when it contains BOTH:
      - /<parent>/<name>           (original, presumed H.264)
      - /<parent>/h265/<name>      (H.265 output)
    AND does NOT already contain:
      - /<parent>/h264/<name>      (would mean already reorganized)

    The case of the subfolder names is normalized when matching, but the
    actual move uses the path Dropbox returned.
    """
    # Index every file by its parent directory and by lowercase basename.
    # Keys are case-insensitive parent paths so we can match h265/ siblings.
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

    for parent, files in by_parent.items():
        # Skip subfolders themselves
        last = PurePosixPath(parent).name.lower()
        if last in ('h264', 'h265'):
            continue
        if parent in seen_parents:
            continue
        seen_parents.add(parent)

        h265_files = find_subfolder_files(parent, 'h265')
        if not h265_files:
            continue
        h264_files = find_subfolder_files(parent, 'h264')

        pairs: list[PairCandidate] = []
        for name, original in files.items():
            if name.lower() == 'h265 feito.txt':
                continue
            # Match by case-insensitive name lookup in h265/
            h265_match = None
            for h265_name, h265_info in h265_files.items():
                if h265_name.lower() == name.lower():
                    h265_match = h265_info
                    break
            if h265_match is None:
                continue
            # Already-reorganized check: if a file with the same name exists
            # under h264/, the swap was already done — skip the pair.
            already = any(n.lower() == name.lower() for n in h264_files)
            if already:
                continue
            pairs.append(PairCandidate(
                parent=parent,
                name=name,
                original=original,
                h265=h265_match,
            ))

        if pairs:
            candidates.append(FolderCandidate(parent=parent, pairs=pairs))

    return candidates
