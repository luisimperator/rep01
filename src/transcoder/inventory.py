"""
Inventory scanner for complete Dropbox folder mapping.

Scans all files via API (without downloading) to build a complete
picture of transcoding work needed.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Generator

from .dropbox_client import DropboxClient, DropboxFileInfo
from .utils import (
    get_output_path,
    get_h265_log_path,
    is_in_h265_folder,
    is_partial_file,
    is_video_file,
    is_youtube_download,
    matches_exclude_pattern,
)

if TYPE_CHECKING:
    from .config import Config

logger = logging.getLogger(__name__)


class FileCategory(str, Enum):
    """Categories for inventory files."""
    NEEDS_TRANSCODING = "needs_transcoding"
    ALREADY_DONE = "already_done"
    SKIPPED_TOO_SMALL = "skipped_too_small"
    SKIPPED_YOUTUBE = "skipped_youtube"
    SKIPPED_EXCLUDED = "skipped_excluded"
    SKIPPED_IN_H265_FOLDER = "skipped_in_h265_folder"


@dataclass
class InventoryFile:
    """Information about a file in the inventory."""
    path: str
    size_bytes: int
    category: FileCategory
    output_exists: bool = False
    in_h265_log: bool = False

    @property
    def size_gb(self) -> float:
        return self.size_bytes / (1024 ** 3)


@dataclass
class InventoryStats:
    """Aggregated statistics from inventory scan."""
    total_files: int = 0
    total_size_bytes: int = 0

    # By category
    needs_transcoding_count: int = 0
    needs_transcoding_bytes: int = 0

    already_done_count: int = 0
    already_done_bytes: int = 0

    skipped_too_small_count: int = 0
    skipped_too_small_bytes: int = 0

    skipped_youtube_count: int = 0
    skipped_youtube_bytes: int = 0

    skipped_excluded_count: int = 0
    skipped_excluded_bytes: int = 0

    skipped_h265_folder_count: int = 0
    skipped_h265_folder_bytes: int = 0

    # Estimates
    estimated_output_bytes: int = 0  # Assuming 75% compression
    estimated_savings_bytes: int = 0

    @property
    def total_size_tb(self) -> float:
        return self.total_size_bytes / (1024 ** 4)

    @property
    def needs_transcoding_tb(self) -> float:
        return self.needs_transcoding_bytes / (1024 ** 4)

    @property
    def already_done_tb(self) -> float:
        return self.already_done_bytes / (1024 ** 4)

    @property
    def estimated_savings_tb(self) -> float:
        return self.estimated_savings_bytes / (1024 ** 4)

    @property
    def progress_percent(self) -> float:
        total_work = self.needs_transcoding_bytes + self.already_done_bytes
        if total_work == 0:
            return 100.0
        return (self.already_done_bytes / total_work) * 100

    def estimate_days_remaining(self, gb_per_hour: float = 50.0) -> float:
        """
        Estimate days to complete remaining work.

        Args:
            gb_per_hour: Processing speed in GB/hour (default 50 GB/h for good HW encoder)
        """
        remaining_gb = self.needs_transcoding_bytes / (1024 ** 3)
        hours = remaining_gb / gb_per_hour
        return hours / 24


@dataclass
class Inventory:
    """Complete inventory of Dropbox folder."""
    scan_time: str
    dropbox_root: str
    min_size_gb: float
    stats: InventoryStats
    files: list[InventoryFile] = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            'scan_time': self.scan_time,
            'dropbox_root': self.dropbox_root,
            'min_size_gb': self.min_size_gb,
            'stats': asdict(self.stats),
            'files': [
                {
                    'path': f.path,
                    'size_bytes': f.size_bytes,
                    'size_gb': round(f.size_gb, 2),
                    'category': f.category.value,
                    'output_exists': f.output_exists,
                    'in_h265_log': f.in_h265_log,
                }
                for f in self.files
            ]
        }

    def save(self, path: Path) -> None:
        """Save inventory to JSON file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(self.to_dict(), f, indent=2, ensure_ascii=False)

    @classmethod
    def load(cls, path: Path) -> Inventory:
        """Load inventory from JSON file."""
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        stats = InventoryStats(**data['stats'])
        files = [
            InventoryFile(
                path=f['path'],
                size_bytes=f['size_bytes'],
                category=FileCategory(f['category']),
                output_exists=f.get('output_exists', False),
                in_h265_log=f.get('in_h265_log', False),
            )
            for f in data.get('files', [])
        ]

        return cls(
            scan_time=data['scan_time'],
            dropbox_root=data['dropbox_root'],
            min_size_gb=data['min_size_gb'],
            stats=stats,
            files=files,
        )


class InventoryScanner:
    """
    Scans Dropbox folder to build complete inventory.

    Uses only API calls - no file downloads required.
    """

    # Assumed compression ratio for H.264 -> H.265
    COMPRESSION_RATIO = 0.25  # Output is ~25% of input (75% savings)

    def __init__(
        self,
        config: Config,
        dropbox_client: DropboxClient,
    ):
        self.config = config
        self.dropbox = dropbox_client
        self._h265_log_cache: dict[str, set[str]] = {}

    def scan(
        self,
        include_files: bool = True,
        progress_callback: callable = None,
    ) -> Inventory:
        """
        Perform full inventory scan.

        Args:
            include_files: If True, include individual file details in inventory.
            progress_callback: Optional callback(files_scanned, current_file_path)

        Returns:
            Complete inventory with stats and optionally file details.
        """
        stats = InventoryStats()
        files: list[InventoryFile] = []

        self._h265_log_cache.clear()

        logger.info(f"Starting inventory scan of {self.config.dropbox_root}")

        for file_info in self.dropbox.list_folder(
            self.config.dropbox_root,
            recursive=True,
        ):
            stats.total_files += 1
            stats.total_size_bytes += file_info.size

            if progress_callback:
                progress_callback(stats.total_files, file_info.path)

            # Categorize file
            inv_file = self._categorize_file(file_info)

            if inv_file is None:
                # Not a video file, skip
                continue

            # Update stats based on category
            self._update_stats(stats, inv_file)

            if include_files:
                files.append(inv_file)

        # Calculate estimates
        stats.estimated_output_bytes = int(
            stats.needs_transcoding_bytes * self.COMPRESSION_RATIO
        )
        stats.estimated_savings_bytes = (
            stats.needs_transcoding_bytes - stats.estimated_output_bytes
        )

        logger.info(
            f"Inventory complete: {stats.total_files} files, "
            f"{stats.needs_transcoding_count} need transcoding "
            f"({stats.needs_transcoding_tb:.2f} TB)"
        )

        return Inventory(
            scan_time=datetime.now(timezone.utc).isoformat(),
            dropbox_root=self.config.dropbox_root,
            min_size_gb=self.config.min_size_gb,
            stats=stats,
            files=files if include_files else [],
        )

    def _categorize_file(self, file_info: DropboxFileInfo) -> InventoryFile | None:
        """Categorize a single file."""
        path = file_info.path

        # Skip non-video files
        if not is_video_file(path, self.config.video_extensions):
            return None

        # Check if in h265 output folder
        if is_in_h265_folder(path):
            return InventoryFile(
                path=path,
                size_bytes=file_info.size,
                category=FileCategory.SKIPPED_IN_H265_FOLDER,
            )

        # Skip partial files
        if is_partial_file(path):
            return InventoryFile(
                path=path,
                size_bytes=file_info.size,
                category=FileCategory.SKIPPED_EXCLUDED,
            )

        # Check exclude patterns
        if matches_exclude_pattern(path, self.config.exclude_patterns):
            return InventoryFile(
                path=path,
                size_bytes=file_info.size,
                category=FileCategory.SKIPPED_EXCLUDED,
            )

        # Check if YouTube download
        if is_youtube_download(path):
            return InventoryFile(
                path=path,
                size_bytes=file_info.size,
                category=FileCategory.SKIPPED_YOUTUBE,
            )

        # Check minimum size
        min_bytes = self.config.min_size_bytes()
        if file_info.size < min_bytes:
            return InventoryFile(
                path=path,
                size_bytes=file_info.size,
                category=FileCategory.SKIPPED_TOO_SMALL,
            )

        # Check if already processed (output exists or in h265 log)
        output_path = get_output_path(path)
        output_exists = self.dropbox.file_exists(output_path)
        in_h265_log = self._is_in_h265_feito_log(file_info)

        if output_exists or in_h265_log:
            return InventoryFile(
                path=path,
                size_bytes=file_info.size,
                category=FileCategory.ALREADY_DONE,
                output_exists=output_exists,
                in_h265_log=in_h265_log,
            )

        # File needs transcoding
        return InventoryFile(
            path=path,
            size_bytes=file_info.size,
            category=FileCategory.NEEDS_TRANSCODING,
        )

    def _is_in_h265_feito_log(self, file_info: DropboxFileInfo) -> bool:
        """Check if file is in h265 feito.txt log (cached)."""
        log_path = get_h265_log_path(file_info.path)
        filename = PurePosixPath(file_info.path).name

        if log_path in self._h265_log_cache:
            return filename in self._h265_log_cache[log_path]

        log_content = self.dropbox.read_text_file(log_path)
        if log_content is None:
            self._h265_log_cache[log_path] = set()
            return False

        filenames = set()
        for line in log_content.splitlines():
            parts = line.split('|')
            if len(parts) >= 2:
                logged_filename = parts[1].strip()
                filenames.add(logged_filename)

        self._h265_log_cache[log_path] = filenames
        return filename in filenames

    def _update_stats(self, stats: InventoryStats, inv_file: InventoryFile) -> None:
        """Update aggregated stats based on file category."""
        cat = inv_file.category
        size = inv_file.size_bytes

        if cat == FileCategory.NEEDS_TRANSCODING:
            stats.needs_transcoding_count += 1
            stats.needs_transcoding_bytes += size
        elif cat == FileCategory.ALREADY_DONE:
            stats.already_done_count += 1
            stats.already_done_bytes += size
        elif cat == FileCategory.SKIPPED_TOO_SMALL:
            stats.skipped_too_small_count += 1
            stats.skipped_too_small_bytes += size
        elif cat == FileCategory.SKIPPED_YOUTUBE:
            stats.skipped_youtube_count += 1
            stats.skipped_youtube_bytes += size
        elif cat == FileCategory.SKIPPED_EXCLUDED:
            stats.skipped_excluded_count += 1
            stats.skipped_excluded_bytes += size
        elif cat == FileCategory.SKIPPED_IN_H265_FOLDER:
            stats.skipped_h265_folder_count += 1
            stats.skipped_h265_folder_bytes += size


def format_inventory_report(inventory: Inventory, gb_per_hour: float = 50.0) -> str:
    """
    Format inventory as a human-readable report.

    Args:
        inventory: The inventory to format.
        gb_per_hour: Processing speed for time estimates.

    Returns:
        Formatted report string.
    """
    s = inventory.stats
    lines = [
        "=" * 60,
        "DROPBOX VIDEO TRANSCODER - INVENTORY REPORT",
        "=" * 60,
        "",
        f"Scan time:     {inventory.scan_time}",
        f"Dropbox root:  {inventory.dropbox_root}",
        f"Min file size: {inventory.min_size_gb} GB",
        "",
        "-" * 60,
        "SUMMARY",
        "-" * 60,
        "",
        f"Total video files scanned:    {s.total_files:,}",
        f"Total size:                   {s.total_size_tb:.2f} TB",
        "",
        f"Progress:                     {s.progress_percent:.1f}%",
        "",
        "-" * 60,
        "BREAKDOWN",
        "-" * 60,
        "",
        f"Needs transcoding:    {s.needs_transcoding_count:>8,} files  ({s.needs_transcoding_tb:>7.2f} TB)",
        f"Already done:         {s.already_done_count:>8,} files  ({s.already_done_tb:>7.2f} TB)",
        "",
        "Skipped:",
        f"  - Too small:        {s.skipped_too_small_count:>8,} files  ({s.skipped_too_small_bytes / (1024**4):>7.2f} TB)",
        f"  - YouTube:          {s.skipped_youtube_count:>8,} files  ({s.skipped_youtube_bytes / (1024**4):>7.2f} TB)",
        f"  - Excluded:         {s.skipped_excluded_count:>8,} files  ({s.skipped_excluded_bytes / (1024**4):>7.2f} TB)",
        f"  - In h265 folder:   {s.skipped_h265_folder_count:>8,} files  ({s.skipped_h265_folder_bytes / (1024**4):>7.2f} TB)",
        "",
        "-" * 60,
        "ESTIMATES (assuming 75% compression, {:.0f} GB/hour)".format(gb_per_hour),
        "-" * 60,
        "",
        f"Estimated output size:        {s.estimated_output_bytes / (1024**4):.2f} TB",
        f"Estimated space savings:      {s.estimated_savings_tb:.2f} TB",
        f"Estimated days remaining:     {s.estimate_days_remaining(gb_per_hour):.1f} days",
        "",
        "=" * 60,
    ]

    return "\n".join(lines)


def format_top_files(
    inventory: Inventory,
    category: FileCategory = FileCategory.NEEDS_TRANSCODING,
    limit: int = 20,
) -> str:
    """
    Format a list of top files by size for a given category.

    Args:
        inventory: The inventory.
        category: Category to filter by.
        limit: Maximum files to show.

    Returns:
        Formatted list string.
    """
    files = [f for f in inventory.files if f.category == category]
    files.sort(key=lambda f: f.size_bytes, reverse=True)
    files = files[:limit]

    lines = [
        f"Top {len(files)} files - {category.value}:",
        "-" * 60,
    ]

    for i, f in enumerate(files, 1):
        lines.append(f"{i:>3}. {f.size_gb:>7.2f} GB  {f.path}")

    return "\n".join(lines)
