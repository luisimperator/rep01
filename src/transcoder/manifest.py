"""
Cloud Manifest - Persistent state storage in Dropbox.

Saves processing state to Dropbox so it persists across:
- SSD wipes ("Make online-only")
- PC restarts
- Multiple machines on the same Dropbox account

Each PC gets its own manifest folder based on hostname to avoid conflicts.
"""

import json
import socket
import os
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional


def get_pc_name() -> str:
    """Get unique identifier for this PC."""
    # Try hostname first
    hostname = socket.gethostname()

    # Clean up hostname (remove domain if present)
    if '.' in hostname:
        hostname = hostname.split('.')[0]

    return hostname


def get_manifest_dir(base_dropbox_path: str = r"D:\HeavyDrops Dropbox\HeavyDrops\App h265 Converter") -> Path:
    """
    Get the manifest directory for this PC.

    Returns path like: D:\HeavyDrops Dropbox\HeavyDrops\App h265 Converter\PC-GAMING\
    """
    pc_name = get_pc_name()
    return Path(base_dropbox_path) / pc_name


@dataclass
class ProcessedFile:
    """Record of a processed file."""
    original_path: str           # Original path before processing
    output_path: str             # Path to h265 output
    input_size_bytes: int        # Original file size
    output_size_bytes: int       # Compressed file size
    compression_ratio: float     # output/input ratio
    processed_at: str            # ISO timestamp
    encoder_used: str            # nvenc, qsv, cpu
    cq_value: int                # Quality setting used
    duration_seconds: float      # Video duration
    transcode_seconds: float     # How long transcoding took


@dataclass
class ManifestStats:
    """Aggregated statistics."""
    total_files_processed: int = 0
    total_input_bytes: int = 0
    total_output_bytes: int = 0
    total_saved_bytes: int = 0
    total_transcode_seconds: float = 0

    # Session stats (reset each run)
    session_files: int = 0
    session_input_bytes: int = 0
    session_output_bytes: int = 0
    session_start: str = ""

    @property
    def total_saved_gb(self) -> float:
        return self.total_saved_bytes / (1024 ** 3)

    @property
    def total_input_tb(self) -> float:
        return self.total_input_bytes / (1024 ** 4)

    @property
    def avg_compression_ratio(self) -> float:
        if self.total_input_bytes == 0:
            return 0
        return self.total_output_bytes / self.total_input_bytes

    @property
    def avg_transcode_speed_gbh(self) -> float:
        """Average processing speed in GB/hour."""
        if self.total_transcode_seconds == 0:
            return 0
        hours = self.total_transcode_seconds / 3600
        gb = self.total_input_bytes / (1024 ** 3)
        return gb / hours


@dataclass
class CloudManifest:
    """
    Persistent manifest stored in Dropbox.

    Tracks all processed files so we don't re-process them
    even after clearing local storage.
    """
    pc_name: str
    created_at: str
    last_updated: str
    stats: ManifestStats = field(default_factory=ManifestStats)

    # Map of original_path -> ProcessedFile
    # Using dict for O(1) lookup
    processed_files: dict[str, ProcessedFile] = field(default_factory=dict)

    # Files that failed processing (path -> error message)
    failed_files: dict[str, str] = field(default_factory=dict)

    # Files currently being processed (for crash recovery)
    in_progress: list[str] = field(default_factory=list)

    def is_processed(self, file_path: str) -> bool:
        """Check if a file has already been processed."""
        # Normalize path for comparison
        normalized = self._normalize_path(file_path)
        return normalized in self.processed_files

    def is_failed(self, file_path: str) -> bool:
        """Check if a file previously failed."""
        normalized = self._normalize_path(file_path)
        return normalized in self.failed_files

    def add_processed(self, record: ProcessedFile) -> None:
        """Record a successfully processed file."""
        normalized = self._normalize_path(record.original_path)
        self.processed_files[normalized] = record

        # Update stats
        self.stats.total_files_processed += 1
        self.stats.total_input_bytes += record.input_size_bytes
        self.stats.total_output_bytes += record.output_size_bytes
        self.stats.total_saved_bytes += (record.input_size_bytes - record.output_size_bytes)
        self.stats.total_transcode_seconds += record.transcode_seconds

        # Session stats
        self.stats.session_files += 1
        self.stats.session_input_bytes += record.input_size_bytes
        self.stats.session_output_bytes += record.output_size_bytes

        # Remove from failed if it was there
        if normalized in self.failed_files:
            del self.failed_files[normalized]

        # Remove from in_progress
        if normalized in self.in_progress:
            self.in_progress.remove(normalized)

        self.last_updated = datetime.now().isoformat()

    def add_failed(self, file_path: str, error: str) -> None:
        """Record a failed file."""
        normalized = self._normalize_path(file_path)
        self.failed_files[normalized] = error

        # Remove from in_progress
        if normalized in self.in_progress:
            self.in_progress.remove(normalized)

        self.last_updated = datetime.now().isoformat()

    def mark_in_progress(self, file_path: str) -> None:
        """Mark a file as currently being processed."""
        normalized = self._normalize_path(file_path)
        if normalized not in self.in_progress:
            self.in_progress.append(normalized)
        self.last_updated = datetime.now().isoformat()

    def clear_in_progress(self, file_path: str) -> None:
        """Remove file from in-progress list."""
        normalized = self._normalize_path(file_path)
        if normalized in self.in_progress:
            self.in_progress.remove(normalized)

    def reset_failed(self, file_path: Optional[str] = None) -> int:
        """
        Reset failed files to allow retry.

        Args:
            file_path: Specific file to reset, or None to reset all.

        Returns:
            Number of files reset.
        """
        if file_path:
            normalized = self._normalize_path(file_path)
            if normalized in self.failed_files:
                del self.failed_files[normalized]
                return 1
            return 0
        else:
            count = len(self.failed_files)
            self.failed_files.clear()
            return count

    def start_session(self) -> None:
        """Start a new processing session."""
        self.stats.session_files = 0
        self.stats.session_input_bytes = 0
        self.stats.session_output_bytes = 0
        self.stats.session_start = datetime.now().isoformat()

    def _normalize_path(self, path: str) -> str:
        """Normalize path for consistent comparison."""
        # Convert to lowercase, use forward slashes
        return path.lower().replace('\\', '/')

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            'pc_name': self.pc_name,
            'created_at': self.created_at,
            'last_updated': self.last_updated,
            'stats': asdict(self.stats),
            'processed_files': {
                k: asdict(v) for k, v in self.processed_files.items()
            },
            'failed_files': self.failed_files,
            'in_progress': self.in_progress,
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'CloudManifest':
        """Create from dictionary."""
        stats = ManifestStats(**data.get('stats', {}))

        processed_files = {}
        for k, v in data.get('processed_files', {}).items():
            processed_files[k] = ProcessedFile(**v)

        return cls(
            pc_name=data['pc_name'],
            created_at=data['created_at'],
            last_updated=data['last_updated'],
            stats=stats,
            processed_files=processed_files,
            failed_files=data.get('failed_files', {}),
            in_progress=data.get('in_progress', []),
        )


class ManifestManager:
    """
    Manages the cloud manifest file.

    Handles loading, saving, and auto-save functionality.
    """

    MANIFEST_FILENAME = "manifest.json"

    def __init__(
        self,
        base_dropbox_path: str = r"D:\HeavyDrops Dropbox\HeavyDrops\App h265 Converter",
        auto_save_interval: int = 5,  # Save every N processed files
    ):
        self.base_path = Path(base_dropbox_path)
        self.pc_name = get_pc_name()
        self.manifest_dir = self.base_path / self.pc_name
        self.manifest_path = self.manifest_dir / self.MANIFEST_FILENAME
        self.auto_save_interval = auto_save_interval
        self._unsaved_changes = 0

        self.manifest: CloudManifest = self._load_or_create()

    def _load_or_create(self) -> CloudManifest:
        """Load existing manifest or create new one."""
        if self.manifest_path.exists():
            try:
                with open(self.manifest_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                manifest = CloudManifest.from_dict(data)
                print(f"[Manifest] Loaded: {len(manifest.processed_files)} processed files")
                return manifest
            except Exception as e:
                print(f"[Manifest] Error loading, creating new: {e}")

        # Create new manifest
        now = datetime.now().isoformat()
        manifest = CloudManifest(
            pc_name=self.pc_name,
            created_at=now,
            last_updated=now,
        )

        # Ensure directory exists
        self.manifest_dir.mkdir(parents=True, exist_ok=True)

        print(f"[Manifest] Created new manifest for PC: {self.pc_name}")
        return manifest

    def save(self, force: bool = False) -> None:
        """
        Save manifest to disk.

        Args:
            force: If True, save immediately. Otherwise, use auto-save interval.
        """
        self._unsaved_changes += 1

        if not force and self._unsaved_changes < self.auto_save_interval:
            return

        try:
            # Ensure directory exists
            self.manifest_dir.mkdir(parents=True, exist_ok=True)

            # Write to temp file first, then rename (atomic on most systems)
            temp_path = self.manifest_path.with_suffix('.tmp')
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(self.manifest.to_dict(), f, indent=2, ensure_ascii=False)

            # Rename temp to final
            temp_path.replace(self.manifest_path)

            self._unsaved_changes = 0
        except Exception as e:
            print(f"[Manifest] Error saving: {e}")

    def is_processed(self, file_path: str) -> bool:
        """Check if file was already processed."""
        return self.manifest.is_processed(file_path)

    def is_failed(self, file_path: str) -> bool:
        """Check if file previously failed."""
        return self.manifest.is_failed(file_path)

    def should_process(self, file_path: str) -> bool:
        """Check if file should be processed (not done, not failed)."""
        return not self.is_processed(file_path) and not self.is_failed(file_path)

    def record_success(
        self,
        original_path: str,
        output_path: str,
        input_size: int,
        output_size: int,
        encoder: str,
        cq_value: int,
        duration: float,
        transcode_time: float,
    ) -> None:
        """Record a successfully processed file."""
        record = ProcessedFile(
            original_path=original_path,
            output_path=output_path,
            input_size_bytes=input_size,
            output_size_bytes=output_size,
            compression_ratio=output_size / input_size if input_size > 0 else 0,
            processed_at=datetime.now().isoformat(),
            encoder_used=encoder,
            cq_value=cq_value,
            duration_seconds=duration,
            transcode_seconds=transcode_time,
        )
        self.manifest.add_processed(record)
        self.save()

    def record_failure(self, file_path: str, error: str) -> None:
        """Record a failed file."""
        self.manifest.add_failed(file_path, error)
        self.save(force=True)

    def mark_in_progress(self, file_path: str) -> None:
        """Mark file as being processed."""
        self.manifest.mark_in_progress(file_path)
        self.save()

    def reset_failed(self, file_path: Optional[str] = None) -> int:
        """Reset failed files."""
        count = self.manifest.reset_failed(file_path)
        self.save(force=True)
        return count

    def start_session(self) -> None:
        """Start a new processing session."""
        self.manifest.start_session()
        self.save(force=True)

    def get_stats_summary(self) -> str:
        """Get human-readable stats summary."""
        s = self.manifest.stats
        lines = [
            f"=== Manifest Stats ({self.pc_name}) ===",
            f"Total processed: {s.total_files_processed:,} files",
            f"Total input:     {s.total_input_tb:.2f} TB",
            f"Total saved:     {s.total_saved_gb:,.1f} GB",
            f"Avg compression: {s.avg_compression_ratio:.1%}",
            f"Avg speed:       {s.avg_transcode_speed_gbh:.1f} GB/hour",
            f"",
            f"Session: {s.session_files} files ({s.session_input_bytes / (1024**3):.1f} GB)",
            f"Failed:  {len(self.manifest.failed_files)} files",
        ]
        return "\n".join(lines)

    def get_manifest_path(self) -> Path:
        """Get path to manifest file."""
        return self.manifest_path

    def close(self) -> None:
        """Save and close."""
        self.save(force=True)


# Convenience function for quick checks
def quick_is_processed(
    file_path: str,
    base_dropbox_path: str = r"D:\HeavyDrops Dropbox\HeavyDrops\App h265 Converter",
) -> bool:
    """
    Quick check if a file was processed (loads manifest each time).

    For batch operations, use ManifestManager instead.
    """
    manager = ManifestManager(base_dropbox_path)
    return manager.is_processed(file_path)
