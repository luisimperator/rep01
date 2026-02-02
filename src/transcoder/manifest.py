"""
Global Cloud Manifest - Unified state storage in Dropbox.

Single manifest shared by all PCs on the same Dropbox account.
Tracks:
- All processed files (which PC processed each one)
- Daily progress history
- Overall statistics

Location: {dropbox_drive}:\HeavyDrops Dropbox\HeavyDrops\App h265 Converter\global_manifest.json
"""

import json
import socket
import os
import string
import threading
from dataclasses import dataclass, field, asdict
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Dict, List


def find_dropbox_path() -> Optional[Path]:
    """
    Auto-detect HeavyDrops Dropbox path by searching available drives.
    Works for Heavy1-Heavy6 (D:) and Heavy7 (C:).
    """
    # The folder structure we're looking for
    dropbox_folder = "HeavyDrops Dropbox"
    app_subfolder = Path("HeavyDrops") / "App h265 Converter"

    # Check common drives in order of preference
    drives_to_check = ['D', 'C', 'E', 'F', 'G', 'H']

    # Also check all available drive letters on Windows
    if os.name == 'nt':
        available_drives = []
        for letter in string.ascii_uppercase:
            drive_path = Path(f"{letter}:\\")
            if drive_path.exists():
                available_drives.append(letter)
        # Prioritize D and C, then add others
        drives_to_check = ['D', 'C'] + [d for d in available_drives if d not in ['D', 'C']]

    for drive in drives_to_check:
        dropbox_root = Path(f"{drive}:\\{dropbox_folder}")
        if dropbox_root.exists():
            full_path = dropbox_root / app_subfolder
            print(f"[Manifest] Found Dropbox at: {full_path}")
            return full_path

    # Fallback: try common locations
    fallback_paths = [
        Path(os.path.expanduser("~")) / "Dropbox" / "HeavyDrops" / "App h265 Converter",
        Path(os.path.expanduser("~")) / dropbox_folder / "HeavyDrops" / "App h265 Converter",
    ]

    for path in fallback_paths:
        if path.parent.exists():  # Check if HeavyDrops folder exists
            print(f"[Manifest] Found Dropbox at: {path}")
            return path

    return None


def get_pc_name() -> str:
    """Get unique identifier for this PC."""
    hostname = socket.gethostname()
    if '.' in hostname:
        hostname = hostname.split('.')[0]
    return hostname


@dataclass
class ProcessedFile:
    """Record of a processed file."""
    original_path: str
    output_path: str
    input_size_bytes: int
    output_size_bytes: int
    compression_ratio: float
    processed_at: str
    processed_by_pc: str  # Which PC processed this file
    encoder_used: str
    cq_value: int
    duration_seconds: float = 0
    transcode_seconds: float = 0


@dataclass
class DailyProgress:
    """Progress for a single day."""
    date: str  # YYYY-MM-DD
    files_processed: int = 0
    bytes_processed: int = 0
    bytes_saved: int = 0
    by_pc: Dict[str, int] = field(default_factory=dict)  # PC name -> files count


@dataclass
class GlobalStats:
    """Aggregated statistics across all PCs."""
    total_files_processed: int = 0
    total_input_bytes: int = 0
    total_output_bytes: int = 0
    total_saved_bytes: int = 0
    total_transcode_seconds: float = 0

    # Estimates (set by scanning)
    total_files_to_process: int = 0
    total_bytes_to_process: int = 0

    @property
    def total_saved_gb(self) -> float:
        return self.total_saved_bytes / (1024 ** 3)

    @property
    def total_saved_tb(self) -> float:
        return self.total_saved_bytes / (1024 ** 4)

    @property
    def total_input_tb(self) -> float:
        return self.total_input_bytes / (1024 ** 4)

    @property
    def total_to_process_tb(self) -> float:
        return self.total_bytes_to_process / (1024 ** 4)

    @property
    def progress_percent(self) -> float:
        total = self.total_input_bytes + self.total_bytes_to_process
        if total == 0:
            return 100.0
        return (self.total_input_bytes / total) * 100

    @property
    def avg_compression_ratio(self) -> float:
        if self.total_input_bytes == 0:
            return 0.25  # Default estimate
        return self.total_output_bytes / self.total_input_bytes

    @property
    def estimated_final_savings_tb(self) -> float:
        """Estimate total savings when all files are processed."""
        ratio = self.avg_compression_ratio if self.avg_compression_ratio > 0 else 0.25
        future_savings = self.total_bytes_to_process * (1 - ratio)
        return (self.total_saved_bytes + future_savings) / (1024 ** 4)

    @property
    def avg_speed_gbh(self) -> float:
        """Average processing speed in GB/hour."""
        if self.total_transcode_seconds == 0:
            return 50.0  # Default estimate
        hours = self.total_transcode_seconds / 3600
        gb = self.total_input_bytes / (1024 ** 3)
        return gb / hours if hours > 0 else 50.0

    @property
    def estimated_days_remaining(self) -> float:
        """Estimate days to complete remaining work."""
        if self.avg_speed_gbh == 0:
            return 0
        remaining_gb = self.total_bytes_to_process / (1024 ** 3)
        hours = remaining_gb / self.avg_speed_gbh
        return hours / 24


@dataclass
class SkippedFile:
    """Record of a file that doesn't need transcoding."""
    path: str
    reason: str  # already_h265, too_small, youtube, excluded, in_h265_folder
    size_bytes: int
    checked_at: str
    checked_by_pc: str


@dataclass
class GlobalManifest:
    """
    Global manifest shared by all PCs.
    """
    created_at: str
    last_updated: str
    last_updated_by: str  # PC name that last updated
    stats: GlobalStats = field(default_factory=GlobalStats)

    # All processed files (path -> ProcessedFile)
    processed_files: Dict[str, ProcessedFile] = field(default_factory=dict)

    # Skipped files that don't need transcoding (path -> SkippedFile)
    skipped_files: Dict[str, SkippedFile] = field(default_factory=dict)

    # Failed files (path -> error message)
    failed_files: Dict[str, str] = field(default_factory=dict)

    # Daily progress history (date -> DailyProgress)
    daily_history: Dict[str, DailyProgress] = field(default_factory=dict)

    # Active PCs (PC name -> last seen timestamp)
    active_pcs: Dict[str, str] = field(default_factory=dict)

    # Imported h265 feitos.txt files (path -> list of filenames)
    imported_h265_logs: Dict[str, str] = field(default_factory=dict)  # log_path -> last_import_time

    def is_processed(self, file_path: str) -> bool:
        normalized = self._normalize_path(file_path)
        return normalized in self.processed_files

    def is_skipped(self, file_path: str) -> bool:
        normalized = self._normalize_path(file_path)
        return normalized in self.skipped_files

    def is_failed(self, file_path: str) -> bool:
        normalized = self._normalize_path(file_path)
        return normalized in self.failed_files

    def get_skip_reason(self, file_path: str) -> Optional[str]:
        normalized = self._normalize_path(file_path)
        if normalized in self.skipped_files:
            return self.skipped_files[normalized].reason
        return None

    def add_skipped(self, file_path: str, reason: str, size_bytes: int, pc_name: str) -> None:
        """Record a file that doesn't need transcoding."""
        normalized = self._normalize_path(file_path)
        self.skipped_files[normalized] = SkippedFile(
            path=file_path,
            reason=reason,
            size_bytes=size_bytes,
            checked_at=datetime.now().isoformat(),
            checked_by_pc=pc_name,
        )
        self.last_updated = datetime.now().isoformat()
        self.last_updated_by = pc_name

    def add_processed(self, record: ProcessedFile, pc_name: str) -> None:
        """Record a successfully processed file."""
        normalized = self._normalize_path(record.original_path)
        record.processed_by_pc = pc_name
        self.processed_files[normalized] = record

        # Update stats
        self.stats.total_files_processed += 1
        self.stats.total_input_bytes += record.input_size_bytes
        self.stats.total_output_bytes += record.output_size_bytes
        self.stats.total_saved_bytes += (record.input_size_bytes - record.output_size_bytes)
        self.stats.total_transcode_seconds += record.transcode_seconds

        # Update daily progress
        today = date.today().isoformat()
        if today not in self.daily_history:
            self.daily_history[today] = DailyProgress(date=today)

        day = self.daily_history[today]
        day.files_processed += 1
        day.bytes_processed += record.input_size_bytes
        day.bytes_saved += (record.input_size_bytes - record.output_size_bytes)
        if pc_name not in day.by_pc:
            day.by_pc[pc_name] = 0
        day.by_pc[pc_name] += 1

        # Remove from failed if present
        if normalized in self.failed_files:
            del self.failed_files[normalized]

        self.last_updated = datetime.now().isoformat()
        self.last_updated_by = pc_name

    def add_failed(self, file_path: str, error: str, pc_name: str) -> None:
        normalized = self._normalize_path(file_path)
        self.failed_files[normalized] = f"{error} (by {pc_name})"
        self.last_updated = datetime.now().isoformat()
        self.last_updated_by = pc_name

    def reset_failed(self, file_path: Optional[str] = None) -> int:
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

    def update_estimates(self, total_files: int, total_bytes: int) -> None:
        """Update estimates from a scan."""
        self.stats.total_files_to_process = total_files
        self.stats.total_bytes_to_process = total_bytes

    def register_pc(self, pc_name: str) -> None:
        """Register this PC as active."""
        self.active_pcs[pc_name] = datetime.now().isoformat()

    def get_daily_progress(self, days: int = 7) -> List[DailyProgress]:
        """Get progress for last N days."""
        sorted_dates = sorted(self.daily_history.keys(), reverse=True)[:days]
        return [self.daily_history[d] for d in sorted_dates]

    def _normalize_path(self, path: str) -> str:
        return path.lower().replace('\\', '/')

    def to_dict(self) -> dict:
        return {
            'created_at': self.created_at,
            'last_updated': self.last_updated,
            'last_updated_by': self.last_updated_by,
            'stats': asdict(self.stats),
            'processed_files': {k: asdict(v) for k, v in self.processed_files.items()},
            'skipped_files': {k: asdict(v) for k, v in self.skipped_files.items()},
            'failed_files': self.failed_files,
            'daily_history': {k: asdict(v) for k, v in self.daily_history.items()},
            'active_pcs': self.active_pcs,
            'imported_h265_logs': self.imported_h265_logs,
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'GlobalManifest':
        stats = GlobalStats(**data.get('stats', {}))

        processed_files = {}
        for k, v in data.get('processed_files', {}).items():
            processed_files[k] = ProcessedFile(**v)

        skipped_files = {}
        for k, v in data.get('skipped_files', {}).items():
            skipped_files[k] = SkippedFile(**v)

        daily_history = {}
        for k, v in data.get('daily_history', {}).items():
            daily_history[k] = DailyProgress(**v)

        return cls(
            created_at=data['created_at'],
            last_updated=data['last_updated'],
            last_updated_by=data.get('last_updated_by', 'unknown'),
            stats=stats,
            processed_files=processed_files,
            skipped_files=skipped_files,
            failed_files=data.get('failed_files', {}),
            daily_history=daily_history,
            active_pcs=data.get('active_pcs', {}),
            imported_h265_logs=data.get('imported_h265_logs', {}),
        )


class GlobalManifestManager:
    """
    Manages the global manifest file with thread-safe operations.
    """

    MANIFEST_FILENAME = "global_manifest.json"

    def __init__(
        self,
        base_dropbox_path: Optional[str] = None,
        auto_save_interval: int = 3,
    ):
        # Auto-detect Dropbox path if not provided
        if base_dropbox_path:
            self.base_path = Path(base_dropbox_path)
        else:
            detected = find_dropbox_path()
            if detected:
                self.base_path = detected
            else:
                # Last resort fallback
                self.base_path = Path(r"D:\HeavyDrops Dropbox\HeavyDrops\App h265 Converter")
                print(f"[Manifest] WARNING: Could not detect Dropbox, using default: {self.base_path}")

        self.manifest_path = self.base_path / self.MANIFEST_FILENAME
        self.pc_name = get_pc_name()
        self.auto_save_interval = auto_save_interval
        self._unsaved_changes = 0
        self._lock = threading.Lock()

        self.manifest: GlobalManifest = self._load_or_create()
        self.manifest.register_pc(self.pc_name)

    def _load_or_create(self) -> GlobalManifest:
        """Load existing manifest or create new one."""
        if self.manifest_path.exists():
            try:
                with open(self.manifest_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                manifest = GlobalManifest.from_dict(data)
                print(f"[Manifest] Loaded: {manifest.stats.total_files_processed} files processed")
                return manifest
            except Exception as e:
                print(f"[Manifest] Error loading, creating new: {e}")

        now = datetime.now().isoformat()
        manifest = GlobalManifest(
            created_at=now,
            last_updated=now,
            last_updated_by=self.pc_name,
        )

        self.base_path.mkdir(parents=True, exist_ok=True)
        print(f"[Manifest] Created new global manifest")
        return manifest

    def refresh(self) -> GlobalManifest:
        """Reload manifest from disk (to get updates from other PCs)."""
        with self._lock:
            if self.manifest_path.exists():
                try:
                    with open(self.manifest_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    self.manifest = GlobalManifest.from_dict(data)
                    self.manifest.register_pc(self.pc_name)
                except Exception as e:
                    print(f"[Manifest] Refresh error: {e}")
        return self.manifest

    def save(self, force: bool = False) -> None:
        """Save manifest to disk."""
        with self._lock:
            self._unsaved_changes += 1

            if not force and self._unsaved_changes < self.auto_save_interval:
                return

            try:
                self.base_path.mkdir(parents=True, exist_ok=True)

                temp_path = self.manifest_path.with_suffix('.tmp')
                with open(temp_path, 'w', encoding='utf-8') as f:
                    json.dump(self.manifest.to_dict(), f, indent=2, ensure_ascii=False)

                temp_path.replace(self.manifest_path)
                self._unsaved_changes = 0
            except Exception as e:
                print(f"[Manifest] Save error: {e}")

    def is_processed(self, file_path: str) -> bool:
        return self.manifest.is_processed(file_path)

    def is_skipped(self, file_path: str) -> bool:
        return self.manifest.is_skipped(file_path)

    def is_failed(self, file_path: str) -> bool:
        return self.manifest.is_failed(file_path)

    def should_process(self, file_path: str) -> bool:
        """Check if file needs processing (not processed, not skipped, not failed)."""
        return (not self.is_processed(file_path) and
                not self.is_skipped(file_path) and
                not self.is_failed(file_path))

    def get_skip_reason(self, file_path: str) -> Optional[str]:
        return self.manifest.get_skip_reason(file_path)

    def record_skipped(self, file_path: str, reason: str, size_bytes: int = 0) -> None:
        """Record a file that doesn't need transcoding."""
        self.manifest.add_skipped(file_path, reason, size_bytes, self.pc_name)
        self.save()

    def record_success(
        self,
        original_path: str,
        output_path: str,
        input_size: int,
        output_size: int,
        encoder: str,
        cq_value: int,
        duration: float = 0,
        transcode_time: float = 0,
    ) -> None:
        record = ProcessedFile(
            original_path=original_path,
            output_path=output_path,
            input_size_bytes=input_size,
            output_size_bytes=output_size,
            compression_ratio=output_size / input_size if input_size > 0 else 0,
            processed_at=datetime.now().isoformat(),
            processed_by_pc=self.pc_name,
            encoder_used=encoder,
            cq_value=cq_value,
            duration_seconds=duration,
            transcode_seconds=transcode_time,
        )
        self.manifest.add_processed(record, self.pc_name)
        self.save()

    def record_failure(self, file_path: str, error: str) -> None:
        self.manifest.add_failed(file_path, error, self.pc_name)
        self.save(force=True)

    def reset_failed(self, file_path: Optional[str] = None) -> int:
        count = self.manifest.reset_failed(file_path)
        self.save(force=True)
        return count

    def update_estimates(self, total_files: int, total_bytes: int) -> None:
        """Update work estimates from a scan."""
        self.manifest.update_estimates(total_files, total_bytes)
        self.save(force=True)

    def get_dashboard_data(self) -> dict:
        """Get data for dashboard display."""
        s = self.manifest.stats
        return {
            'pc_name': self.pc_name,
            'last_updated': self.manifest.last_updated,
            'last_updated_by': self.manifest.last_updated_by,
            'active_pcs': list(self.manifest.active_pcs.keys()),

            # Progress
            'total_processed': s.total_files_processed,
            'total_to_process': s.total_files_to_process,
            'progress_percent': s.progress_percent,

            # Sizes
            'processed_tb': s.total_input_tb,
            'to_process_tb': s.total_to_process_tb,
            'saved_tb': s.total_saved_tb,
            'estimated_total_savings_tb': s.estimated_final_savings_tb,

            # Performance
            'avg_compression': (1 - s.avg_compression_ratio) * 100,
            'avg_speed_gbh': s.avg_speed_gbh,
            'days_remaining': s.estimated_days_remaining,

            # Daily history
            'daily_progress': [
                {
                    'date': d.date,
                    'files': d.files_processed,
                    'gb_processed': d.bytes_processed / (1024**3),
                    'gb_saved': d.bytes_saved / (1024**3),
                    'by_pc': d.by_pc,
                }
                for d in self.manifest.get_daily_progress(14)
            ],

            # Failures and skipped
            'failed_count': len(self.manifest.failed_files),
            'skipped_count': len(self.manifest.skipped_files),
        }

    def import_h265_feitos_txt(self, log_path: str, content: str) -> int:
        """
        Import entries from h265 feitos.txt into manifest as processed files.

        Args:
            log_path: Path to the h265 feitos.txt file
            content: Content of the file

        Returns:
            Number of entries imported
        """
        if log_path in self.manifest.imported_h265_logs:
            # Already imported this file
            return 0

        imported = 0
        for line in content.splitlines():
            line = line.strip()
            if not line:
                continue

            # Format: date|filename|input_size|output_size|ratio
            parts = line.split('|')
            if len(parts) >= 4:
                try:
                    filename = parts[1].strip()
                    input_size = int(parts[2].strip()) if parts[2].strip().isdigit() else 0
                    output_size = int(parts[3].strip()) if parts[3].strip().isdigit() else 0

                    # Create a processed file record
                    record = ProcessedFile(
                        original_path=filename,  # Just filename, full path unknown
                        output_path="",
                        input_size_bytes=input_size,
                        output_size_bytes=output_size,
                        compression_ratio=output_size / input_size if input_size > 0 else 0.25,
                        processed_at=parts[0].strip() if parts[0] else datetime.now().isoformat(),
                        processed_by_pc="imported",
                        encoder_used="unknown",
                        cq_value=0,
                    )

                    # Use filename as key (normalized)
                    normalized = filename.lower()
                    if normalized not in self.manifest.processed_files:
                        self.manifest.processed_files[normalized] = record
                        self.manifest.stats.total_files_processed += 1
                        self.manifest.stats.total_input_bytes += input_size
                        self.manifest.stats.total_output_bytes += output_size
                        self.manifest.stats.total_saved_bytes += (input_size - output_size)
                        imported += 1
                except (ValueError, IndexError):
                    continue

        # Mark as imported
        self.manifest.imported_h265_logs[log_path] = datetime.now().isoformat()
        self.save(force=True)

        print(f"[Manifest] Imported {imported} entries from {log_path}")
        return imported

    def get_stats_summary(self) -> dict:
        """Get a quick summary of manifest stats."""
        return {
            'processed': len(self.manifest.processed_files),
            'skipped': len(self.manifest.skipped_files),
            'failed': len(self.manifest.failed_files),
            'total_tb': self.manifest.stats.total_input_tb,
            'saved_tb': self.manifest.stats.total_saved_tb,
        }

    def get_manifest_path(self) -> Path:
        return self.manifest_path

    def close(self) -> None:
        self.save(force=True)


# Backward compatibility alias
ManifestManager = GlobalManifestManager
