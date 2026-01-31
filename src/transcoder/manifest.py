"""
Global Cloud Manifest - Unified state storage in Dropbox.

Single manifest shared by all PCs on the same Dropbox account.
Tracks:
- All processed files (which PC processed each one)
- Daily progress history
- Overall statistics

Location: D:\HeavyDrops Dropbox\HeavyDrops\App h265 Converter\global_manifest.json
"""

import json
import socket
import os
import threading
from dataclasses import dataclass, field, asdict
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Dict, List


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

    # Failed files (path -> error message)
    failed_files: Dict[str, str] = field(default_factory=dict)

    # Daily progress history (date -> DailyProgress)
    daily_history: Dict[str, DailyProgress] = field(default_factory=dict)

    # Active PCs (PC name -> last seen timestamp)
    active_pcs: Dict[str, str] = field(default_factory=dict)

    def is_processed(self, file_path: str) -> bool:
        normalized = self._normalize_path(file_path)
        return normalized in self.processed_files

    def is_failed(self, file_path: str) -> bool:
        normalized = self._normalize_path(file_path)
        return normalized in self.failed_files

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
            'failed_files': self.failed_files,
            'daily_history': {k: asdict(v) for k, v in self.daily_history.items()},
            'active_pcs': self.active_pcs,
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'GlobalManifest':
        stats = GlobalStats(**data.get('stats', {}))

        processed_files = {}
        for k, v in data.get('processed_files', {}).items():
            processed_files[k] = ProcessedFile(**v)

        daily_history = {}
        for k, v in data.get('daily_history', {}).items():
            daily_history[k] = DailyProgress(**v)

        return cls(
            created_at=data['created_at'],
            last_updated=data['last_updated'],
            last_updated_by=data.get('last_updated_by', 'unknown'),
            stats=stats,
            processed_files=processed_files,
            failed_files=data.get('failed_files', {}),
            daily_history=daily_history,
            active_pcs=data.get('active_pcs', {}),
        )


class GlobalManifestManager:
    """
    Manages the global manifest file with thread-safe operations.
    """

    MANIFEST_FILENAME = "global_manifest.json"

    def __init__(
        self,
        base_dropbox_path: str = r"D:\HeavyDrops Dropbox\HeavyDrops\App h265 Converter",
        auto_save_interval: int = 3,
    ):
        self.base_path = Path(base_dropbox_path)
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

    def is_failed(self, file_path: str) -> bool:
        return self.manifest.is_failed(file_path)

    def should_process(self, file_path: str) -> bool:
        return not self.is_processed(file_path) and not self.is_failed(file_path)

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

            # Failures
            'failed_count': len(self.manifest.failed_files),
        }

    def get_manifest_path(self) -> Path:
        return self.manifest_path

    def close(self) -> None:
        self.save(force=True)


# Backward compatibility alias
ManifestManager = GlobalManifestManager
