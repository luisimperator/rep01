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
import time
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
    Per-PC manifest manager — each PC writes its own file, reads all for merged view.
    Eliminates race conditions from concurrent writes to a shared JSON file.

    Storage: {base_path}/manifests/{pc_name}.json
    Legacy:  {base_path}/global_manifest.json (read-only fallback during migration)
    """

    MANIFESTS_DIR = "manifests"
    LEGACY_MANIFEST = "global_manifest.json"

    def __init__(
        self,
        base_dropbox_path: Optional[str] = None,
        auto_save_interval: int = 3,
    ):
        if base_dropbox_path:
            self.base_path = Path(base_dropbox_path)
        else:
            detected = find_dropbox_path()
            if detected:
                self.base_path = detected
            else:
                self.base_path = Path(r"D:\HeavyDrops Dropbox\HeavyDrops\App h265 Converter")
                print(f"[Manifest] WARNING: Could not detect Dropbox, using default: {self.base_path}")

        self.manifests_dir = self.base_path / self.MANIFESTS_DIR
        self.pc_name = get_pc_name()
        self.my_manifest_path = self.manifests_dir / f"{self.pc_name}.json"
        self.auto_save_interval = auto_save_interval
        self._unsaved_changes = 0
        self._lock = threading.Lock()

        self.manifests_dir.mkdir(parents=True, exist_ok=True)
        self._migrate_legacy_manifest()
        self.manifest: GlobalManifest = self._load_or_create()
        self.manifest.register_pc(self.pc_name)

        # Merged cache for cross-PC lookups
        self._merged_processed: set = set()
        self._merged_skipped: set = set()
        self._merged_failed: set = set()
        self._last_merge_time: float = 0
        self._merge_interval: int = 60
        self._refresh_merged_cache()

    def _migrate_legacy_manifest(self) -> None:
        """Migrate from global_manifest.json to per-PC manifest."""
        legacy_path = self.base_path / self.LEGACY_MANIFEST
        if not legacy_path.exists() or self.my_manifest_path.exists():
            return
        try:
            with open(legacy_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            legacy = GlobalManifest.from_dict(data)
            # Filter to only this PC's entries
            my_processed = {k: v for k, v in legacy.processed_files.items()
                           if v.processed_by_pc in (self.pc_name, 'imported')}
            my_skipped = {k: v for k, v in legacy.skipped_files.items()
                         if v.checked_by_pc == self.pc_name}
            my_failed = {k: v for k, v in legacy.failed_files.items()
                        if f'(by {self.pc_name})' in str(v)}
            my_daily = {}
            for dk, dv in legacy.daily_history.items():
                if self.pc_name in dv.by_pc:
                    my_count = dv.by_pc[self.pc_name]
                    ratio = my_count / dv.files_processed if dv.files_processed > 0 else 0
                    my_daily[dk] = DailyProgress(
                        date=dk, files_processed=my_count,
                        bytes_processed=int(dv.bytes_processed * ratio),
                        bytes_saved=int(dv.bytes_saved * ratio),
                        by_pc={self.pc_name: my_count})
            now = datetime.now().isoformat()
            my_manifest = GlobalManifest(
                created_at=now, last_updated=now, last_updated_by=self.pc_name,
                stats=GlobalStats(
                    total_files_processed=len(my_processed),
                    total_input_bytes=sum(r.input_size_bytes for r in my_processed.values()),
                    total_output_bytes=sum(r.output_size_bytes for r in my_processed.values()),
                    total_saved_bytes=sum(r.input_size_bytes - r.output_size_bytes for r in my_processed.values()),
                    total_transcode_seconds=sum(r.transcode_seconds for r in my_processed.values()),
                    total_files_to_process=legacy.stats.total_files_to_process,
                    total_bytes_to_process=legacy.stats.total_bytes_to_process,
                ),
                processed_files=my_processed, skipped_files=my_skipped,
                failed_files=my_failed, daily_history=my_daily,
                active_pcs={self.pc_name: now},
                imported_h265_logs=dict(legacy.imported_h265_logs),
            )
            with open(self.my_manifest_path, 'w', encoding='utf-8') as f:
                json.dump(my_manifest.to_dict(), f, indent=2, ensure_ascii=False)
            print(f"[Manifest] Migrated {self.pc_name} data from legacy manifest")
        except Exception as e:
            print(f"[Manifest] Migration error: {e}")

    def _load_or_create(self) -> GlobalManifest:
        """Load this PC's manifest or create new one."""
        if self.my_manifest_path.exists():
            try:
                with open(self.my_manifest_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                manifest = GlobalManifest.from_dict(data)
                print(f"[Manifest] Loaded {self.pc_name}: {manifest.stats.total_files_processed} files")
                return manifest
            except Exception as e:
                print(f"[Manifest] Error loading, creating new: {e}")
        now = datetime.now().isoformat()
        self.manifests_dir.mkdir(parents=True, exist_ok=True)
        return GlobalManifest(created_at=now, last_updated=now, last_updated_by=self.pc_name)

    def _load_all_manifests(self) -> List[GlobalManifest]:
        """Load all per-PC manifests for merging."""
        manifests = []
        try:
            for f in self.manifests_dir.glob('*.json'):
                if f == self.my_manifest_path:
                    manifests.append(self.manifest)
                    continue
                try:
                    with open(f, 'r', encoding='utf-8') as fp:
                        data = json.load(fp)
                    manifests.append(GlobalManifest.from_dict(data))
                except Exception:
                    continue
            # Legacy fallback
            legacy_path = self.base_path / self.LEGACY_MANIFEST
            if legacy_path.exists():
                try:
                    with open(legacy_path, 'r', encoding='utf-8') as fp:
                        data = json.load(fp)
                    manifests.append(GlobalManifest.from_dict(data))
                except Exception:
                    pass
        except Exception:
            pass
        return manifests if manifests else [self.manifest]

    def _refresh_merged_cache(self) -> None:
        """Rebuild merged lookup sets from all PC manifests."""
        all_manifests = self._load_all_manifests()
        merged_p, merged_s, merged_f = set(), set(), set()
        for m in all_manifests:
            merged_p.update(m.processed_files.keys())
            merged_s.update(m.skipped_files.keys())
            merged_f.update(m.failed_files.keys())
        self._merged_processed = merged_p
        self._merged_skipped = merged_s
        self._merged_failed = merged_f
        self._last_merge_time = time.time()

    def _ensure_merged_fresh(self) -> None:
        if time.time() - self._last_merge_time > self._merge_interval:
            self._refresh_merged_cache()

    def refresh(self) -> GlobalManifest:
        """Reload own manifest and refresh merged cache."""
        with self._lock:
            if self.my_manifest_path.exists():
                try:
                    with open(self.my_manifest_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    self.manifest = GlobalManifest.from_dict(data)
                    self.manifest.register_pc(self.pc_name)
                except Exception as e:
                    print(f"[Manifest] Refresh error: {e}")
            self._refresh_merged_cache()
        return self.manifest

    def save(self, force: bool = False) -> None:
        """Save this PC's manifest to disk."""
        with self._lock:
            self._unsaved_changes += 1
            if not force and self._unsaved_changes < self.auto_save_interval:
                return
            try:
                self.manifests_dir.mkdir(parents=True, exist_ok=True)
                temp_path = self.my_manifest_path.with_suffix('.tmp')
                with open(temp_path, 'w', encoding='utf-8') as f:
                    json.dump(self.manifest.to_dict(), f, indent=2, ensure_ascii=False)
                temp_path.replace(self.my_manifest_path)
                self._unsaved_changes = 0
            except Exception as e:
                print(f"[Manifest] Save error: {e}")

    def is_processed(self, file_path: str) -> bool:
        if self.manifest.is_processed(file_path):
            return True
        self._ensure_merged_fresh()
        return self.manifest._normalize_path(file_path) in self._merged_processed

    def is_skipped(self, file_path: str) -> bool:
        if self.manifest.is_skipped(file_path):
            return True
        self._ensure_merged_fresh()
        return self.manifest._normalize_path(file_path) in self._merged_skipped

    def is_failed(self, file_path: str) -> bool:
        if self.manifest.is_failed(file_path):
            return True
        self._ensure_merged_fresh()
        return self.manifest._normalize_path(file_path) in self._merged_failed

    def should_process(self, file_path: str) -> bool:
        return (not self.is_processed(file_path) and
                not self.is_skipped(file_path) and
                not self.is_failed(file_path))

    def get_skip_reason(self, file_path: str) -> Optional[str]:
        return self.manifest.get_skip_reason(file_path)

    def record_skipped(self, file_path: str, reason: str, size_bytes: int = 0) -> None:
        self.manifest.add_skipped(file_path, reason, size_bytes, self.pc_name)
        normalized = self.manifest._normalize_path(file_path)
        self._merged_skipped.add(normalized)
        self.save()

    def record_success(
        self, original_path: str, output_path: str,
        input_size: int, output_size: int, encoder: str, cq_value: int,
        duration: float = 0, transcode_time: float = 0,
    ) -> None:
        record = ProcessedFile(
            original_path=original_path, output_path=output_path,
            input_size_bytes=input_size, output_size_bytes=output_size,
            compression_ratio=output_size / input_size if input_size > 0 else 0,
            processed_at=datetime.now().isoformat(), processed_by_pc=self.pc_name,
            encoder_used=encoder, cq_value=cq_value,
            duration_seconds=duration, transcode_seconds=transcode_time,
        )
        self.manifest.add_processed(record, self.pc_name)
        normalized = self.manifest._normalize_path(original_path)
        self._merged_processed.add(normalized)
        self.save()

    def record_failure(self, file_path: str, error: str) -> None:
        self.manifest.add_failed(file_path, error, self.pc_name)
        normalized = self.manifest._normalize_path(file_path)
        self._merged_failed.add(normalized)
        self.save(force=True)

    def reset_failed(self, file_path: Optional[str] = None) -> int:
        count = self.manifest.reset_failed(file_path)
        self._refresh_merged_cache()
        self.save(force=True)
        return count

    def update_estimates(self, total_files: int, total_bytes: int) -> None:
        self.manifest.update_estimates(total_files, total_bytes)
        self.save(force=True)

    def get_dashboard_data(self) -> dict:
        """Get merged dashboard data from all PC manifests."""
        self._ensure_merged_fresh()
        all_manifests = self._load_all_manifests()
        all_processed, all_failed, all_daily = {}, {}, {}
        all_active_pcs = {}
        t_in, t_out, t_saved, t_sec = 0, 0, 0, 0.0
        t_to_proc, t_bytes_to_proc = 0, 0

        for m in all_manifests:
            for path, rec in m.processed_files.items():
                if path not in all_processed:
                    all_processed[path] = rec
                    t_in += rec.input_size_bytes
                    t_out += rec.output_size_bytes
                    t_saved += rec.input_size_bytes - rec.output_size_bytes
                    t_sec += rec.transcode_seconds
            for path, err in m.failed_files.items():
                if path not in all_failed:
                    all_failed[path] = err
            for dk, dv in m.daily_history.items():
                if dk not in all_daily:
                    all_daily[dk] = DailyProgress(date=dk)
                all_daily[dk].files_processed += dv.files_processed
                all_daily[dk].bytes_processed += dv.bytes_processed
                all_daily[dk].bytes_saved += dv.bytes_saved
                for pc, cnt in dv.by_pc.items():
                    all_daily[dk].by_pc[pc] = all_daily[dk].by_pc.get(pc, 0) + cnt
            all_active_pcs.update(m.active_pcs)
            t_to_proc = max(t_to_proc, m.stats.total_files_to_process)
            t_bytes_to_proc = max(t_bytes_to_proc, m.stats.total_bytes_to_process)

        n_proc = len(all_processed)
        total_files = n_proc + t_to_proc
        progress = (n_proc / total_files * 100) if total_files > 0 else 0
        avg_ratio = t_out / t_in if t_in > 0 else 0.25
        speed = (t_in / (1024**3)) / (t_sec / 3600) if t_sec > 0 else 50
        remaining_gb = t_bytes_to_proc / (1024**3)
        days = (remaining_gb / speed / 24) if speed > 0 else 0

        return {
            'pc_name': self.pc_name,
            'last_updated': self.manifest.last_updated,
            'last_updated_by': self.manifest.last_updated_by,
            'active_pcs': list(all_active_pcs.keys()),
            'total_processed': n_proc, 'total_to_process': t_to_proc,
            'progress_percent': progress,
            'processed_tb': t_in / (1024**4), 'to_process_tb': t_bytes_to_proc / (1024**4),
            'saved_tb': t_saved / (1024**4),
            'estimated_total_savings_tb': (t_saved + t_bytes_to_proc * (1 - avg_ratio)) / (1024**4),
            'avg_compression': (1 - avg_ratio) * 100, 'avg_speed_gbh': speed,
            'days_remaining': days,
            'daily_progress': [
                {'date': d.date, 'files': d.files_processed,
                 'gb_processed': d.bytes_processed / (1024**3),
                 'gb_saved': d.bytes_saved / (1024**3), 'by_pc': d.by_pc}
                for d in (all_daily[k] for k in sorted(all_daily, reverse=True)[:14])
            ],
            'failed_count': len(all_failed),
            'skipped_count': len(self._merged_skipped),
        }

    def import_h265_feitos_txt(self, log_path: str, content: str) -> int:
        """Import entries from h265 feitos.txt into manifest."""
        if log_path in self.manifest.imported_h265_logs:
            return 0
        self._ensure_merged_fresh()
        imported = 0
        for line in content.splitlines():
            line = line.strip()
            if not line:
                continue
            parts = line.split('|')
            if len(parts) >= 4:
                try:
                    filename = parts[1].strip()
                    input_size = int(parts[2].strip()) if parts[2].strip().isdigit() else 0
                    output_size = int(parts[3].strip()) if parts[3].strip().isdigit() else 0
                    normalized = filename.lower()
                    if normalized in self._merged_processed:
                        continue
                    if normalized not in self.manifest.processed_files:
                        record = ProcessedFile(
                            original_path=filename, output_path="",
                            input_size_bytes=input_size, output_size_bytes=output_size,
                            compression_ratio=output_size / input_size if input_size > 0 else 0.25,
                            processed_at=parts[0].strip() or datetime.now().isoformat(),
                            processed_by_pc="imported", encoder_used="unknown", cq_value=0,
                        )
                        self.manifest.processed_files[normalized] = record
                        self.manifest.stats.total_files_processed += 1
                        self.manifest.stats.total_input_bytes += input_size
                        self.manifest.stats.total_output_bytes += output_size
                        self.manifest.stats.total_saved_bytes += (input_size - output_size)
                        self._merged_processed.add(normalized)
                        imported += 1
                except (ValueError, IndexError):
                    continue
        self.manifest.imported_h265_logs[log_path] = datetime.now().isoformat()
        self.save(force=True)
        print(f"[Manifest] Imported {imported} entries from {log_path}")
        return imported

    def get_stats_summary(self) -> dict:
        """Get merged stats across all PCs."""
        self._ensure_merged_fresh()
        all_manifests = self._load_all_manifests()
        seen = set()
        t_in, t_saved = 0, 0
        for m in all_manifests:
            for path, rec in m.processed_files.items():
                if path not in seen:
                    seen.add(path)
                    t_in += rec.input_size_bytes
                    t_saved += rec.input_size_bytes - rec.output_size_bytes
        return {
            'processed': len(seen), 'skipped': len(self._merged_skipped),
            'failed': len(self._merged_failed),
            'total_tb': t_in / (1024**4), 'saved_tb': t_saved / (1024**4),
        }

    def cleanup_old_history(self, max_days: int = 90) -> int:
        """Remove daily history entries older than max_days."""
        from datetime import timedelta
        cutoff = (datetime.now() - timedelta(days=max_days)).strftime('%Y-%m-%d')
        old_dates = [d for d in self.manifest.daily_history if d < cutoff]
        for d in old_dates:
            del self.manifest.daily_history[d]
        if old_dates:
            self.save(force=True)
        return len(old_dates)

    def get_manifest_path(self) -> Path:
        return self.my_manifest_path

    def close(self) -> None:
        self.cleanup_old_history()
        self.save(force=True)


# Backward compatibility alias
ManifestManager = GlobalManifestManager
