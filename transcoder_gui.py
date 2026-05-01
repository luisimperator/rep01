#!/usr/bin/env python3
"""
HeavyDrops Transcoder v6.0.27

Dropbox Video Transcoder - GUI Version
Simple graphical interface for local folder transcoding.

Features:
- H.264 to H.265/HEVC video transcoding
- Hardware acceleration: NVIDIA NVENC, Intel QSV, CPU fallback
- Dropbox integration with smart file handling
- Auto-organizes files: h264/ backup folder, h265 to original location
- Queue management: smaller files first, disk space monitoring
- Progress bar with ETA, queue counter
- START/PAUSE/STOP controls
- Beep notification when queue finishes
"""

VERSION = "6.0.27"

import socket
import subprocess
import sys
import time
import json
import re
import sqlite3
import shutil
import queue
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext

# Embedded manifest implementation (standalone GUI — no external module dependency)
import string as _string

def find_dropbox_path():
    """Auto-detect HeavyDrops Dropbox path by searching available drives."""
    import os
    app_subfolder = Path("HeavyDrops") / "App h265 Converter"

    # Method 1: Read Dropbox info.json (official way — works regardless of folder name)
    for env_var in ['APPDATA', 'LOCALAPPDATA']:
        env_val = os.environ.get(env_var, '')
        if not env_val:
            continue
        info_path = Path(env_val) / 'Dropbox' / 'info.json'
        if info_path.exists():
            try:
                with open(info_path, 'r', encoding='utf-8') as f:
                    info = json.load(f)
                for account_type in ['personal', 'business']:
                    if account_type in info and 'path' in info[account_type]:
                        dropbox_root = Path(info[account_type]['path'])
                        candidate = dropbox_root / app_subfolder
                        if dropbox_root.exists():
                            return candidate
            except Exception:
                pass

    # Method 2: Search for *Dropbox* folders on available drives
    drives_to_check = ['D', 'C', 'E', 'F', 'G', 'H']
    if os.name == 'nt':
        for letter in _string.ascii_uppercase:
            if Path(f"{letter}:\\").exists() and letter not in drives_to_check:
                drives_to_check.append(letter)

    for drive in drives_to_check:
        drive_path = Path(f"{drive}:\\")
        if not drive_path.exists():
            continue
        try:
            for folder in drive_path.iterdir():
                if folder.is_dir() and 'dropbox' in folder.name.lower():
                    candidate = folder / app_subfolder
                    return candidate
        except PermissionError:
            continue

    return None

def get_pc_name():
    hostname = socket.gethostname()
    if '.' in hostname:
        hostname = hostname.split('.')[0]
    return hostname

# Embedded ManifestManager class — per-PC manifests (v5.5: zero race conditions)
class ManifestManager:
    """Per-PC manifest manager. Each PC writes its own file, reads all for merged view."""

    MANIFESTS_DIR = "manifests"
    LEGACY_MANIFEST = "global_manifest.json"

    def __init__(self, base_dropbox_path=None):
        if base_dropbox_path:
            self.base_path = Path(base_dropbox_path)
        else:
            detected = find_dropbox_path()
            self.base_path = detected if detected else Path(r"C:\transcoder\manifest_data")

        self.manifests_dir = self.base_path / self.MANIFESTS_DIR
        self.pc_name = get_pc_name()
        self.my_manifest_path = self.manifests_dir / f"{self.pc_name}.json"
        self._lock = threading.Lock()
        self._unsaved_changes = 0

        # Initialize per-PC manifest
        self.manifests_dir.mkdir(parents=True, exist_ok=True)
        self._migrate_legacy_manifest()
        self.manifest = self._load_or_create()
        self._register_pc()

        # Merged cache for fast cross-PC lookups (refreshed periodically)
        self._merged_processed = set()
        self._merged_skipped = set()
        self._merged_failed = set()
        self._all_manifests_cache = []
        self._last_merge_time = 0
        self._merge_interval = 60  # seconds
        self._refresh_merged_cache()

    def _migrate_legacy_manifest(self):
        """Migrate from single global_manifest.json to per-PC manifests."""
        legacy_path = self.base_path / self.LEGACY_MANIFEST
        if not legacy_path.exists() or self.my_manifest_path.exists():
            return
        try:
            with open(legacy_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            my_data = self._extract_pc_data(data)
            with open(self.my_manifest_path, 'w', encoding='utf-8') as f:
                json.dump(my_data, f, indent=2, ensure_ascii=False)
            print(f"[Manifest] Migrated {self.pc_name} data from legacy manifest")
        except Exception as e:
            print(f"[Manifest] Migration error: {e}")

    def _extract_pc_data(self, global_data):
        """Extract this PC's entries from the legacy global manifest."""
        now = datetime.now().isoformat()
        my_processed = {}
        for path, record in global_data.get('processed_files', {}).items():
            pc = record.get('processed_by_pc', '') if isinstance(record, dict) else ''
            if pc == self.pc_name or pc == 'imported':
                my_processed[path] = record
        my_skipped = {}
        for path, record in global_data.get('skipped_files', {}).items():
            pc = record.get('checked_by_pc', '') if isinstance(record, dict) else ''
            if pc == self.pc_name:
                my_skipped[path] = record
        my_failed = {}
        for path, error in global_data.get('failed_files', {}).items():
            if f'(by {self.pc_name})' in str(error):
                my_failed[path] = error
        my_daily = {}
        for date_key, day_data in global_data.get('daily_history', {}).items():
            by_pc = day_data.get('by_pc', {})
            if self.pc_name in by_pc:
                my_count = by_pc[self.pc_name]
                total_files = day_data.get('files_processed', 0)
                ratio = my_count / total_files if total_files > 0 else 0
                my_daily[date_key] = {
                    'date': date_key,
                    'files_processed': my_count,
                    'bytes_processed': int(day_data.get('bytes_processed', 0) * ratio),
                    'bytes_saved': int(day_data.get('bytes_saved', 0) * ratio),
                    'by_pc': {self.pc_name: my_count},
                }
        stats = {
            'total_files_processed': len(my_processed),
            'total_input_bytes': sum(
                r.get('input_size_bytes', 0) if isinstance(r, dict) else 0
                for r in my_processed.values()
            ),
            'total_output_bytes': sum(
                r.get('output_size_bytes', 0) if isinstance(r, dict) else 0
                for r in my_processed.values()
            ),
            'total_saved_bytes': sum(
                (r.get('input_size_bytes', 0) - r.get('output_size_bytes', 0)) if isinstance(r, dict) else 0
                for r in my_processed.values()
            ),
            'total_transcode_seconds': sum(
                r.get('transcode_seconds', 0) if isinstance(r, dict) else 0
                for r in my_processed.values()
            ),
            'total_files_to_process': global_data.get('stats', {}).get('total_files_to_process', 0),
            'total_bytes_to_process': global_data.get('stats', {}).get('total_bytes_to_process', 0),
        }
        return {
            'pc_name': self.pc_name,
            'created_at': now, 'last_updated': now, 'last_updated_by': self.pc_name,
            'stats': stats,
            'processed_files': my_processed, 'skipped_files': my_skipped,
            'failed_files': my_failed, 'daily_history': my_daily,
            'active_pcs': {self.pc_name: now},
            'imported_h265_logs': global_data.get('imported_h265_logs', {}),
        }

    def _load_or_create(self):
        """Load this PC's manifest or create a new one."""
        if self.my_manifest_path.exists():
            try:
                with open(self.my_manifest_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                print(f"[Manifest] Loaded {self.pc_name}: {data.get('stats', {}).get('total_files_processed', 0)} files")
                return self._ensure_fields(data)
            except Exception as e:
                print(f"[Manifest] Error loading: {e}")
        now = datetime.now().isoformat()
        return {
            'pc_name': self.pc_name,
            'created_at': now, 'last_updated': now, 'last_updated_by': self.pc_name,
            'stats': {
                'total_files_processed': 0, 'total_input_bytes': 0,
                'total_output_bytes': 0, 'total_saved_bytes': 0,
                'total_transcode_seconds': 0, 'total_files_to_process': 0,
                'total_bytes_to_process': 0,
            },
            'processed_files': {}, 'skipped_files': {}, 'failed_files': {},
            'daily_history': {}, 'active_pcs': {}, 'imported_h265_logs': {},
        }

    def _ensure_fields(self, data):
        """Ensure all required fields exist in manifest dict."""
        data.setdefault('pc_name', self.pc_name)
        data.setdefault('stats', {})
        for key in ['total_files_processed', 'total_input_bytes', 'total_output_bytes',
                     'total_saved_bytes', 'total_transcode_seconds',
                     'total_files_to_process', 'total_bytes_to_process']:
            data['stats'].setdefault(key, 0)
        data.setdefault('processed_files', {})
        data.setdefault('skipped_files', {})
        data.setdefault('failed_files', {})
        data.setdefault('daily_history', {})
        data.setdefault('active_pcs', {})
        data.setdefault('imported_h265_logs', {})
        return data

    def _load_all_manifests(self):
        """Load all per-PC manifests from the manifests directory."""
        manifests = []
        try:
            for f in self.manifests_dir.glob('*.json'):
                if f == self.my_manifest_path:
                    manifests.append(self.manifest)  # Use in-memory (most up-to-date)
                    continue
                try:
                    with open(f, 'r', encoding='utf-8') as fp:
                        data = json.load(fp)
                    manifests.append(self._ensure_fields(data))
                except Exception:
                    continue
            # Also check legacy manifest as fallback during migration
            legacy_path = self.base_path / self.LEGACY_MANIFEST
            if legacy_path.exists():
                try:
                    with open(legacy_path, 'r', encoding='utf-8') as fp:
                        data = json.load(fp)
                    manifests.append(self._ensure_fields(data))
                except Exception:
                    pass
        except Exception:
            pass
        if not manifests:
            manifests.append(self.manifest)
        return manifests

    def _refresh_merged_cache(self):
        """Reload all manifests and build merged lookup sets."""
        all_manifests = self._load_all_manifests()
        merged_processed = set()
        merged_skipped = set()
        merged_failed = set()
        for m in all_manifests:
            merged_processed.update(m.get('processed_files', {}).keys())
            merged_skipped.update(m.get('skipped_files', {}).keys())
            merged_failed.update(m.get('failed_files', {}).keys())
        self._merged_processed = merged_processed
        self._merged_skipped = merged_skipped
        self._merged_failed = merged_failed
        self._all_manifests_cache = all_manifests
        self._last_merge_time = time.time()

    def _ensure_merged_fresh(self):
        """Refresh merged cache if stale."""
        if time.time() - self._last_merge_time > self._merge_interval:
            self._refresh_merged_cache()

    def _register_pc(self):
        self.manifest['active_pcs'][self.pc_name] = datetime.now().isoformat()

    def _normalize_path(self, path):
        return str(path).lower().replace('\\', '/')

    def refresh(self):
        """Reload own manifest from disk and refresh merged cache."""
        with self._lock:
            if self.my_manifest_path.exists():
                try:
                    with open(self.my_manifest_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    self.manifest = self._ensure_fields(data)
                    self._register_pc()
                except Exception as e:
                    print(f"[Manifest] Refresh error: {e}")
            self._refresh_merged_cache()
        return self.manifest

    def save(self, force=False):
        """Save this PC's manifest to disk."""
        with self._lock:
            self._unsaved_changes += 1
            if not force and self._unsaved_changes < 3:
                return
            try:
                self.manifests_dir.mkdir(parents=True, exist_ok=True)
                temp_path = self.my_manifest_path.with_suffix('.tmp')
                with open(temp_path, 'w', encoding='utf-8') as f:
                    json.dump(self.manifest, f, indent=2, ensure_ascii=False)
                temp_path.replace(self.my_manifest_path)
                self._unsaved_changes = 0
            except Exception as e:
                print(f"[Manifest] Save error: {e}")

    def is_processed(self, file_path):
        normalized = self._normalize_path(file_path)
        if normalized in self.manifest['processed_files']:
            return True
        self._ensure_merged_fresh()
        return normalized in self._merged_processed

    def is_skipped(self, file_path):
        normalized = self._normalize_path(file_path)
        if normalized in self.manifest['skipped_files']:
            return True
        self._ensure_merged_fresh()
        return normalized in self._merged_skipped

    def is_failed(self, file_path):
        normalized = self._normalize_path(file_path)
        if normalized in self.manifest['failed_files']:
            return True
        self._ensure_merged_fresh()
        return normalized in self._merged_failed

    def record_success(self, original_path, output_path, input_size, output_size, encoder, cq_value, duration=0, transcode_time=0):
        normalized = self._normalize_path(original_path)
        self.manifest['processed_files'][normalized] = {
            'original_path': original_path,
            'output_path': output_path,
            'input_size_bytes': input_size,
            'output_size_bytes': output_size,
            'compression_ratio': output_size / input_size if input_size > 0 else 0,
            'processed_at': datetime.now().isoformat(),
            'processed_by_pc': self.pc_name,
            'encoder_used': encoder,
            'cq_value': cq_value,
        }
        self.manifest['stats']['total_files_processed'] += 1
        self.manifest['stats']['total_input_bytes'] += input_size
        self.manifest['stats']['total_output_bytes'] += output_size
        self.manifest['stats']['total_saved_bytes'] += (input_size - output_size)
        self.manifest['stats']['total_transcode_seconds'] += transcode_time
        self.manifest['last_updated'] = datetime.now().isoformat()
        self.manifest['last_updated_by'] = self.pc_name

        today = datetime.now().strftime('%Y-%m-%d')
        if today not in self.manifest['daily_history']:
            self.manifest['daily_history'][today] = {'date': today, 'files_processed': 0, 'bytes_processed': 0, 'bytes_saved': 0, 'by_pc': {}}
        self.manifest['daily_history'][today]['files_processed'] += 1
        self.manifest['daily_history'][today]['bytes_processed'] += input_size
        self.manifest['daily_history'][today]['bytes_saved'] += (input_size - output_size)
        self.manifest['daily_history'][today]['by_pc'][self.pc_name] = self.manifest['daily_history'][today]['by_pc'].get(self.pc_name, 0) + 1
        self._merged_processed.add(normalized)
        self.save()

    def record_failure(self, file_path, error):
        normalized = self._normalize_path(file_path)
        self.manifest['failed_files'][normalized] = f"{error} (by {self.pc_name})"
        self._merged_failed.add(normalized)
        self.save(force=True)

    def record_skipped(self, file_path, reason, size_bytes=0):
        normalized = self._normalize_path(file_path)
        self.manifest['skipped_files'][normalized] = {
            'path': file_path,
            'reason': reason,
            'size_bytes': size_bytes,
            'checked_at': datetime.now().isoformat(),
            'checked_by_pc': self.pc_name,
        }
        self._merged_skipped.add(normalized)
        self.save()

    def reset_failed(self, file_path=None):
        if file_path:
            normalized = self._normalize_path(file_path)
            if normalized in self.manifest['failed_files']:
                del self.manifest['failed_files'][normalized]
                self._merged_failed.discard(normalized)
                self.save(force=True)
                return 1
            return 0
        count = len(self.manifest['failed_files'])
        self.manifest['failed_files'].clear()
        self._refresh_merged_cache()
        self.save(force=True)
        return count

    def update_estimates(self, total_files, total_bytes):
        current_files = self.manifest['stats'].get('total_files_to_process', 0)
        current_bytes = self.manifest['stats'].get('total_bytes_to_process', 0)
        if total_files > current_files:
            self.manifest['stats']['total_files_to_process'] = total_files
        if total_bytes > current_bytes:
            self.manifest['stats']['total_bytes_to_process'] = total_bytes
        self.save(force=True)

    def import_h265_feitos_txt(self, log_path, content):
        """Import entries from h265 feito.txt log files."""
        if log_path in self.manifest['imported_h265_logs']:
            return 0
        self._ensure_merged_fresh()
        imported = 0
        for line in content.splitlines():
            line = line.strip()
            if not line or 'H264 FOLDER DELETED' in line:
                continue
            parts = line.split('|')
            if len(parts) >= 3:
                try:
                    timestamp = parts[0].strip()
                    filename = parts[1].strip()
                    size_part = parts[2].strip() if len(parts) > 2 else ""
                    input_size = 0
                    output_size = 0
                    if '->' in size_part:
                        size_match = re.match(r'([\d.]+)MB\s*->\s*([\d.]+)MB', size_part)
                        if size_match:
                            input_size = int(float(size_match.group(1)) * 1024 * 1024)
                            output_size = int(float(size_match.group(2)) * 1024 * 1024)
                    log_dir = str(Path(log_path).parent.parent)
                    full_path = f"{log_dir}/{filename}"
                    normalized = self._normalize_path(full_path)
                    # Skip if already known by any PC (avoids duplicate imports)
                    if normalized in self._merged_processed:
                        continue
                    if normalized not in self.manifest['processed_files']:
                        self.manifest['processed_files'][normalized] = {
                            'original_path': full_path, 'output_path': '',
                            'input_size_bytes': input_size, 'output_size_bytes': output_size,
                            'compression_ratio': output_size / input_size if input_size > 0 else 0.25,
                            'processed_at': timestamp if timestamp else datetime.now().isoformat(),
                            'processed_by_pc': 'imported', 'encoder_used': 'unknown', 'cq_value': 0,
                        }
                        self.manifest['stats']['total_files_processed'] += 1
                        self.manifest['stats']['total_input_bytes'] += input_size
                        self.manifest['stats']['total_output_bytes'] += output_size
                        self.manifest['stats']['total_saved_bytes'] += (input_size - output_size)
                        self._merged_processed.add(normalized)
                        imported += 1
                except Exception:
                    continue
        self.manifest['imported_h265_logs'][log_path] = datetime.now().isoformat()
        self.save(force=True)
        print(f"[Manifest] Imported {imported} entries from {log_path}")
        return imported

    def get_stats_summary(self):
        """Get merged stats across all PCs."""
        self._ensure_merged_fresh()
        total_processed = set()
        total_input = 0
        total_saved = 0
        for m in self._all_manifests_cache:
            for path, record in m.get('processed_files', {}).items():
                if path not in total_processed:
                    total_processed.add(path)
                    if isinstance(record, dict):
                        total_input += record.get('input_size_bytes', 0)
                        total_saved += record.get('input_size_bytes', 0) - record.get('output_size_bytes', 0)
        return {
            'processed': len(total_processed),
            'skipped': len(self._merged_skipped),
            'failed': len(self._merged_failed),
            'total_tb': total_input / (1024**4),
            'saved_tb': total_saved / (1024**4),
        }

    def get_dashboard_data(self):
        """Get merged dashboard data from all PC manifests."""
        self._ensure_merged_fresh()
        all_processed = {}
        all_failed = {}
        all_daily = {}
        all_active_pcs = {}
        total_input = 0
        total_output = 0
        total_saved = 0
        total_transcode_sec = 0
        total_to_process = 0
        total_bytes_to_process = 0

        for m in self._all_manifests_cache:
            for path, record in m.get('processed_files', {}).items():
                if path not in all_processed:
                    all_processed[path] = record
                    if isinstance(record, dict):
                        total_input += record.get('input_size_bytes', 0)
                        total_output += record.get('output_size_bytes', 0)
                        total_saved += record.get('input_size_bytes', 0) - record.get('output_size_bytes', 0)
                        total_transcode_sec += record.get('transcode_seconds', 0)
            for path, error in m.get('failed_files', {}).items():
                if path not in all_failed:
                    all_failed[path] = error
            for date_key, day_data in m.get('daily_history', {}).items():
                if date_key not in all_daily:
                    all_daily[date_key] = {'date': date_key, 'files_processed': 0, 'bytes_processed': 0, 'bytes_saved': 0, 'by_pc': {}}
                all_daily[date_key]['files_processed'] += day_data.get('files_processed', 0)
                all_daily[date_key]['bytes_processed'] += day_data.get('bytes_processed', 0)
                all_daily[date_key]['bytes_saved'] += day_data.get('bytes_saved', 0)
                for pc, count in day_data.get('by_pc', {}).items():
                    all_daily[date_key]['by_pc'][pc] = all_daily[date_key]['by_pc'].get(pc, 0) + count
            all_active_pcs.update(m.get('active_pcs', {}))
            m_stats = m.get('stats', {})
            total_to_process = max(total_to_process, m_stats.get('total_files_to_process', 0))
            total_bytes_to_process = max(total_bytes_to_process, m_stats.get('total_bytes_to_process', 0))

        actual_processed = len(all_processed)
        total_files = actual_processed + total_to_process
        progress = (actual_processed / total_files * 100) if total_files > 0 else 0
        avg_ratio = total_output / total_input if total_input > 0 else 0.25
        speed = (total_input / (1024**3)) / (total_transcode_sec / 3600) if total_transcode_sec > 0 else 50
        remaining_gb = total_bytes_to_process / (1024**3)
        days = (remaining_gb / speed / 24) if speed > 0 else 0

        daily = []
        for date_key in sorted(all_daily.keys(), reverse=True)[:14]:
            d = all_daily[date_key]
            daily.append({
                'date': d['date'], 'files': d['files_processed'],
                'gb_processed': d['bytes_processed'] / (1024**3),
                'gb_saved': d['bytes_saved'] / (1024**3),
                'by_pc': d.get('by_pc', {}),
            })

        return {
            'pc_name': self.pc_name,
            'last_updated': self.manifest['last_updated'],
            'last_updated_by': self.manifest['last_updated_by'],
            'active_pcs': list(all_active_pcs.keys()),
            'total_processed': actual_processed,
            'total_to_process': total_to_process,
            'progress_percent': progress,
            'processed_tb': total_input / (1024**4),
            'to_process_tb': total_bytes_to_process / (1024**4),
            'saved_tb': total_saved / (1024**4),
            'estimated_total_savings_tb': (total_saved + total_bytes_to_process * (1 - avg_ratio)) / (1024**4),
            'avg_compression': (1 - avg_ratio) * 100,
            'avg_speed_gbh': speed,
            'days_remaining': days,
            'daily_progress': daily,
            'failed_count': len(all_failed),
            'skipped_count': len(self._merged_skipped),
        }

    def cleanup_old_history(self, max_days=90):
        """Remove daily history entries older than max_days."""
        from datetime import timedelta
        cutoff = (datetime.now() - timedelta(days=max_days)).strftime('%Y-%m-%d')
        old_dates = [d for d in self.manifest['daily_history'] if d < cutoff]
        for d in old_dates:
            del self.manifest['daily_history'][d]
        if old_dates:
            self.save(force=True)
        return len(old_dates)

    def get_manifest_path(self):
        return self.my_manifest_path

    def close(self):
        self.cleanup_old_history()
        self.save(force=True)


def get_dropbox_base_path() -> Path:
    """Get the Dropbox base path, auto-detecting if possible."""
    detected = find_dropbox_path()
    if detected:
        return detected
    # Fallback: use C:\transcoder as local-only manifest storage
    return Path(r"C:\transcoder\manifest_data")

# Windows-specific for beep sound
try:
    import winsound
    HAS_WINSOUND = True
except ImportError:
    HAS_WINSOUND = False


class TranscoderGUI:
    # Settings file path
    SETTINGS_FILE = Path(r"C:\transcoder\settings.json")
    DELETION_RECORDS_FILE = Path(r"C:\transcoder\deletion_records.json")

    def __init__(self, root):
        self.root = root
        self.pc_name = get_pc_name()
        self.root.title(f"HeavyDrops Transcoder v{VERSION} - H.264 → H.265 [{self.pc_name}]")
        self.root.geometry("900x700")
        self.root.minsize(800, 600)

        # Auto-detect Dropbox path (works for D:, C:, or any drive)
        self.dropbox_base = get_dropbox_base_path()
        print(f"[GUI] Dropbox path: {self.dropbox_base}")

        # State
        self.running = False
        self.paused = False
        self.wav_running = False
        self.current_process = None  # Current FFmpeg process
        self.worker_thread = None
        self.current_file = None
        self.db_conn = None
        self.cloud_manifest = None  # Cloud manifest manager
        self.files_in_batch = 0  # Track files processed in current batch

        # Download queue management - balance downloads with SSD space
        self.pending_downloads = {}  # {path_str: estimated_size_bytes}
        self.pending_downloads_lock = threading.Lock()
        self.max_pending_downloads = 50  # Max files downloading at once
        self.min_free_space_gb = 20  # Keep at least 20GB free for pending downloads

        # Download progress tracking for ETA estimation
        self._download_progress = {}  # {path_str: {'started': time, 'last_size': bytes, 'speed': bytes/sec}}
        self._download_speed_samples = []  # Rolling average of download speeds

        # READY QUEUE: Files confirmed downloaded and ready to transcode
        # This decouples downloading from transcoding - no more waiting!
        self.ready_queue = queue.Queue()  # Queue of (path, size, folder_priority) tuples
        self.ready_queue_worker_running = False
        self.ready_queue_worker_thread = None
        self.files_being_checked = set()  # Files currently being checked for readiness
        self.files_being_checked_lock = threading.Lock()

        # === NEW ARCHITECTURE v2.0: Queue-first, folder-complete, zero mass-probe ===
        # Objetivo: manter fila de ~100 arquivos sem varrer 70k/200k arquivos
        # Princípios:
        # 1. Queue-first: se tenho 100 na fila, não preciso olhar o universo
        # 2. Folder-complete: priorizar completar uma pasta antes de começar outra
        # 3. Zero mass-probe: varredura incremental por pasta, nunca global

        self.QUEUE_TARGET_SIZE = 100  # Tamanho alvo da fila
        self.QUEUE_REFILL_THRESHOLD = 99  # Refill quando cair abaixo disso

        # Estados dos itens na fila
        # READY_LOCAL, QUEUED_REMOTE, DOWNLOADING, FAILED_RETRY, TRANSCODING, DONE

        # Folder tracker: estado de cada pasta em progresso
        # {folder_path: {
        #     'status': 'ACTIVE' | 'COMPLETING' | 'DONE',
        #     'total_known': int,      # arquivos conhecidos (pode crescer)
        #     'selected': int,         # quantos na fila ativa
        #     'done': int,             # quantos finalizados
        #     'last_file_index': int,  # último arquivo listado (paginação)
        #     'files_list': list,      # cache dos arquivos da pasta (ordem alfabética)
        # }}
        self.folder_tracker = {}
        self.folder_tracker_lock = threading.Lock()

        # Lista de pastas a processar (ordem alfabética, lazy-loaded)
        self.pending_folders = []  # [(folder_path, priority)]
        self.pending_folders_index = 0  # Próxima pasta a explorar
        self.pending_folders_loaded = False  # Se já carregou a lista de pastas

        # Active queue: itens selecionados (até QUEUE_TARGET_SIZE)
        # Cada item: {'path': Path, 'size': int, 'folder': str, 'status': str, 'retry_at': float}
        self.active_queue = []  # Lista ordenada de itens
        self.active_queue_lock = threading.Lock()
        self._queue_items_set = set()  # Fast lookup to avoid duplicates in active_queue
        self._in_ready_queue = set()  # Tracks items specifically in ready_queue

        self.local_eligible_exhausted = False  # Gate for downloads
        self.QUEUE_SNAPSHOT_FILE = self.dropbox_base / "App h265 Converter" / ".queue_snapshot_v2.json"

        # PRE-PROBE BUFFER: Files already probed and ready for instant transcode
        # This eliminates the 30+ second gap between transcodes
        self.probed_queue = queue.Queue()  # Queue of (path, size, probe_data) tuples
        self.probed_queue_worker_running = False
        self.PROBED_BUFFER_SIZE = 5  # Keep 5 files pre-probed

        # Idle state tracking - avoid spamming "nothing to do" logs
        self._last_scan_had_work = True  # Assume work initially so first idle is logged

        # Default settings (using auto-detected path)
        default_folder = str(self.dropbox_base)
        default_logs = str(self.dropbox_base / "logs")
        self.watch_folder = tk.StringVar(value=default_folder)
        self.log_folder = tk.StringVar(value=default_logs)
        self.min_size_gb = tk.DoubleVar(value=0)
        self.encoder = tk.StringVar(value="nvenc")
        self.cq_value = tk.IntVar(value=24)
        self.auto_delete_h264 = tk.BooleanVar(value=False)  # Delete h264 backups after verification
        self.offline_mode = tk.BooleanVar(value=False)  # Don't trigger downloads, only process local files
        self.auto_start = tk.BooleanVar(value=True)  # Auto-start processing on launch (daemon mode)

        # Hourly speed tracking - list of (timestamp, bytes, seconds) for last hour
        self._hourly_transcode_records = []

        # Deletion tracking - list of (timestamp, bytes_deleted) for tracking GB freed
        self._deletion_records = []
        self._load_deletion_records()  # Load persisted records from disk

        # Stats
        self.files_processed = tk.IntVar(value=0)
        self.total_saved_gb = tk.DoubleVar(value=0)

        # Load saved settings
        self.load_settings()

        self.setup_ui()
        self.setup_database()
        self.setup_cloud_manifest()
        self.load_stats()
        self.check_ffmpeg()

        # Start download status UI updater (every 2 seconds)
        self._update_download_status_ui()

        # Save settings when window closes
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

        # Auto-start processing after UI is ready (daemon mode)
        if self.auto_start.get():
            self.root.after(1000, self._auto_start_daemon)

    def get_watch_folders(self) -> list:
        """Get list of watch folders (supports semicolon-separated paths)."""
        raw = self.watch_folder.get()
        folders = []
        for part in raw.split(';'):
            part = part.strip()
            if part:
                p = Path(part)
                if p.exists():
                    folders.append(p)
        return folders if folders else [Path(raw)]  # Fallback to original if no valid paths

    def setup_ui(self):
        """Create the UI."""
        # Main container
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # === SETTINGS FRAME ===
        settings_frame = ttk.LabelFrame(main_frame, text="Settings", padding="10")
        settings_frame.pack(fill=tk.X, pady=(0, 10))

        # Watch folder (multiple folders separated by semicolon)
        ttk.Label(settings_frame, text="Watch Folder(s):").grid(row=0, column=0, sticky=tk.W, pady=5)
        folder_frame = ttk.Frame(settings_frame)
        folder_frame.grid(row=0, column=1, sticky=tk.EW, pady=5)
        ttk.Entry(folder_frame, textvariable=self.watch_folder, width=60).pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Button(folder_frame, text="Browse", command=self.browse_folder).pack(side=tk.LEFT, padx=(5, 0))

        # Encoder
        ttk.Label(settings_frame, text="Encoder:").grid(row=1, column=0, sticky=tk.W, pady=5)
        encoder_frame = ttk.Frame(settings_frame)
        encoder_frame.grid(row=1, column=1, sticky=tk.W, pady=5)
        ttk.Radiobutton(encoder_frame, text="CPU (libx265) - Works always", variable=self.encoder, value="cpu").pack(side=tk.LEFT)
        ttk.Radiobutton(encoder_frame, text="NVIDIA (NVENC)", variable=self.encoder, value="nvenc").pack(side=tk.LEFT, padx=(20, 0))
        ttk.Radiobutton(encoder_frame, text="Intel (QSV)", variable=self.encoder, value="qsv").pack(side=tk.LEFT, padx=(20, 0))

        # Quality
        ttk.Label(settings_frame, text="Quality (CRF/CQ):").grid(row=2, column=0, sticky=tk.W, pady=5)
        quality_frame = ttk.Frame(settings_frame)
        quality_frame.grid(row=2, column=1, sticky=tk.W, pady=5)
        ttk.Scale(quality_frame, from_=18, to=30, variable=self.cq_value, orient=tk.HORIZONTAL, length=200).pack(side=tk.LEFT)
        ttk.Spinbox(quality_frame, from_=15, to=35, textvariable=self.cq_value, width=5).pack(side=tk.LEFT, padx=(10, 0))
        ttk.Label(quality_frame, text="(lower = better quality, larger file)", font=("", 8)).pack(side=tk.LEFT, padx=(10, 0))

        # Min size
        ttk.Label(settings_frame, text="Min Size (GB):").grid(row=3, column=0, sticky=tk.W, pady=5)
        size_frame = ttk.Frame(settings_frame)
        size_frame.grid(row=3, column=1, sticky=tk.W, pady=5)
        ttk.Spinbox(size_frame, from_=0, to=100, textvariable=self.min_size_gb, width=10).pack(side=tk.LEFT)
        ttk.Label(size_frame, text="(0 = process all files)", font=("", 8)).pack(side=tk.LEFT, padx=(10, 0))

        # Log folder
        ttk.Label(settings_frame, text="Log Folder:").grid(row=4, column=0, sticky=tk.W, pady=5)
        log_frame = ttk.Frame(settings_frame)
        log_frame.grid(row=4, column=1, sticky=tk.EW, pady=5)
        ttk.Entry(log_frame, textvariable=self.log_folder, width=60).pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Button(log_frame, text="Browse", command=self.browse_log_folder).pack(side=tk.LEFT, padx=(5, 0))

        # Auto-delete h264 option
        ttk.Label(settings_frame, text="Options:").grid(row=5, column=0, sticky=tk.W, pady=5)
        options_frame = ttk.Frame(settings_frame)
        options_frame.grid(row=5, column=1, sticky=tk.W, pady=5)
        self.delete_h264_checkbox = ttk.Checkbutton(
            options_frame,
            text="Delete h264 folder after 20 min (all files verified)",
            variable=self.auto_delete_h264,
            command=self._on_delete_h264_toggle
        )
        self.delete_h264_checkbox.pack(side=tk.LEFT)
        ttk.Label(options_frame, text="⚠️ Irreversível!", foreground="red", font=("", 8)).pack(side=tk.LEFT, padx=(10, 0))

        # Offline mode checkbox (same row)
        ttk.Checkbutton(
            options_frame,
            text="Offline Mode (no downloads)",
            variable=self.offline_mode
        ).pack(side=tk.LEFT, padx=(30, 0))

        # Auto-start checkbox (daemon mode)
        ttk.Checkbutton(
            options_frame,
            text="Auto-Start (daemon mode)",
            variable=self.auto_start
        ).pack(side=tk.LEFT, padx=(30, 0))

        settings_frame.columnconfigure(1, weight=1)

        # === CONTROL FRAME ===
        control_frame = ttk.Frame(main_frame)
        control_frame.pack(fill=tk.X, pady=(0, 10))

        self.start_btn = ttk.Button(control_frame, text="▶ START", command=self.toggle_processing, style="Accent.TButton")
        self.start_btn.pack(side=tk.LEFT, padx=(0, 5))

        self.pause_btn = ttk.Button(control_frame, text="⏸ PAUSE", command=self.toggle_pause, state=tk.DISABLED)
        self.pause_btn.pack(side=tk.LEFT, padx=(0, 5))

        self.stop_btn = ttk.Button(control_frame, text="⏹ STOP", command=self.stop_all, state=tk.DISABLED)
        self.stop_btn.pack(side=tk.LEFT, padx=(0, 10))

        ttk.Button(control_frame, text="🔍 Scan", command=self.scan_and_trigger_download).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(control_frame, text="📊 Report", command=self.generate_report).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(control_frame, text="📁 Open Folder", command=self.open_folder).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(control_frame, text="🔄 Reset Failed", command=self.reset_failed).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(control_frame, text="🎵 WAV→MP3", command=self.start_wav_conversion).pack(side=tk.LEFT, padx=(0, 5))
        self.stop_wav_btn = ttk.Button(control_frame, text="⏹ STOP WAV", command=self.stop_wav_conversion, state=tk.DISABLED)
        self.stop_wav_btn.pack(side=tk.LEFT, padx=(0, 5))
        ttk.Button(control_frame, text="🔍 Scan WAV", command=self.scan_audio_files).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(control_frame, text="🗑 Clear History", command=self.clear_history).pack(side=tk.LEFT)

        # Stats
        stats_frame = ttk.Frame(control_frame)
        stats_frame.pack(side=tk.RIGHT)
        ttk.Label(stats_frame, text="Processed:").pack(side=tk.LEFT)
        ttk.Label(stats_frame, textvariable=self.files_processed, font=("", 10, "bold")).pack(side=tk.LEFT, padx=(5, 15))
        ttk.Label(stats_frame, text="Saved:").pack(side=tk.LEFT)
        ttk.Label(stats_frame, textvariable=self.total_saved_gb, font=("", 10, "bold")).pack(side=tk.LEFT, padx=(5, 0))
        ttk.Label(stats_frame, text="GB").pack(side=tk.LEFT)

        # === PROGRESS FRAME ===
        progress_frame = ttk.LabelFrame(main_frame, text="Current Progress", padding="10")
        progress_frame.pack(fill=tk.X, pady=(0, 10))

        # Local folder status
        local_status_frame = ttk.Frame(progress_frame)
        local_status_frame.pack(fill=tk.X)
        ttk.Label(local_status_frame, text="Local:", font=("", 8, "bold")).pack(side=tk.LEFT)
        self.current_file_label = ttk.Label(local_status_frame, text="Idle", font=("", 9))
        self.current_file_label.pack(side=tk.LEFT, padx=(5, 0))

        # Download queue status (new)
        download_status_frame = ttk.Frame(progress_frame)
        download_status_frame.pack(fill=tk.X, pady=(3, 0))
        ttk.Label(download_status_frame, text="Downloads:", font=("", 8, "bold")).pack(side=tk.LEFT)
        self.download_queue_label = ttk.Label(download_status_frame, text="No pending downloads", font=("", 9), foreground="gray")
        self.download_queue_label.pack(side=tk.LEFT, padx=(5, 0))

        # Files in download queue (shows actual filenames)
        self.download_files_label = ttk.Label(progress_frame, text="", font=("", 8), foreground="gray", wraplength=800, justify=tk.LEFT)
        self.download_files_label.pack(fill=tk.X, pady=(2, 0))

        # Progress bar with percentage
        self.progress_var = tk.DoubleVar(value=0)
        self.progress_bar = ttk.Progressbar(progress_frame, mode='determinate', variable=self.progress_var, maximum=100)
        self.progress_bar.pack(fill=tk.X, pady=(5, 0))

        self.progress_label = ttk.Label(progress_frame, text="", font=("", 8))
        self.progress_label.pack(fill=tk.X, pady=(5, 0))

        # === DASHBOARD FRAME ===
        dashboard_frame = ttk.LabelFrame(main_frame, text="📊 Dashboard Global (todos os PCs)", padding="10")
        dashboard_frame.pack(fill=tk.X, pady=(0, 10))

        # Top row: Refresh button and last update info
        dash_top = ttk.Frame(dashboard_frame)
        dash_top.pack(fill=tk.X)

        ttk.Button(dash_top, text="🔄 REFRESH", command=self.refresh_dashboard).pack(side=tk.LEFT)
        ttk.Button(dash_top, text="📋 SCAN", command=self.run_inventory_scan).pack(side=tk.LEFT, padx=(5, 0))
        ttk.Button(dash_top, text="🗑️ CLEANUP", command=self.run_all_cleanups).pack(side=tk.LEFT, padx=(5, 0))
        ttk.Button(dash_top, text="📂 QUEUE", command=self.show_ready_queue).pack(side=tk.LEFT, padx=(5, 0))
        self.dash_last_update = ttk.Label(dash_top, text="", font=("", 8))
        self.dash_last_update.pack(side=tk.LEFT, padx=(10, 0))
        self.dash_skipped_label = ttk.Label(dash_top, text="", font=("", 8), foreground="gray")
        self.dash_skipped_label.pack(side=tk.RIGHT, padx=(10, 0))
        self.dash_active_pcs = ttk.Label(dash_top, text="", font=("", 8))
        self.dash_active_pcs.pack(side=tk.RIGHT)

        # Main stats row
        dash_stats = ttk.Frame(dashboard_frame)
        dash_stats.pack(fill=tk.X, pady=(10, 5))

        # Progress percentage
        self.dash_progress_var = tk.DoubleVar(value=0)
        ttk.Label(dash_stats, text="Progresso Total:", font=("", 9, "bold")).pack(side=tk.LEFT)
        self.dash_progress_pct = ttk.Label(dash_stats, text="0%", font=("", 12, "bold"), foreground="blue")
        self.dash_progress_pct.pack(side=tk.LEFT, padx=(5, 10))

        self.dash_progress_bar = ttk.Progressbar(dash_stats, mode='determinate', variable=self.dash_progress_var, maximum=100, length=200)
        self.dash_progress_bar.pack(side=tk.LEFT, padx=(0, 20))

        # Files count
        self.dash_files_label = ttk.Label(dash_stats, text="0 / 0 arquivos", font=("", 9))
        self.dash_files_label.pack(side=tk.LEFT)

        # Size stats row
        dash_sizes = ttk.Frame(dashboard_frame)
        dash_sizes.pack(fill=tk.X, pady=(5, 5))

        self.dash_processed_label = ttk.Label(dash_sizes, text="Processado: 0 TB", font=("", 9))
        self.dash_processed_label.pack(side=tk.LEFT, padx=(0, 20))

        self.dash_remaining_label = ttk.Label(dash_sizes, text="Restante: 0 TB", font=("", 9))
        self.dash_remaining_label.pack(side=tk.LEFT, padx=(0, 20))

        self.dash_saved_label = ttk.Label(dash_sizes, text="Economizado: 0 TB", font=("", 9), foreground="green")
        self.dash_saved_label.pack(side=tk.LEFT, padx=(0, 20))

        self.dash_estimate_label = ttk.Label(dash_sizes, text="Economia estimada: 0 TB", font=("", 9))
        self.dash_estimate_label.pack(side=tk.LEFT)

        # Performance row
        dash_perf = ttk.Frame(dashboard_frame)
        dash_perf.pack(fill=tk.X, pady=(5, 5))

        self.dash_speed_label = ttk.Label(dash_perf, text="Velocidade: 0 GB/h", font=("", 9))
        self.dash_speed_label.pack(side=tk.LEFT, padx=(0, 20))

        self.dash_compression_label = ttk.Label(dash_perf, text="Compressão: 0%", font=("", 9))
        self.dash_compression_label.pack(side=tk.LEFT, padx=(0, 20))

        self.dash_eta_label = ttk.Label(dash_perf, text="ETA: 0 dias", font=("", 9))
        self.dash_eta_label.pack(side=tk.LEFT)

        # Daily progress row
        dash_daily = ttk.Frame(dashboard_frame)
        dash_daily.pack(fill=tk.X, pady=(5, 0))

        ttk.Label(dash_daily, text="Últimos dias:", font=("", 8)).pack(side=tk.LEFT)
        self.dash_daily_label = ttk.Label(dash_daily, text="", font=("Consolas", 8))
        self.dash_daily_label.pack(side=tk.LEFT, padx=(5, 0))

        # Deleted GB row
        dash_deleted = ttk.Frame(dashboard_frame)
        dash_deleted.pack(fill=tk.X, pady=(5, 0))

        self.dash_deleted_today_label = ttk.Label(dash_deleted, text="Deletado hoje: 0 GB", font=("", 9), foreground="red")
        self.dash_deleted_today_label.pack(side=tk.LEFT, padx=(0, 20))

        self.dash_deleted_week_label = ttk.Label(dash_deleted, text="Últimos 7 dias: 0 GB", font=("", 9), foreground="red")
        self.dash_deleted_week_label.pack(side=tk.LEFT)

        # Global pending info row (clarifies global vs local)
        dash_pending = ttk.Frame(dashboard_frame)
        dash_pending.pack(fill=tk.X, pady=(5, 0))
        self.dash_global_pending_label = ttk.Label(
            dash_pending,
            text="💡 Dados do último SCAN. Clique SCAN para atualizar o inventário global.",
            font=("", 8), foreground="gray"
        )
        self.dash_global_pending_label.pack(side=tk.LEFT)

        # Initial dashboard load
        self.root.after(1000, self.refresh_dashboard)

        # === LOG FRAME ===
        log_frame = ttk.LabelFrame(main_frame, text="Log", padding="5")
        log_frame.pack(fill=tk.BOTH, expand=True)

        self.log_text = scrolledtext.ScrolledText(log_frame, height=15, font=("Consolas", 9))
        self.log_text.pack(fill=tk.BOTH, expand=True)

        # Configure tags for colored text
        self.log_text.tag_config("info", foreground="black")
        self.log_text.tag_config("success", foreground="green")
        self.log_text.tag_config("warning", foreground="orange")
        self.log_text.tag_config("error", foreground="red")

        self.log("Ready. Set your watch folder and click START.")

    def check_ffmpeg(self):
        """Check if FFmpeg is installed and accessible."""
        try:
            result = subprocess.run(['ffmpeg', '-version'], capture_output=True,
                                       text=True, encoding='utf-8', errors='replace', timeout=10)
            if result.returncode == 0:
                # Extract version info
                version_line = result.stdout.split('\n')[0] if result.stdout else 'unknown'
                self.log(f"FFmpeg found: {version_line[:60]}", "success")
            else:
                self.log("FFmpeg found but returned error", "warning")
        except FileNotFoundError:
            self.log("ERROR: FFmpeg not found! Please install FFmpeg.", "error")
            self.log("Download from: https://ffmpeg.org/download.html", "error")
            self.log("Or use: winget install ffmpeg", "info")
            messagebox.showerror("FFmpeg Not Found",
                "FFmpeg is not installed or not in PATH.\n\n"
                "Please install FFmpeg:\n"
                "1. Download from https://ffmpeg.org/download.html\n"
                "2. Or run: winget install ffmpeg\n\n"
                "After installing, restart this application.")
        except Exception as e:
            self.log(f"Error checking FFmpeg: {e}", "warning")

    def browse_folder(self):
        """Open folder browser dialog. Appends with semicolon if holding Shift or if current has folders."""
        # Get current value
        current = self.watch_folder.get().strip()

        # Use last folder as initial dir, or current if only one
        if ';' in current:
            parts = [p.strip() for p in current.split(';') if p.strip()]
            initial = parts[-1] if parts else current
        else:
            initial = current

        folder = filedialog.askdirectory(initialdir=initial)
        if folder:
            # Check if folder is already in the list
            existing = [p.strip() for p in current.split(';') if p.strip()]
            if folder not in existing:
                if current and current != str(self.dropbox_base):
                    # Append with semicolon
                    self.watch_folder.set(f"{current}; {folder}")
                else:
                    # Replace
                    self.watch_folder.set(folder)
            self.save_settings()

    def browse_log_folder(self):
        """Open log folder browser dialog."""
        folder = filedialog.askdirectory(initialdir=self.log_folder.get())
        if folder:
            self.log_folder.set(folder)
            self.save_settings()

    def _on_delete_h264_toggle(self):
        """Handle toggle of auto-delete h264 checkbox."""
        if self.auto_delete_h264.get():
            # User is enabling - show confirmation
            result = messagebox.askyesno(
                "Confirmar Exclusão Automática",
                "ATENÇÃO: Esta opção irá DELETAR PERMANENTEMENTE a PASTA h264 inteira "
                "após 20 minutos da conversão.\n\n"
                "A pasta só será deletada quando:\n"
                "• TODOS os arquivos h265 correspondentes existirem\n"
                "• TODOS os h265 forem verificados como funcionais\n"
                "• 20 minutos terem passado para o Dropbox sincronizar\n\n"
                "Isso é importante para a recuperação via histórico do Dropbox.\n\n"
                "Esta ação é IRREVERSÍVEL!\n\n"
                "Deseja realmente ativar a exclusão automática?",
                icon='warning'
            )
            if not result:
                # User clicked No - uncheck the box
                self.auto_delete_h264.set(False)
            else:
                self.log("Auto-delete h264 ENABLED - folders will be deleted after all files verified", "warning")
                self.save_settings()
        else:
            self.log("Auto-delete h264 disabled - backups will be kept", "info")
            self.save_settings()

    def load_settings(self):
        """Load settings from file."""
        try:
            if self.SETTINGS_FILE.exists():
                with open(self.SETTINGS_FILE, 'r', encoding='utf-8') as f:
                    settings = json.load(f)
                    self.watch_folder.set(settings.get('watch_folder', self.watch_folder.get()))
                    self.log_folder.set(settings.get('log_folder', self.log_folder.get()))
                    self.encoder.set(settings.get('encoder', self.encoder.get()))
                    self.cq_value.set(settings.get('cq_value', self.cq_value.get()))
                    self.min_size_gb.set(settings.get('min_size_gb', self.min_size_gb.get()))
                    self.auto_start.set(settings.get('auto_start', True))  # Default to auto-start enabled
                    # SAFETY: auto_delete_h264 ALWAYS starts unchecked, never loaded from settings
                    # User must explicitly enable it each session
                    self.auto_delete_h264.set(False)
        except Exception:
            pass  # Use defaults if settings can't be loaded

    def save_settings(self):
        """Save settings to file."""
        try:
            self.SETTINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
            settings = {
                'watch_folder': self.watch_folder.get(),
                'log_folder': self.log_folder.get(),
                'encoder': self.encoder.get(),
                'cq_value': self.cq_value.get(),
                'min_size_gb': self.min_size_gb.get(),
                'auto_delete_h264': self.auto_delete_h264.get(),
                'auto_start': self.auto_start.get()
            }
            with open(self.SETTINGS_FILE, 'w', encoding='utf-8') as f:
                json.dump(settings, f, indent=2)
        except Exception:
            pass  # Silently fail if can't save

    def on_close(self):
        """Handle window close."""
        self.save_settings()
        self.running = False
        # Save cloud manifest before closing
        if self.cloud_manifest:
            try:
                self.cloud_manifest.close()
            except Exception:
                pass
        self.root.destroy()

    def _move_with_retry(self, src: Path, dst: Path, max_retries: int = 5):
        """
        Move file with retry logic and exponential backoff.
        Handles file locks from FFmpeg/Dropbox — WinError 32 (OSError) and PermissionError.
        Backoff: 2s → 5s → 10s → 20s → 30s
        """
        delays = [2, 5, 10, 20, 30]  # Exponential backoff
        last_error = None
        for attempt in range(max_retries):
            try:
                shutil.move(str(src), str(dst))
                return  # Success
            except (PermissionError, OSError) as e:
                last_error = e
                if attempt < max_retries - 1:
                    delay = delays[min(attempt, len(delays)-1)]
                    self.root.after(0, lambda a=attempt+1, d=delay, err=e: self.log(
                        f"File locked ({err}), retry {a}/{max_retries-1} in {d}s...", "info"))
                    time.sleep(delay)

        # All retries failed
        raise last_error


    def notify_queue_finished(self):
        """Play beep and unminimize window when encoding queue finishes."""
        # Beep sound
        if HAS_WINSOUND:
            try:
                # Play 3 beeps
                winsound.Beep(800, 200)  # frequency, duration_ms
                time.sleep(0.1)
                winsound.Beep(1000, 200)
                time.sleep(0.1)
                winsound.Beep(1200, 300)
            except Exception:
                pass

        # Unminimize and bring to front
        def bring_to_front():
            self.root.deiconify()  # Restore if minimized
            self.root.lift()       # Bring to front
            self.root.focus_force()  # Force focus
            self.log("✓ Queue finished! All files processed.", "success")

        self.root.after(0, bring_to_front)

    def get_machine_name(self) -> str:
        """Get machine name for log files."""
        try:
            return socket.gethostname()
        except Exception:
            return "unknown"

    def write_success_log(self, input_path: Path, output_path: Path, input_size: int, output_size: int):
        """Write successful encoding to log file."""
        try:
            log_folder = Path(self.log_folder.get())
            log_folder.mkdir(parents=True, exist_ok=True)
            machine_name = self.get_machine_name()
            log_file = log_folder / f"encoding_history_{machine_name}.log"

            reduction = (1 - output_size / input_size) * 100 if input_size > 0 else 0
            input_gb = input_size / (1024**3)
            output_gb = output_size / (1024**3)
            saved_gb = (input_size - output_size) / (1024**3)

            log_entry = (
                f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
                f"SUCCESS | {input_path.name} | "
                f"{input_gb:.2f}GB -> {output_gb:.2f}GB | "
                f"Saved: {saved_gb:.2f}GB ({reduction:.1f}%) | "
                f"Encoder: {self.encoder.get()} CQ:{self.cq_value.get()}\n"
            )

            with open(log_file, 'a', encoding='utf-8') as f:
                f.write(log_entry)
        except Exception as e:
            self.root.after(0, lambda: self.log(f"Could not write to log file: {e}", "warning"))

    def write_h265_done_log(self, h265_folder: Path, filename: str, input_size: int, output_size: int):
        """Write 'h265 feito.txt' log file in h265 folder."""
        try:
            h265_folder.mkdir(parents=True, exist_ok=True)
            log_file = h265_folder / "h265 feito.txt"

            reduction = (1 - output_size / input_size) * 100 if input_size > 0 else 0
            input_mb = input_size / (1024**2)
            output_mb = output_size / (1024**2)

            log_entry = (
                f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
                f"{filename} | "
                f"{input_mb:.1f}MB -> {output_mb:.1f}MB ({reduction:.1f}% menor)\n"
            )

            with open(log_file, 'a', encoding='utf-8') as f:
                f.write(log_entry)
        except Exception:
            pass  # Silent fail - not critical

    def open_folder(self):
        """Open watch folder in explorer."""
        folder = Path(self.watch_folder.get())
        if folder.exists():
            subprocess.Popen(f'explorer "{folder}"')
        else:
            messagebox.showerror("Error", "Folder does not exist")

    def generate_report(self):
        """Generate a technical report of the folder contents."""
        threading.Thread(target=self._do_generate_report, daemon=True).start()

    def _do_generate_report(self):
        """Worker for generating the technical report."""
        folder = Path(self.watch_folder.get())

        if not folder.exists():
            self.root.after(0, lambda: messagebox.showerror("Error", "Folder does not exist"))
            return

        self.root.after(0, lambda: self.log("Generating report... please wait", "info"))
        self.root.after(0, lambda: self.current_file_label.config(text="Scanning folder for report..."))

        try:
            # Collect data
            all_videos = []
            h264_backups = []
            h265_outputs = []

            # Scan all .mp4 video files (skip ._ metadata files from macOS/ATEM)
            for f in folder.rglob('*.mp4'):
                    # Skip macOS/ATEM metadata files
                    if f.name.startswith('._'):
                        continue
                    try:
                        size = f.stat().st_size
                        rel_path = str(f.relative_to(folder))

                        # Check if it's in h264 backup folder
                        if '/h264/' in rel_path or '\\h264\\' in rel_path:
                            h264_backups.append((f, size))
                        # Check if it's in h265 output folder
                        elif '/h265/' in rel_path or '\\h265\\' in rel_path:
                            h265_outputs.append((f, size))
                        else:
                            all_videos.append((f, size))
                    except Exception:
                        pass

            # Get database stats
            with self.db_lock:
                cursor = self.db_conn.execute(
                    "SELECT COUNT(*), SUM(input_size), SUM(output_size) FROM processed WHERE status = 'done'"
                )
                db_row = cursor.fetchone()
                db_count = db_row[0] or 0
                db_input_total = db_row[1] or 0
                db_output_total = db_row[2] or 0

                # Get orphan entries (files in DB but no longer exist)
                cursor = self.db_conn.execute("SELECT input_path FROM processed WHERE status = 'done'")
                db_done_paths = [row[0] for row in cursor]
            orphan_count = 0
            for p in db_done_paths:
                if not Path(p).exists():
                    orphan_count += 1

            # Separate pending from already processed
            pending_videos = []
            for f, size in all_videos:
                if not self.is_processed(f):
                    pending_videos.append((f, size))

            # Check which pending files are local vs cloud
            # IMPORTANT: Don't open files or run ffprobe here - it triggers Dropbox downloads!
            # Use file attributes instead (attrib command on Windows)
            local_pending = []
            cloud_pending = []

            for f, size in pending_videos:
                try:
                    # Method 1: Check if file size is suspiciously small (placeholder)
                    if size < 10000:  # Less than 10KB = definitely a placeholder
                        cloud_pending.append((f, size))
                        continue

                    # Method 2: Use attrib to check Unpinned attribute (Windows/Dropbox)
                    # Files with 'U' attribute are online-only
                    try:
                        result = subprocess.run(
                            ['attrib', str(f)],
                            capture_output=True, text=True,
                            encoding='utf-8', errors='replace', timeout=5,
                            creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, 'CREATE_NO_WINDOW') else 0
                        )
                        # attrib output format: "A  U        C:\path\file.mp4"
                        # U = Unpinned (online-only), P = Pinned (local)
                        attrs = result.stdout.strip()[:20] if result.stdout else ""
                        if 'U' in attrs and 'P' not in attrs:
                            cloud_pending.append((f, size))
                        else:
                            local_pending.append((f, size))
                    except Exception:
                        # If attrib fails, assume local based on size
                        local_pending.append((f, size))
                except Exception:
                    cloud_pending.append((f, size))

            # Calculate sizes
            total_pending_size = sum(s for _, s in pending_videos)
            local_pending_size = sum(s for _, s in local_pending)
            cloud_pending_size = sum(s for _, s in cloud_pending)
            h264_backup_size = sum(s for _, s in h264_backups)
            h265_output_size = sum(s for _, s in h265_outputs)

            # Calculate compression ratio from database
            if db_input_total > 0:
                compression_ratio = (1 - db_output_total / db_input_total) * 100
                space_saved = db_input_total - db_output_total
            else:
                compression_ratio = 46  # Estimated default
                space_saved = 0

            # Estimate future savings
            estimated_savings = total_pending_size * (compression_ratio / 100)

            # Size distribution
            size_small = [(f, s) for f, s in pending_videos if s < 100 * 1024 * 1024]
            size_medium = [(f, s) for f, s in pending_videos if 100 * 1024 * 1024 <= s < 1024 * 1024 * 1024]
            size_large = [(f, s) for f, s in pending_videos if 1024 * 1024 * 1024 <= s < 5 * 1024 * 1024 * 1024]
            size_xlarge = [(f, s) for f, s in pending_videos if s >= 5 * 1024 * 1024 * 1024]

            # Extension distribution
            ext_dist = {}
            for f, s in pending_videos:
                ext = f.suffix.lower()
                if ext not in ext_dist:
                    ext_dist[ext] = {'count': 0, 'size': 0}
                ext_dist[ext]['count'] += 1
                ext_dist[ext]['size'] += s

            # Top folders by pending size
            folder_sizes = {}
            for f, s in pending_videos:
                parent = str(f.parent.relative_to(folder))
                if parent not in folder_sizes:
                    folder_sizes[parent] = {'count': 0, 'size': 0}
                folder_sizes[parent]['count'] += 1
                folder_sizes[parent]['size'] += s

            top_folders = sorted(folder_sizes.items(), key=lambda x: x[1]['size'], reverse=True)[:10]

            # Helper function for formatting
            def fmt_size(bytes_val):
                if bytes_val >= 1024**4:
                    return f"{bytes_val / (1024**4):.2f} TB"
                elif bytes_val >= 1024**3:
                    return f"{bytes_val / (1024**3):.2f} GB"
                elif bytes_val >= 1024**2:
                    return f"{bytes_val / (1024**2):.1f} MB"
                else:
                    return f"{bytes_val / 1024:.0f} KB"

            def pct_bar(pct, width=20):
                filled = int(pct / 100 * width)
                return '█' * filled + '░' * (width - filled)

            # Build report
            machine_name = self.get_machine_name()
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            report = []
            report.append("╔" + "═" * 78 + "╗")
            report.append(f"║{'RELATÓRIO TÉCNICO - HeavyDrops Transcoder v' + VERSION:^78}║")
            report.append(f"║{'Máquina: ' + machine_name:^78}║")
            report.append(f"║{'Gerado em: ' + timestamp:^78}║")
            report.append("╠" + "═" * 78 + "╣")
            report.append(f"║  Pasta: {str(folder)[:68]:<68}  ║")
            report.append("╠" + "═" * 78 + "╣")
            report.append(f"║{'RESUMO GERAL':^78}║")
            report.append("╠" + "═" * 78 + "╣")

            total_all = len(all_videos) + len(h264_backups) + len(h265_outputs)
            total_size_all = sum(s for _, s in all_videos) + h264_backup_size + h265_output_size

            report.append(f"║  Total de vídeos encontrados: {total_all:>8} arquivos  │  {fmt_size(total_size_all):>12}  ║")
            report.append(f"║  ├─ Pendentes para conversão: {len(pending_videos):>8} arquivos  │  {fmt_size(total_pending_size):>12}  ║")
            report.append(f"║  ├─ Já convertidos (no banco):{db_count:>8} arquivos  │  {fmt_size(db_output_total):>12}  ║")
            report.append(f"║  └─ Backups em pastas h264/:  {len(h264_backups):>8} arquivos  │  {fmt_size(h264_backup_size):>12}  ║")

            report.append("╠" + "═" * 78 + "╣")
            report.append(f"║{'ECONOMIA DE ESPAÇO':^78}║")
            report.append("╠" + "═" * 78 + "╣")
            report.append(f"║  Taxa média de compressão alcançada:        {compression_ratio:>5.1f}% (baseado em {db_count} arquivos)  ║")
            report.append(f"║  Espaço economizado até agora:              {fmt_size(space_saved):>15}              ║")
            report.append(f"║  Economia estimada após conversão total:    {fmt_size(estimated_savings):>15}              ║")

            report.append("╠" + "═" * 78 + "╣")
            report.append(f"║{'DISPONIBILIDADE DOS ARQUIVOS (Dropbox Smart Sync)':^78}║")
            report.append("╠" + "═" * 78 + "╣")
            report.append(f"║  Pendentes disponíveis localmente:  {len(local_pending):>6} arquivos  │  {fmt_size(local_pending_size):>12}  ║")
            report.append(f"║  Pendentes somente na nuvem:        {len(cloud_pending):>6} arquivos  │  {fmt_size(cloud_pending_size):>12}  ║")
            report.append(f"║  ► Prontos para conversão imediata: {len(local_pending):>6} arquivos                     ║")

            report.append("╠" + "═" * 78 + "╣")
            report.append(f"║{'CODEC DOS ARQUIVOS':^78}║")
            report.append("╠" + "═" * 78 + "╣")
            report.append(f"║  Presumido H.264:  {len(pending_videos):>6} arquivos  │  {fmt_size(total_pending_size):>12}                ║")
            report.append(f"║  (Verificação de codec desabilitada para não baixar arquivos)            ║")

            report.append("╠" + "═" * 78 + "╣")
            report.append(f"║{'DISTRIBUIÇÃO POR TAMANHO (Pendentes)':^78}║")
            report.append("╠" + "═" * 78 + "╣")

            if len(pending_videos) > 0:
                pct_small = sum(s for _, s in size_small) / total_pending_size * 100 if total_pending_size > 0 else 0
                pct_medium = sum(s for _, s in size_medium) / total_pending_size * 100 if total_pending_size > 0 else 0
                pct_large = sum(s for _, s in size_large) / total_pending_size * 100 if total_pending_size > 0 else 0
                pct_xlarge = sum(s for _, s in size_xlarge) / total_pending_size * 100 if total_pending_size > 0 else 0

                report.append(f"║  Pequenos   (< 100 MB):   {len(size_small):>5} arq │ {fmt_size(sum(s for _,s in size_small)):>10} │ {pct_bar(pct_small, 10)} {pct_small:>4.0f}%  ║")
                report.append(f"║  Médios     (100MB-1GB):  {len(size_medium):>5} arq │ {fmt_size(sum(s for _,s in size_medium)):>10} │ {pct_bar(pct_medium, 10)} {pct_medium:>4.0f}%  ║")
                report.append(f"║  Grandes    (1GB-5GB):    {len(size_large):>5} arq │ {fmt_size(sum(s for _,s in size_large)):>10} │ {pct_bar(pct_large, 10)} {pct_large:>4.0f}%  ║")
                report.append(f"║  Muito grandes (> 5GB):   {len(size_xlarge):>5} arq │ {fmt_size(sum(s for _,s in size_xlarge)):>10} │ {pct_bar(pct_xlarge, 10)} {pct_xlarge:>4.0f}%  ║")
            else:
                report.append(f"║  {'Nenhum arquivo pendente':^74}  ║")

            report.append("╠" + "═" * 78 + "╣")
            report.append(f"║{'DISTRIBUIÇÃO POR EXTENSÃO (Pendentes)':^78}║")
            report.append("╠" + "═" * 78 + "╣")

            if ext_dist:
                sorted_ext = sorted(ext_dist.items(), key=lambda x: x[1]['size'], reverse=True)
                for ext, data in sorted_ext[:5]:
                    pct = data['size'] / total_pending_size * 100 if total_pending_size > 0 else 0
                    report.append(f"║  {ext:<6}  {data['count']:>6} arquivos │ {fmt_size(data['size']):>10} │ {pct_bar(pct, 15)} {pct:>4.0f}%  ║")
            else:
                report.append(f"║  {'Nenhum arquivo pendente':^74}  ║")

            report.append("╠" + "═" * 78 + "╣")
            report.append(f"║{'TOP 10 PASTAS COM MAIS CONTEÚDO PENDENTE':^78}║")
            report.append("╠" + "═" * 78 + "╣")

            if top_folders:
                for i, (folder_name, data) in enumerate(top_folders, 1):
                    folder_display = folder_name[:45] if len(folder_name) <= 45 else "..." + folder_name[-42:]
                    report.append(f"║  {i:>2}. {folder_display:<45} {data['count']:>4} arq │ {fmt_size(data['size']):>10}  ║")
            else:
                report.append(f"║  {'Nenhuma pasta com arquivos pendentes':^74}  ║")

            report.append("╠" + "═" * 78 + "╣")
            report.append(f"║{'SAÚDE DO BANCO DE DADOS':^78}║")
            report.append("╠" + "═" * 78 + "╣")
            report.append(f"║  Registros no banco:                        {db_count:>8}                         ║")
            report.append(f"║  Backups h264 encontrados:                  {len(h264_backups):>8}  {'✓' if len(h264_backups) >= db_count else '⚠'}                        ║")
            report.append(f"║  Entradas órfãs (arquivo não existe mais):  {orphan_count:>8}  {'✓' if orphan_count == 0 else '⚠'}                        ║")

            # Cloud Manifest stats
            report.append("╠" + "═" * 78 + "╣")
            report.append(f"║{'CLOUD MANIFEST (persistente no Dropbox)':^78}║")
            report.append("╠" + "═" * 78 + "╣")
            if self.cloud_manifest:
                cm_stats = self.cloud_manifest.manifest['stats']
                cm_path = str(self.cloud_manifest.get_manifest_path())
                cm_path_display = cm_path[:70] if len(cm_path) <= 70 else "..." + cm_path[-67:]
                saved_gb = cm_stats['total_saved_bytes'] / (1024**3)
                total_in = cm_stats['total_input_bytes']
                total_out = cm_stats['total_output_bytes']
                avg_ratio = total_out / total_in if total_in > 0 else 0.25
                trans_sec = cm_stats['total_transcode_seconds']
                speed_gbh = (total_in / (1024**3)) / (trans_sec / 3600) if trans_sec > 0 else 0
                failed_count = len(self.cloud_manifest.manifest['failed_files'])
                report.append(f"║  PC: {self.pc_name:<25} Caminho: {cm_path_display:<40}  ║")
                report.append(f"║  Total processados (histórico):              {cm_stats['total_files_processed']:>8}                         ║")
                report.append(f"║  Total economizado (histórico):              {saved_gb:>8.1f} GB                      ║")
                report.append(f"║  Taxa média de compressão:                   {(1 - avg_ratio) * 100:>8.1f}%                       ║")
                report.append(f"║  Velocidade média:                           {speed_gbh:>8.1f} GB/h                     ║")
                report.append(f"║  Arquivos com falha:                         {failed_count:>8}  {'✓' if failed_count == 0 else '⚠'}                        ║")
            else:
                report.append(f"║  {'Cloud manifest não disponível':^74}  ║")

            report.append("╠" + "═" * 78 + "╣")
            report.append(f"║{'ALERTAS':^78}║")
            report.append("╠" + "═" * 78 + "╣")

            alerts = []
            if len(size_xlarge) > 0:
                alerts.append(f"⚠ {len(size_xlarge)} arquivo(s) muito grande(s) (>5GB) podem demorar bastante")
            if orphan_count > 0:
                alerts.append(f"⚠ {orphan_count} entrada(s) órfã(s) no banco podem ser limpas")
            if len(cloud_pending) > len(local_pending):
                alerts.append(f"⚠ Maioria dos arquivos está na nuvem ({len(cloud_pending)} de {len(pending_videos)})")
            if not alerts:
                alerts.append("✓ Nenhum alerta - tudo OK!")

            for alert in alerts[:5]:
                report.append(f"║  {alert:<74}  ║")

            report.append("╚" + "═" * 78 + "╝")

            # Display report
            report_text = "\n".join(report)

            # Save report to file
            log_folder = Path(self.log_folder.get())
            log_folder.mkdir(parents=True, exist_ok=True)
            report_file = log_folder / f"report_{machine_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(report_text)

            # Show in new window
            def show_report_window():
                report_win = tk.Toplevel(self.root)
                report_win.title(f"Relatório Técnico - {folder.name}")
                report_win.geometry("700x800")

                text_widget = scrolledtext.ScrolledText(report_win, font=("Consolas", 9), wrap=tk.NONE)
                text_widget.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
                text_widget.insert(tk.END, report_text)
                text_widget.config(state=tk.DISABLED)

                # Buttons frame
                btn_frame = ttk.Frame(report_win)
                btn_frame.pack(fill=tk.X, padx=10, pady=(0, 10))

                ttk.Button(btn_frame, text="Salvar como...",
                    command=lambda: self._save_report_as(report_text)).pack(side=tk.LEFT, padx=(0, 5))
                ttk.Button(btn_frame, text="Fechar",
                    command=report_win.destroy).pack(side=tk.RIGHT)
                ttk.Label(btn_frame, text=f"Salvo em: {report_file.name}",
                    font=("", 8)).pack(side=tk.LEFT, padx=10)

            self.root.after(0, show_report_window)
            self.root.after(0, lambda: self.current_file_label.config(text="Idle"))
            self.root.after(0, lambda: self.log(f"Report saved: {report_file.name}", "success"))

        except Exception as e:
            self.root.after(0, lambda err=e: self.log(f"Error generating report: {err}", "error"))
            self.root.after(0, lambda: self.current_file_label.config(text="Idle"))

    def _save_report_as(self, report_text: str):
        """Save report to a user-selected location."""
        file_path = filedialog.asksaveasfilename(
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
            initialfile=f"report_{self.get_machine_name()}_{datetime.now().strftime('%Y%m%d')}.txt"
        )
        if file_path:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(report_text)
            self.log(f"Report saved to: {file_path}", "success")

    def log(self, message, tag="info"):
        """Add message to log (Brasilia Time UTC-3)."""
        from datetime import timezone, timedelta
        brt = timezone(timedelta(hours=-3))
        timestamp = datetime.now(brt).strftime("%H:%M:%S")
        self.log_text.insert(tk.END, f"[{timestamp}] {message}\n", tag)
        self.log_text.see(tk.END)

    def setup_database(self):
        """Initialize database."""
        db_path = Path(r"C:\transcoder\transcoder_gui.db")
        db_path.parent.mkdir(parents=True, exist_ok=True)

        self.db_conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self.db_conn.execute("PRAGMA journal_mode=WAL")  # WAL mode for concurrent reads
        self.db_lock = threading.Lock()
        self.db_conn.execute("""
            CREATE TABLE IF NOT EXISTS processed (
                id INTEGER PRIMARY KEY,
                input_path TEXT UNIQUE,
                output_path TEXT,
                status TEXT,
                input_size INTEGER,
                output_size INTEGER,
                processed_at TEXT
            )
        """)
        self.db_conn.commit()

    def setup_cloud_manifest(self):
        """Initialize cloud manifest for persistent state in Dropbox."""
        try:
            self.log(f"Dropbox base detectado: {self.dropbox_base}", "info")

            # Check if path exists
            if not self.dropbox_base.exists():
                self.log(f"Criando pasta: {self.dropbox_base}", "info")
                self.dropbox_base.mkdir(parents=True, exist_ok=True)

            self.cloud_manifest = ManifestManager(
                base_dropbox_path=str(self.dropbox_base)
            )
            manifest_path = self.cloud_manifest.get_manifest_path()
            self.log(f"Cloud manifest: {manifest_path}", "success")

            stats = self.cloud_manifest.get_stats_summary()
            self.log(f"PC: {self.pc_name} | Processados: {stats['processed']} | Skipados: {stats['skipped']} | Salvos: {stats['saved_tb']:.2f} TB", "info")
        except Exception as e:
            import traceback
            self.log(f"Cloud manifest error: {e}", "warning")
            self.log(f"Traceback: {traceback.format_exc()}", "warning")
            self.cloud_manifest = None

    def refresh_dashboard(self):
        """Refresh dashboard with latest manifest data."""
        if not self.cloud_manifest:
            self.dash_last_update.config(text="Manifest não disponível")
            return

        try:
            # Reload from disk to get updates from other PCs
            self.cloud_manifest.refresh()
            data = self.cloud_manifest.get_dashboard_data()

            # Update last update info
            last_update = data['last_updated'][:19].replace('T', ' ') if data['last_updated'] else 'nunca'
            self.dash_last_update.config(text=f"Atualizado: {last_update} por {data['last_updated_by']}")

            # Active PCs
            pcs = ', '.join(data['active_pcs'][:5])
            if len(data['active_pcs']) > 5:
                pcs += f" +{len(data['active_pcs'])-5}"
            self.dash_active_pcs.config(text=f"PCs: {pcs}")

            # Skipped count
            skipped = data.get('skipped_count', 0)
            self.dash_skipped_label.config(text=f"Skipados: {skipped:,}" if skipped > 0 else "")

            # Progress
            pct = data['progress_percent']
            self.dash_progress_var.set(pct)
            self.dash_progress_pct.config(text=f"{pct:.1f}%")

            total = data['total_processed'] + data['total_to_process']
            self.dash_files_label.config(text=f"{data['total_processed']:,} / {total:,} arquivos")

            # Sizes
            self.dash_processed_label.config(text=f"Processado: {data['processed_tb']:.2f} TB")
            self.dash_remaining_label.config(text=f"Restante: {data['to_process_tb']:.2f} TB")
            self.dash_saved_label.config(text=f"Economizado: {data['saved_tb']:.2f} TB")
            self.dash_estimate_label.config(text=f"Economia total estimada: {data['estimated_total_savings_tb']:.2f} TB")

            # Performance - use hourly speed if available, otherwise historical
            hourly_speed = self.get_hourly_speed_gbh()
            speed_display = hourly_speed if hourly_speed > 0 else data['avg_speed_gbh']
            speed_label = f"Velocidade: {speed_display:.1f} GB/h" + (" (última hora)" if hourly_speed > 0 else " (histórico)")
            self.dash_speed_label.config(text=speed_label)
            self.dash_compression_label.config(text=f"Compressão: {data['avg_compression']:.1f}%")
            self.dash_eta_label.config(text=f"ETA: {data['days_remaining']:.0f} dias")

            # Daily progress (last 7 days as text)
            daily_text = ""
            for day in data['daily_progress'][:7]:
                date_short = day['date'][5:]  # MM-DD
                daily_text += f"[{date_short}: {day['files']}arq {day['gb_saved']:.0f}GB] "
            self.dash_daily_label.config(text=daily_text.strip())

            # Deleted GB stats
            deleted_today = self.get_deleted_gb_today()
            deleted_week = self.get_deleted_gb_last_days(7)
            self.dash_deleted_today_label.config(text=f"Deletado hoje: {deleted_today:.1f} GB")
            self.dash_deleted_week_label.config(text=f"Últimos 7 dias: {deleted_week:.1f} GB")

            self.log("Dashboard atualizado", "info")
        except Exception as e:
            self.log(f"Erro ao atualizar dashboard: {e}", "warning")

    def show_ready_queue(self):
        """Show popup with list of files in the ready queue."""
        # Get queue contents (without removing items)
        queue_items = []
        temp_items = []

        # Empty queue into temp list
        while not self.ready_queue.empty():
            try:
                item = self.ready_queue.get_nowait()
                temp_items.append(item)
            except Exception:
                break

        # Put items back and build display list
        for item in temp_items:
            self.ready_queue.put(item)
            # Item can be (path, size) or (path, size, priority)
            path = item[0]
            size = item[1]
            size_gb = size / (1024**3)
            queue_items.append(f"{path.name}  ({size_gb:.2f} GB)\n   {path.parent}")

        # Create popup window
        popup = tk.Toplevel(self.root)
        popup.title(f"Ready Queue - {len(queue_items)} files")
        popup.geometry("700x500")
        popup.transient(self.root)

        # Header
        header = ttk.Frame(popup, padding="10")
        header.pack(fill=tk.X)
        ttk.Label(header, text=f"Files ready to transcode: {len(queue_items)}", font=("", 11, "bold")).pack(side=tk.LEFT)

        total_size = sum(item[1] for item in temp_items) / (1024**3)
        ttk.Label(header, text=f"Total: {total_size:.2f} GB", font=("", 10)).pack(side=tk.RIGHT)

        # List
        list_frame = ttk.Frame(popup, padding="10")
        list_frame.pack(fill=tk.BOTH, expand=True)

        text_widget = tk.Text(list_frame, wrap=tk.WORD, font=("Consolas", 9))
        scrollbar = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=text_widget.yview)
        text_widget.configure(yscrollcommand=scrollbar.set)

        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        text_widget.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        if queue_items:
            for i, item in enumerate(queue_items, 1):
                text_widget.insert(tk.END, f"{i}. {item}\n\n")
        else:
            text_widget.insert(tk.END, "Queue is empty.\n\nThe ready queue worker adds files here when they are:\n• Downloaded (not cloud-only)\n• Not already HEVC\n• Not low bitrate (<8 Mbps)\n• Not already processed")

        text_widget.config(state=tk.DISABLED)

        # Close button
        ttk.Button(popup, text="Close", command=popup.destroy).pack(pady=10)

    def run_inventory_scan(self):
        """Run inventory scan via Dropbox API (no downloads)."""
        if self.running:
            messagebox.showwarning("Scan", "Pare o transcoder antes de rodar o scan!")
            return

        watch_folder = self.watch_folder.get()
        if not watch_folder:
            messagebox.showwarning("Scan", "Configure a pasta do Dropbox primeiro!")
            return

        self.log("=" * 60, "info")
        self.log("INICIANDO SCAN DO INVENTÁRIO (sem downloads)", "info")
        self.log(f"Pasta: {watch_folder}", "info")
        self.log("=" * 60, "info")

        # Run scan in thread to not block UI
        def scan_thread():
            try:
                self._do_inventory_scan(watch_folder)
            except Exception as e:
                self.root.after(0, lambda: self.log(f"Erro no scan: {e}", "error"))

        threading.Thread(target=scan_thread, daemon=True).start()

    def _do_inventory_scan(self, watch_folder: str):
        """Perform the inventory scan."""
        import os
        from pathlib import Path

        watch_path = Path(watch_folder)
        if not watch_path.exists():
            self.root.after(0, lambda: self.log(f"Pasta não existe: {watch_folder}", "error"))
            return

        # Stats
        total_files = 0
        total_size = 0
        needs_transcoding = 0
        needs_transcoding_size = 0
        already_done = 0
        already_h265 = 0
        skipped_small = 0
        h265_logs_found = 0

        # Track WHERE pending files are (folder -> count)
        pending_by_folder = {}
        pending_files_list = []  # List of (path, size) for download triggering

        min_size_bytes = int(self.min_size_gb.get() * 1024 * 1024 * 1024)
        video_extensions = {'.mp4', '.mkv', '.avi', '.mov', '.wmv', '.m4v', '.webm'}

        self.root.after(0, lambda: self.log("Escaneando arquivos locais...", "info"))

        # First, import all h265 feitos.txt files we find (batch - no individual logging)
        total_h265_entries_imported = 0
        if self.cloud_manifest:
            for root, dirs, files in os.walk(watch_path):
                for f in files:
                    if f == "h265 feito.txt":
                        log_path = os.path.join(root, f)
                        try:
                            with open(log_path, 'r', encoding='utf-8', errors='ignore') as fp:
                                content = fp.read()
                            imported = self.cloud_manifest.import_h265_feitos_txt(log_path, content)
                            if imported > 0:
                                h265_logs_found += 1
                                total_h265_entries_imported += imported
                        except Exception as e:
                            pass
            # Log summary only once at the end
            if h265_logs_found > 0:
                self.root.after(0, lambda n=h265_logs_found, e=total_h265_entries_imported:
                    self.log(f"Importado {e} entradas de {n} arquivos 'h265 feito.txt'", "success"))

        # Now scan all video files
        for root, dirs, files in os.walk(watch_path):
            # Skip h264 and h265 backup folders
            dirs[:] = [d for d in dirs if d.lower() not in ('h264', 'h265')]

            for f in files:
                ext = os.path.splitext(f)[1].lower()
                if ext not in video_extensions:
                    continue

                file_path = os.path.join(root, f)
                try:
                    size = os.path.getsize(file_path)
                except Exception:
                    continue

                total_files += 1
                total_size += size

                # Update progress every 100 files
                if total_files % 100 == 0:
                    msg = f"Escaneando... {total_files} arquivos ({total_size / (1024**4):.2f} TB)"
                    self.root.after(0, lambda m=msg: self.log(m, "info"))

                # Check if already processed
                if self.cloud_manifest and self.cloud_manifest.is_processed(file_path):
                    already_done += 1
                    continue

                # Check if already skipped
                if self.cloud_manifest and self.cloud_manifest.is_skipped(file_path):
                    continue

                # Skip DJI drone files (user wants originals preserved)
                if f.upper().startswith('DJI_'):
                    if self.cloud_manifest:
                        self.cloud_manifest.record_skipped(file_path, "drone_dji", size)
                    continue

                # Check if too small
                if size < min_size_bytes:
                    skipped_small += 1
                    if self.cloud_manifest:
                        self.cloud_manifest.record_skipped(file_path, "too_small", size)
                    continue

                # Check if already H.265 (quick check by extension and name)
                if '_h265' in f.lower() or '.hevc' in f.lower():
                    already_h265 += 1
                    if self.cloud_manifest:
                        self.cloud_manifest.record_skipped(file_path, "already_h265", size)
                    continue

                # Check if in h265 feito log (already checked during is_processed via manifest)
                if self._is_in_h265_feito_log(Path(file_path)):
                    already_done += 1
                    continue

                # File needs transcoding
                needs_transcoding += 1
                needs_transcoding_size += size

                # Track which folder this file is in
                folder_name = os.path.basename(root)
                pending_by_folder[folder_name] = pending_by_folder.get(folder_name, 0) + 1

                # Add to pending files list (for download triggering)
                pending_files_list.append((file_path, size))

        # Update manifest with estimates
        if self.cloud_manifest:
            self.cloud_manifest.update_estimates(needs_transcoding, needs_transcoding_size)
            self.cloud_manifest.save(force=True)

        # Smart download: only trigger downloads if ready queue needs more files (target: 50 minimum)
        if pending_files_list and not self.offline_mode.get():
            current_queue_size = self.ready_queue.qsize()
            downloads_needed = max(0, 50 - current_queue_size)

            if downloads_needed > 0:
                # Sort by size (smaller first) for faster progress
                pending_files_list.sort(key=lambda x: x[1])
                files_to_trigger = pending_files_list[:downloads_needed]

                self.root.after(0, lambda n=len(files_to_trigger), q=current_queue_size: self.log(
                    f"Ready queue has {q} files. Triggering downloads for {n} more...", "info"))

                for file_path, size in files_to_trigger:
                    if len(self.pending_downloads) < self.max_pending_downloads:
                        self._add_to_pending_downloads(file_path, size)
                        self._trigger_dropbox_download(Path(file_path))
            else:
                self.root.after(0, lambda q=current_queue_size: self.log(
                    f"Ready queue already has {q} files. No downloads triggered.", "info"))

        # Show results
        def show_results():
            self.log("=" * 60, "success")
            self.log("SCAN COMPLETO", "success")
            self.log("=" * 60, "success")
            self.log(f"Total de arquivos de vídeo: {total_files:,}", "info")
            self.log(f"Tamanho total: {total_size / (1024**4):.2f} TB", "info")
            self.log(f"", "info")
            self.log(f"Já processados: {already_done:,}", "success")
            self.log(f"Já são H.265: {already_h265:,}", "info")
            self.log(f"Muito pequenos: {skipped_small:,}", "info")
            self.log(f"", "info")
            self.log(f"PRECISAM TRANSCODAR: {needs_transcoding:,} ({needs_transcoding_size / (1024**4):.2f} TB)", "warning")

            # Show WHERE the pending files are (top 10 folders)
            if pending_by_folder:
                self.log(f"", "info")
                self.log(f"ONDE ESTÃO OS ARQUIVOS PENDENTES:", "warning")
                top_folders = sorted(pending_by_folder.items(), key=lambda x: -x[1])[:10]
                for folder, count in top_folders:
                    self.log(f"  📁 {folder}: {count} arquivo(s)", "info")
                if len(pending_by_folder) > 10:
                    others = sum(c for f, c in list(pending_by_folder.items())[10:])
                    self.log(f"  ... e mais {len(pending_by_folder) - 10} pastas ({others} arquivos)", "info")

            self.log(f"", "info")
            if h265_logs_found > 0:
                self.log(f"Importados {h265_logs_found} arquivos h265 feitos.txt", "success")

            # Show download trigger info
            triggered = min(len(pending_files_list), 50)
            if triggered > 0:
                self.log(f"Downloads iniciados: {triggered} arquivos (primeiros da fila)", "success")

            self.log("=" * 60, "success")

            # Refresh dashboard to show new data
            self.refresh_dashboard()

        self.root.after(0, show_results)

    def load_stats(self):
        """Load stats from database."""
        try:
            with self.db_lock:
                cursor = self.db_conn.execute(
                    "SELECT COUNT(*), SUM(input_size - output_size) FROM processed WHERE status = 'done'"
                )
                row = cursor.fetchone()
            self.files_processed.set(row[0] or 0)
            saved = (row[1] or 0) / (1024**3)
            self.total_saved_gb.set(round(saved, 2))
        except Exception:
            pass

    def is_processed(self, path: Path) -> bool:
        """Check if file was already processed or should be skipped."""
        # First check local database (thread-safe)
        try:
            with self.db_lock:
                cursor = self.db_conn.execute(
                    "SELECT status FROM processed WHERE input_path = ?", (str(path),)
                )
                row = cursor.fetchone()
        except Exception:
            row = None
        if row is not None:
            status = row[0]
            # Accept any 'done' or 'skipped_*' status (hevc, lowbitrate, exists, etc.)
            if status and (status == 'done' or status.startswith('skipped')):
                return True

        # Check cloud manifest - processed files
        if self.cloud_manifest and self.cloud_manifest.is_processed(str(path)):
            return True

        # Check cloud manifest - skipped files (already H.265, too small, etc.)
        if self.cloud_manifest and self.cloud_manifest.is_skipped(str(path)):
            return True

        # Also check h265 feito.txt and h264 folder (for files processed on other machines)
        return self._is_in_h265_feito_log(path)

    def _is_in_h265_feito_log(self, path: Path) -> bool:
        """
        Check if file appears in h265 feito.txt or has h264 backup.
        This avoids downloading files that were already converted (possibly on another machine).
        """
        try:
            folder = path.parent
            filename = path.name

            # Check if h264 backup exists (means file was already processed)
            h264_folder = folder / 'h264'
            if h264_folder.exists() and (h264_folder / filename).exists():
                return True

            # Check h265 feito.txt
            h265_folder = folder / 'h265'
            log_file = h265_folder / "h265 feito.txt"
            if log_file.exists():
                try:
                    with open(log_file, 'r', encoding='utf-8') as f:
                        content = f.read()
                        if filename in content:
                            return True
                except Exception:
                    pass

            return False
        except Exception:
            return False

    def mark_processed(self, input_path: Path, output_path: str, status: str,
                      input_size: int = 0, output_size: int = 0,
                      duration: float = 0, transcode_time: float = 0):
        """Mark file as processed in local DB and cloud manifest."""
        # Save to local database (thread-safe)
        with self.db_lock:
            self.db_conn.execute("""
                INSERT OR REPLACE INTO processed
                (input_path, output_path, status, input_size, output_size, processed_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (str(input_path), output_path, status, input_size, output_size,
                  datetime.now().isoformat()))
            self.db_conn.commit()
        self.load_stats()

        # Save to cloud manifest (survives SSD wipes)
        if self.cloud_manifest:
            try:
                if status == 'done':
                    self.cloud_manifest.record_success(
                        original_path=str(input_path),
                        output_path=output_path,
                        input_size=input_size,
                        output_size=output_size,
                        encoder=self.encoder.get(),
                        cq_value=self.cq_value.get(),
                        duration=duration,
                        transcode_time=transcode_time,
                    )
                elif status == 'error':
                    self.cloud_manifest.record_failure(str(input_path), "Encoding failed")
                elif status and status.startswith('skipped'):
                    # Save skipped files too (hevc, lowbitrate, exists, etc.)
                    self.cloud_manifest.record_skipped(str(input_path), status, input_size)
            except Exception as e:
                self.log(f"Cloud manifest save error: {e}", "warning")

    def reset_failed(self):
        """Reset failed files so they can be retried."""
        with self.db_lock:
            cursor = self.db_conn.execute("SELECT COUNT(*) FROM processed WHERE status = 'error'")
            count = cursor.fetchone()[0]

        # Also count cloud manifest failures
        cloud_count = 0
        if self.cloud_manifest:
            cloud_count = len(self.cloud_manifest.manifest['failed_files'])

        total = max(count, cloud_count)
        if total == 0:
            messagebox.showinfo("Info", "No failed files to reset.")
            return

        if messagebox.askyesno("Confirm", f"Reset {total} failed files for retry?"):
            with self.db_lock:
                self.db_conn.execute("DELETE FROM processed WHERE status = 'error'")
                self.db_conn.commit()
            # Also reset in cloud manifest
            if self.cloud_manifest:
                self.cloud_manifest.reset_failed()
            self.log(f"Reset {total} failed files for retry", "success")

    def clear_history(self):
        """Clear processing history."""
        if messagebox.askyesno("Confirm", "Clear all processing history?"):
            with self.db_lock:
                self.db_conn.execute("DELETE FROM processed")
                self.db_conn.commit()
            self.load_stats()
            self.log("History cleared", "warning")

    def start_wav_conversion(self):
        """Start standalone WAV→MP3 conversion (when video transcoding is not running)."""
        if self.running:
            self.log("WAV conversion already running in parallel with video transcoding.", "info")
            return

        folder = Path(self.watch_folder.get())
        if not folder.exists():
            messagebox.showerror("Error", "Watch folder not found!")
            return

        self.log("Starting standalone WAV→MP3 conversion...", "info")
        self.wav_running = True
        self.stop_wav_btn.config(state=tk.NORMAL)

        def wav_worker():
            try:
                count = self.process_audio_files(folder)
                self.root.after(0, lambda c=count: self.log(
                    f"WAV conversion finished: {c} files converted", "success"))
            except Exception as e:
                self.root.after(0, lambda err=e: self.log(
                    f"WAV conversion error: {err}", "error"))
            finally:
                self.wav_running = False
                self.root.after(0, lambda: self.stop_wav_btn.config(state=tk.DISABLED))
                self.notify_queue_finished()

        threading.Thread(target=wav_worker, daemon=True).start()

    def stop_wav_conversion(self):
        """Stop standalone WAV→MP3 conversion."""
        self.wav_running = False
        self.stop_wav_btn.config(state=tk.DISABLED)
        self.log("WAV conversion stopped", "warning")

    def scan_audio_files(self):
        """Scan 'Audio Source Files' folders and trigger Dropbox download for WAV files."""
        threading.Thread(target=self._do_scan_audio, daemon=True).start()

    def _do_scan_audio(self):
        """Worker for scanning and triggering WAV downloads."""
        folder = Path(self.watch_folder.get())

        if not folder.exists():
            self.root.after(0, lambda: self.log(f"Folder not found: {folder}", "error"))
            return

        self.root.after(0, lambda: self.log("Scanning Audio Source Files for WAVs..."))

        # Check available disk space - reserve 10GB minimum
        free_gb = self.get_free_disk_space(folder)
        available_for_download = max(0, (free_gb - 10) * 1024**3)  # Convert to bytes, keep 10GB free

        if free_gb < 15:
            self.root.after(0, lambda g=free_gb: self.log(
                f"Low disk space ({g:.1f} GB). Limiting downloads.", "warning"))

        # Find WAV files only in "Audio Source Files" folders
        wav_files = []
        for audio_folder in folder.rglob('Audio Source Files'):
            if audio_folder.is_dir():
                for ext in ['.wav', '.WAV']:
                    for f in audio_folder.glob(f'*{ext}'):
                        if not f.name.startswith('._') and f.parent.name != 'wav':
                            wav_files.append(f)

        self.root.after(0, lambda: self.log(f"Found {len(wav_files)} WAV files"))

        triggered = 0
        triggered_size = 0
        already_local = 0
        cloud_files = 0
        skipped_space = 0

        for wav_path in wav_files:
            try:
                size = wav_path.stat().st_size

                if size < 1000:
                    cloud_files += 1
                    continue

                try:
                    with open(wav_path, 'rb') as f:
                        f.read(1)
                    already_local += 1
                except OSError as e:
                    if e.errno == 22:  # Invalid argument - cloud file
                        cloud_files += 1
                        if not self.offline_mode.get() and triggered_size + size <= available_for_download:
                            self._trigger_dropbox_download(wav_path)
                            triggered += 1
                            triggered_size += size
                        else:
                            skipped_space += 1
                    else:
                        raise

            except PermissionError:
                triggered += 1
            except Exception:
                cloud_files += 1

        triggered_gb = triggered_size / (1024**3)
        msg = f"WAV Scan: {already_local} local, {triggered} downloading ({triggered_gb:.1f}GB)"
        if skipped_space > 0:
            msg += f", {skipped_space} skipped (no space)"
        self.root.after(0, lambda m=msg: self.log(m, "success"))

    def _auto_start_daemon(self):
        """Auto-start processing in daemon mode. Called after UI initialization."""
        if self.auto_start.get() and not self.running:
            self.log("Auto-starting daemon mode...", "success")
            self.toggle_processing()

    def toggle_processing(self):
        """Start processing with instant queue from snapshot or warm start."""
        if not self.running:
            self.running = True
            self.paused = False
            self.start_btn.config(state=tk.DISABLED)
            self.pause_btn.config(state=tk.NORMAL, text="⏸ PAUSE")
            self.stop_btn.config(state=tk.NORMAL)
            self.progress_var.set(0)

            # Start initialization in background thread to not block UI
            threading.Thread(target=self._startup_sequence, daemon=True).start()

    def _startup_sequence(self):
        """Background startup: load queue then start workers."""
        # === PHASE 0: Load queue (instant if snapshot exists) ===
        self.root.after(0, lambda: self.log("Initializing...", "info"))

        # Try to load queue from snapshot (instant restart — no os.walk!)
        snapshot_ok = self.load_queue_snapshot()
        if snapshot_ok:
            queue_size = self.ready_queue.qsize()
            self.root.after(0, lambda q=queue_size: self.log(
                f"Ready queue: {q} files ready to transcode", "success"))

            # Start ALL workers immediately — snapshot loaded, queue is ready
            self._start_all_workers()

            if queue_size > 0:
                self.root.after(0, lambda: self.log("Starting transcoding + WAV conversion!", "success"))
            else:
                self.root.after(0, lambda: self.log("Queue empty - scanning for files...", "info"))
        else:
            # No snapshot — warm start needed.
            # Start workers FIRST, then scan folders in background.
            # Workers will wait on empty queue until scan populates it.
            self._start_all_workers()
            self.root.after(0, lambda: self.log("No snapshot — scanning folders...", "info"))
            self.warm_start_local_queue()
            queue_size = self.ready_queue.qsize()
            if queue_size > 0:
                self.root.after(0, lambda q=queue_size: self.log(
                    f"Queue loaded: {q} files ready", "success"))

    def _start_all_workers(self):
        """Start all background worker threads."""
        # Start the ready queue worker (monitors queue, triggers downloads when needed)
        self.ready_queue_worker_running = True
        self.ready_queue_worker_thread = threading.Thread(target=self.ready_queue_worker, daemon=True)
        self.ready_queue_worker_thread.start()

        # Start the probed queue worker (pre-probes files for instant transcode)
        self.probed_queue_worker_running = True
        self.probed_queue_thread = threading.Thread(target=self.probed_queue_worker, daemon=True)
        self.probed_queue_thread.start()

        # Start the main processing loop (transcodes from probed queue — uses GPU/QSV)
        self.worker_thread = threading.Thread(target=self.process_loop, daemon=True)
        self.worker_thread.start()

        # Start WAV→MP3 processing loop in parallel (uses CPU only, doesn't compete with QSV)
        self.wav_worker_thread = threading.Thread(target=self._wav_processing_loop, daemon=True)
        self.wav_worker_thread.start()

    def toggle_pause(self):
        """Pause or resume encoding."""
        if self.paused:
            # Resume
            self.paused = False
            self.pause_btn.config(text="⏸ PAUSE")
            self.resume_ffmpeg()
            self.log("Resumed encoding", "success")
        else:
            # Pause
            self.paused = True
            self.pause_btn.config(text="▶ RESUME")
            self.pause_ffmpeg()
            self.log("Paused encoding (FFmpeg suspended)", "warning")

    def stop_all(self):
        """Stop all encoding and clear all queues immediately."""
        self.running = False
        self.paused = False
        self.ready_queue_worker_running = False  # Stop the ready queue worker
        self.probed_queue_worker_running = False  # Stop the probed queue worker

        # Clear the ready queue
        while not self.ready_queue.empty():
            try:
                self.ready_queue.get_nowait()
            except Exception:
                break

        # Clear the probed queue
        while not self.probed_queue.empty():
            try:
                self.probed_queue.get_nowait()
            except Exception:
                break

        # Clear the active queue (v5.3: full queue clear on STOP)
        with self.active_queue_lock:
            self.active_queue.clear()
        self._queue_items_set.clear()
        self._in_ready_queue.clear()

        # Clear pending downloads
        with self.pending_downloads_lock:
            self.pending_downloads.clear()

        # Delete queue snapshot so it doesn't reload on next start
        try:
            if self.QUEUE_SNAPSHOT_FILE.exists():
                self.QUEUE_SNAPSHOT_FILE.unlink()
        except Exception:
            pass

        # Kill FFmpeg process if running
        if self.current_process:
            try:
                self.current_process.terminate()
                self.log("FFmpeg process terminated", "warning")
            except Exception:
                pass

        self.start_btn.config(state=tk.NORMAL)
        self.pause_btn.config(state=tk.DISABLED, text="⏸ PAUSE")
        self.stop_btn.config(state=tk.DISABLED)
        self.progress_var.set(0)
        self.current_file_label.config(text="Idle")
        self.progress_label.config(text="")
        self.log("Stopped all encoding and cleared queue", "warning")

    def pause_ffmpeg(self):
        """Suspend the FFmpeg process (Windows)."""
        if self.current_process and self.current_process.poll() is None:
            try:
                import ctypes
                kernel32 = ctypes.windll.kernel32
                handle = kernel32.OpenProcess(0x1F0FFF, False, self.current_process.pid)
                kernel32.DebugActiveProcess(self.current_process.pid)
                self.log(f"FFmpeg process {self.current_process.pid} suspended", "info")
            except Exception as e:
                # Fallback: just set paused flag, loop will wait
                self.log(f"Soft pause (process continues until next file): {e}", "warning")

    def resume_ffmpeg(self):
        """Resume the FFmpeg process (Windows)."""
        if self.current_process and self.current_process.poll() is None:
            try:
                import ctypes
                kernel32 = ctypes.windll.kernel32
                kernel32.DebugActiveProcessStop(self.current_process.pid)
                self.log(f"FFmpeg process {self.current_process.pid} resumed", "info")
            except Exception as e:
                self.log(f"Resume note: {e}", "info")

    def scan_and_trigger_download(self):
        """Scan folder and trigger Dropbox to download files (without encoding)."""
        threading.Thread(target=self._do_scan_trigger, daemon=True).start()

    def _do_scan_trigger(self):
        """Worker for scan and trigger download."""
        # Skip entirely in offline mode
        if self.offline_mode.get():
            self.root.after(0, lambda: self.log("Offline mode: download trigger skipped", "info"))
            return

        folder = Path(self.watch_folder.get())

        if not folder.exists():
            self.root.after(0, lambda: self.log(f"Folder not found: {folder}", "error"))
            return

        self.root.after(0, lambda: self.log(f"Scanning {folder} to trigger downloads..."))

        # Smart download: check how many files we need in ready queue (target: 50 minimum)
        current_queue_size = self.ready_queue.qsize()
        downloads_needed = max(0, 50 - current_queue_size)

        if downloads_needed == 0:
            self.root.after(0, lambda q=current_queue_size: self.log(
                f"Ready queue already has {q} files. No downloads needed.", "info"))
            return

        self.root.after(0, lambda n=downloads_needed, q=current_queue_size: self.log(
            f"Ready queue has {q} files. Will download up to {n} more.", "info"))

        # Check available disk space - reserve 10GB minimum
        free_gb = self.get_free_disk_space(folder)
        available_for_download = max(0, (free_gb - 10) * 1024**3)  # Convert to bytes, keep 10GB free

        if free_gb < 15:
            self.root.after(0, lambda g=free_gb: self.log(
                f"Low disk space ({g:.1f} GB). Limiting downloads.", "warning"))

        # Find video files (only .mp4, skip ._ metadata files from macOS/ATEM and DJI drone files)
        video_files = []
        for ext in ['.mp4', '.MP4']:
            for f in folder.rglob(f'*{ext}'):
                # Skip h265/h264 folders, macOS/ATEM metadata files, and DJI drone files
                if ('h265' not in str(f).lower() and 'h264' not in str(f).lower()
                    and not f.name.startswith('._') and not f.name.upper().startswith('DJI_')):
                    video_files.append(f)

        self.root.after(0, lambda: self.log(f"Found {len(video_files)} video files"))

        triggered = 0
        triggered_size = 0  # Track size of files we've triggered for download
        already_local = 0
        cloud_files = 0
        skipped_space = 0

        for video_path in video_files:
            try:
                # Check file size first
                size = video_path.stat().st_size

                # Use safe cloud check (doesn't trigger download)
                if self._is_cloud_only_file(video_path):
                    cloud_files += 1
                    # Check if we have space AND haven't reached download limit
                    if triggered < downloads_needed and triggered_size + size <= available_for_download:
                        self._trigger_dropbox_download(video_path)
                        triggered += 1
                        triggered_size += size
                    elif triggered >= downloads_needed:
                        pass  # Already have enough downloads queued
                    else:
                        skipped_space += 1
                else:
                    already_local += 1

            except PermissionError:
                # File is being synced by Dropbox
                cloud_files += 1
            except Exception:
                # Silent - don't spam log with errors for each file
                cloud_files += 1

        # Report results
        triggered_gb = triggered_size / (1024**3)
        msg = f"Scan: {already_local} local, {triggered} downloading ({triggered_gb:.1f}GB)"
        if skipped_space > 0:
            msg += f", {skipped_space} skipped (no space)"
        self.root.after(0, lambda m=msg: self.log(m, "success"))

    def _check_single_file_for_queue(self, video_path: Path, min_size_gb: float) -> dict:
        """
        Check a single file for ready queue eligibility. Returns dict with result.
        Designed to run in parallel threads.
        """
        result = {"status": None, "path": video_path, "size": 0, "needs_download": False}
        path_str = str(video_path)

        try:
            # Skip if already processed
            if self.is_processed(video_path):
                result["status"] = "already_processed"
                return result

            # Check file size
            try:
                size = video_path.stat().st_size
                result["size"] = size
            except Exception:
                result["status"] = "stat_error"
                return result

            # Skip too small (but not cloud placeholders which are also small)
            # First check if it's a cloud file using safe method (no download trigger)
            if self._is_cloud_only_file(video_path):
                result["status"] = "cloud"
                result["needs_download"] = True
                return result

            # Now we know it's local - check size
            if size / (1024**3) < min_size_gb:
                result["status"] = "too_small"
                return result

            # Verify file is actually readable (should be since it passed cloud check)
            try:
                with open(video_path, 'rb') as f:
                    f.read(1024)  # Try to read
            except OSError as e:
                if e.errno == 22:  # Cloud file (shouldn't happen but just in case)
                    result["status"] = "cloud"
                    result["needs_download"] = True
                    return result
                result["status"] = "read_error"
                return result

            # Check if output already exists (fast check before probe)
            output_folder = video_path.parent / 'h265'
            output_path = output_folder / video_path.name
            if output_path.exists():
                result["status"] = "output_exists"
                result["output_size"] = output_path.stat().st_size
                return result

            # FULL PRE-CHECK: Probe video to check codec and bitrate
            probe_data = self.probe_video(video_path)
            if not probe_data:
                result["status"] = "probe_failed"
                return result

            # Skip if already HEVC
            if self.is_hevc(probe_data):
                result["status"] = "already_hevc"
                return result

            # Skip if low bitrate (< 8 Mbps)
            bitrate = self.get_bitrate(probe_data, size)
            if bitrate > 0 and bitrate < 8:
                result["status"] = "low_bitrate"
                return result

            # ALL CHECKS PASSED
            result["status"] = "ready"
            return result

        except Exception as e:
            result["status"] = f"error:{e}"
            return result

    # ==================== NEW ARCHITECTURE v2.0: QUEUE-FIRST ====================

    def save_queue_snapshot(self):
        """
        v2.0: Persiste active_queue, folder_tracker e pending_folders_index.
        Permite restart instantâneo sem varredura.
        """
        try:
            # Serializar active_queue
            with self.active_queue_lock:
                items = []
                for item in self.active_queue:
                    items.append({
                        "path": str(item['path']),
                        "size": item['size'],
                        "folder": item['folder'],
                        "status": item['status'],
                        "retry_at": item.get('retry_at', 0)
                    })

            # Serializar folder_tracker
            with self.folder_tracker_lock:
                folders = {}
                for folder_path, info in self.folder_tracker.items():
                    folders[folder_path] = {
                        'status': info['status'],
                        'total_known': info['total_known'],
                        'selected': info['selected'],
                        'done': info['done'],
                        'priority': info.get('priority', 0)
                    }

            # Serializar pending_folders para evitar os.walk() no restart
            pending = []
            for folder_path, priority in self.pending_folders:
                pending.append({"path": folder_path, "priority": priority})

            snapshot = {
                "version": 4,  # v4 = pending_folders cached, skip os.walk
                "timestamp": time.time(),
                "pending_folders_index": self.pending_folders_index,
                "active_queue": items,
                "folder_tracker": folders,
                "pending_folders": pending
            }

            self.QUEUE_SNAPSHOT_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(self.QUEUE_SNAPSHOT_FILE, 'w', encoding='utf-8') as f:
                json.dump(snapshot, f, indent=2)

        except Exception as e:
            pass  # Silent fail - persistence is nice-to-have

    def load_queue_snapshot(self) -> bool:
        """
        v2.0: Carrega snapshot e restaura estado completo.
        Retorna True se carregou com sucesso.
        """
        try:
            if not self.QUEUE_SNAPSHOT_FILE.exists():
                return False

            with open(self.QUEUE_SNAPSHOT_FILE, 'r', encoding='utf-8') as f:
                snapshot = json.load(f)

            # Verificar versão
            version = snapshot.get("version", 1)
            if version < 4:
                self.root.after(0, lambda: self.log("Old snapshot format, rebuilding...", "info"))
                return False

            # Verificar idade (> 24 horas = reconstruir)
            age_hours = (time.time() - snapshot.get("timestamp", 0)) / 3600
            if age_hours > 24:
                self.root.after(0, lambda: self.log("Snapshot too old, rebuilding...", "info"))
                return False

            # Restaurar pending_folders do snapshot (evita os.walk!)
            cached_folders = snapshot.get("pending_folders", [])
            if cached_folders:
                self.pending_folders = [(f["path"], f["priority"]) for f in cached_folders]
                self.pending_folders_loaded = True
                self.pending_folders_index = min(
                    snapshot.get("pending_folders_index", 0),
                    len(self.pending_folders)
                )
                self.root.after(0, lambda n=len(self.pending_folders): self.log(
                    f"Restored {n} folders from snapshot (no scan needed)", "success"))

                # Atualizar pending_folders em background para detectar novas pastas
                threading.Thread(target=self._background_refresh_folders, daemon=True).start()
            else:
                # Fallback: snapshot sem pending_folders (não deveria acontecer)
                self._load_pending_folders()
                self.pending_folders_index = min(
                    snapshot.get("pending_folders_index", 0),
                    len(self.pending_folders)
                )

            # Carregar folder_tracker
            folders = snapshot.get("folder_tracker", {})
            with self.folder_tracker_lock:
                for folder_path, info in folders.items():
                    self.folder_tracker[folder_path] = info

            # Carregar active_queue (validando arquivos)
            items = snapshot.get("active_queue", [])
            loaded = 0

            for item_data in items:
                path = Path(item_data["path"])
                path_str = str(path)

                # Validar arquivo
                if not path.exists():
                    continue
                if path_str in self._queue_items_set:
                    continue
                if self.is_processed(path):
                    continue

                # Verificar se ainda é local
                is_local = self._is_likely_local_file_fast(path)
                status = 'READY_LOCAL' if is_local else item_data.get('status', 'QUEUED_REMOTE')

                item = {
                    'path': path,
                    'size': item_data.get('size', 0),
                    'folder': item_data.get('folder', str(path.parent)),
                    'status': status,
                    'retry_at': item_data.get('retry_at', 0)
                }

                with self.active_queue_lock:
                    self.active_queue.append(item)
                    self._queue_items_set.add(path_str)
                    loaded += 1

            if loaded > 0:
                self.root.after(0, lambda n=loaded: self.log(
                    f"Loaded {n} files from snapshot - instant start!", "success"))

                # Sincronizar para ready_queue
                self._sync_to_ready_queue()
                return True

            return False

        except Exception as e:
            return False

    def _is_likely_local_file_fast(self, file_path: Path) -> bool:
        """
        FAST check if file is likely local (not cloud-only).
        Uses only file size check - no subprocess calls.
        Returns True if file is likely local and ready.
        """
        try:
            size = file_path.stat().st_size
            # Cloud placeholders are tiny (< 1KB typically)
            # Real video files are much larger
            return size > 100000  # > 100KB = likely local
        except Exception:
            return False

    # =========================================================================
    # NEW ARCHITECTURE v2.0: Queue-first, folder-complete, zero mass-probe
    # =========================================================================

    def _load_pending_folders(self):
        """
        Carrega lista de pastas que contêm arquivos MP4.
        PROGRESSIVO: emite primeiros resultados rápido para permitir
        início imediato do encoding enquanto scan continua.
        NÃO lista os arquivos ainda - só as pastas. Lazy loading.
        """
        if self.pending_folders_loaded:
            return

        self.root.after(0, lambda: self.log("Discovering folders (not files)...", "info"))
        start_time = time.time()

        folders_with_mp4 = set()
        first_batch_emitted = False
        FIRST_BATCH_SIZE = 20  # Emit first 20 folders ASAP for instant queue fill

        for watch_folder in self.get_watch_folders():
            import os
            for dirpath, dirnames, filenames in os.walk(str(watch_folder)):
                # Prune h264/h265 dirs from traversal to speed up os.walk
                dirnames[:] = [d for d in dirnames
                               if d.lower() not in ('h265', 'h264')]

                dirpath_lower = dirpath.lower()
                if 'h265' in dirpath_lower or 'h264' in dirpath_lower:
                    continue

                # Check if folder has MP4 files
                has_mp4 = any(f.lower().endswith('.mp4') for f in filenames
                             if not f.startswith('._') and not f.upper().startswith('DJI_'))
                if has_mp4:
                    folders_with_mp4.add(dirpath)

                # Emit first batch early so queue can start filling
                if not first_batch_emitted and len(folders_with_mp4) >= FIRST_BATCH_SIZE:
                    first_batch_emitted = True
                    batch = sorted(folders_with_mp4, key=lambda x: x.lower())
                    priority = []
                    normal = []
                    for fp in batch:
                        h264_path = Path(fp) / 'h264'
                        if h264_path.exists():
                            try:
                                if any(h264_path.iterdir()):
                                    priority.append((fp, 1))
                                    continue
                            except Exception:
                                pass
                        normal.append((fp, 0))
                    self.pending_folders = priority + normal
                    self.pending_folders_index = 0
                    self.pending_folders_loaded = True  # Allow workers to start
                    elapsed_batch = time.time() - start_time
                    self.root.after(0, lambda n=len(self.pending_folders), t=elapsed_batch: self.log(
                        f"First {n} folders ready in {t:.1f}s — filling queue...", "info"))
                    # Fill initial queue immediately
                    self._refill_queue_incremental()
                    self._sync_to_ready_queue()

        # Final sort and priority classification of ALL folders
        sorted_folders = sorted(folders_with_mp4, key=lambda x: x.lower())

        priority_folders = []
        normal_folders = []

        for folder_path in sorted_folders:
            h264_path = Path(folder_path) / 'h264'
            if h264_path.exists():
                try:
                    if any(h264_path.iterdir()):
                        priority_folders.append((folder_path, 1))
                        continue
                except Exception:
                    pass
            normal_folders.append((folder_path, 0))

        self.pending_folders = priority_folders + normal_folders
        if not first_batch_emitted:
            self.pending_folders_index = 0
        self.pending_folders_loaded = True

        elapsed = time.time() - start_time
        self.root.after(0, lambda n=len(self.pending_folders), t=elapsed: self.log(
            f"Found {n} folders with MP4 files in {t:.1f}s", "success"))

    def _background_refresh_folders(self):
        """
        Atualiza pending_folders em background para detectar novas pastas
        que foram adicionadas desde o último snapshot.
        Não bloqueia nenhum worker — apenas adiciona novas pastas ao final.
        """
        try:
            existing_paths = {fp for fp, _ in self.pending_folders}

            new_folders = []
            for watch_folder in self.get_watch_folders():
                import os
                for dirpath, dirnames, filenames in os.walk(str(watch_folder)):
                    dirpath_lower = dirpath.lower()
                    if 'h265' in dirpath_lower or 'h264' in dirpath_lower:
                        continue
                    has_mp4 = any(f.lower().endswith('.mp4') for f in filenames
                                 if not f.startswith('._') and not f.upper().startswith('DJI_'))
                    if has_mp4 and dirpath not in existing_paths:
                        new_folders.append(dirpath)

            if new_folders:
                sorted_new = sorted(new_folders, key=lambda x: x.lower())
                for folder_path in sorted_new:
                    self.pending_folders.append((folder_path, 0))
                self.root.after(0, lambda n=len(new_folders): self.log(
                    f"Background scan found {n} new folders", "info"))
        except Exception:
            pass  # Non-critical — snapshot folders are still valid

    def _get_folder_files(self, folder_path: str) -> list:
        """
        Lista arquivos MP4 de uma pasta específica, em ordem alfabética.
        Retorna lista de (path, size) para arquivos elegíveis.
        NÃO faz probe - apenas lista.
        """
        folder = Path(folder_path)
        min_size_bytes = int(self.min_size_gb.get() * 1024**3)
        files = []

        try:
            for f in sorted(folder.iterdir(), key=lambda x: str(x).lower()):
                if not f.is_file():
                    continue
                if not f.suffix.lower() == '.mp4':
                    continue
                if f.name.startswith('._') or f.name.upper().startswith('DJI_'):
                    continue

                path_str = str(f)

                # Skip if already in queue
                if path_str in self._queue_items_set:
                    continue

                # Skip if already processed
                if self.is_processed(f):
                    continue

                # Skip if output already exists (h265/filename.mp4)
                # This catches files converted but not in DB, avoiding 20-70s probe waste
                h265_output = folder / 'h265' / f.name
                if h265_output.exists():
                    try:
                        out_size = h265_output.stat().st_size
                        if out_size > 100000:  # Real file, not placeholder
                            continue
                    except Exception:
                        pass

                # Check size
                try:
                    size = f.stat().st_size
                    if size < min_size_bytes:
                        continue
                    # Cloud placeholder check (< 100KB = likely cloud)
                    is_local = size > 100000
                except Exception:
                    continue

                files.append({
                    'path': f,
                    'size': size,
                    'folder': folder_path,
                    'is_local': is_local,
                    'status': 'READY_LOCAL' if is_local else 'QUEUED_REMOTE',
                    'retry_at': 0
                })
        except Exception as e:
            pass

        return files

    def _refill_queue_incremental(self) -> int:
        """
        Adiciona arquivos à fila até atingir QUEUE_TARGET_SIZE.
        Estratégia incremental:
        1. Primeiro, completa pastas já ativas (folder_tracker)
        2. Depois, abre novas pastas da lista (pending_folders)
        Retorna número de arquivos adicionados.
        """
        with self.active_queue_lock:
            current_size = len(self.active_queue)
            if current_size >= self.QUEUE_TARGET_SIZE:
                return 0

            needed = self.QUEUE_TARGET_SIZE - current_size

        added = 0

        # === FASE 1: Completar pastas já ativas ===
        with self.folder_tracker_lock:
            active_folders = [(path, info) for path, info in self.folder_tracker.items()
                             if info['status'] in ('ACTIVE', 'COMPLETING')]
            # Sort by priority (completing first) then by how close to completion
            active_folders.sort(key=lambda x: (
                0 if x[1]['status'] == 'COMPLETING' else 1,
                x[1]['total_known'] - x[1]['done']  # Fewer remaining = higher priority
            ))

        for folder_path, info in active_folders:
            if added >= needed:
                break

            # Get more files from this folder
            files = self._get_folder_files(folder_path)
            for file_info in files:
                if added >= needed:
                    break

                path_str = str(file_info['path'])
                if path_str in self._queue_items_set:
                    continue

                with self.active_queue_lock:
                    self.active_queue.append(file_info)
                    self._queue_items_set.add(path_str)

                with self.folder_tracker_lock:
                    if folder_path in self.folder_tracker:
                        self.folder_tracker[folder_path]['selected'] += 1

                added += 1

        # === FASE 2: Abrir novas pastas se ainda precisar ===
        while added < needed and self.pending_folders_index < len(self.pending_folders):
            folder_path, priority = self.pending_folders[self.pending_folders_index]

            # Skip if already tracked
            with self.folder_tracker_lock:
                if folder_path in self.folder_tracker:
                    self.pending_folders_index += 1
                    continue

            # Get files from this new folder
            files = self._get_folder_files(folder_path)

            if not files:
                # Pasta vazia ou todos processados
                self.pending_folders_index += 1
                continue

            # Initialize folder tracker
            with self.folder_tracker_lock:
                self.folder_tracker[folder_path] = {
                    'status': 'ACTIVE',
                    'total_known': len(files),
                    'selected': 0,
                    'done': 0,
                    'priority': priority
                }

            # Add files to queue
            for file_info in files:
                if added >= needed:
                    break

                path_str = str(file_info['path'])
                if path_str in self._queue_items_set:
                    continue

                with self.active_queue_lock:
                    self.active_queue.append(file_info)
                    self._queue_items_set.add(path_str)

                with self.folder_tracker_lock:
                    self.folder_tracker[folder_path]['selected'] += 1

                added += 1

            self.pending_folders_index += 1

        return added

    def _mark_item_done(self, file_path: Path):
        """Marca item como concluído, remove da active_queue e atualiza folder tracker."""
        folder_path = str(file_path.parent)
        path_str = str(file_path)

        # Remove from active_queue immediately to free slot for _refill
        with self.active_queue_lock:
            self.active_queue = [item for item in self.active_queue
                                 if str(item['path']) != path_str]
            self._queue_items_set.discard(path_str)

        with self.folder_tracker_lock:
            if folder_path in self.folder_tracker:
                self.folder_tracker[folder_path]['done'] += 1
                info = self.folder_tracker[folder_path]

                # Verificar se pasta está completa
                if info['done'] >= info['total_known']:
                    self.folder_tracker[folder_path]['status'] = 'DONE'
                    self.root.after(0, lambda p=folder_path: self.log(
                        f"📁 FOLDER COMPLETE: {Path(p).name}", "success"))
                elif info['done'] >= info['total_known'] * 0.8:
                    # 80% completa - marcar como COMPLETING
                    self.folder_tracker[folder_path]['status'] = 'COMPLETING'

    def _trigger_downloads_incremental(self, max_downloads: int) -> int:
        """
        Dispara downloads para itens QUEUED_REMOTE na fila.
        NÃO faz varredura global - só olha a fila ativa.
        """
        if self.offline_mode.get():
            return 0

        triggered = 0

        with self.active_queue_lock:
            for item in self.active_queue:
                if triggered >= max_downloads:
                    break

                if item['status'] == 'QUEUED_REMOTE':
                    # Verificar se pode iniciar download
                    file_path = item['path']
                    if self._can_trigger_download(item['size']):
                        self._add_to_pending_downloads(str(file_path), item['size'])
                        self._trigger_dropbox_download(file_path)
                        item['status'] = 'DOWNLOADING'
                        triggered += 1

        return triggered

    def _check_download_completion(self):
        """
        Verifica itens em DOWNLOADING e atualiza para READY_LOCAL se prontos.
        """
        with self.active_queue_lock:
            for item in self.active_queue:
                if item['status'] == 'DOWNLOADING':
                    file_path = item['path']
                    if self._is_likely_local_file_fast(file_path):
                        item['status'] = 'READY_LOCAL'
                        self._remove_from_pending_downloads(str(file_path))
                elif item['status'] == 'FAILED_RETRY':
                    # Verificar se pode tentar novamente
                    if time.time() >= item.get('retry_at', 0):
                        item['status'] = 'QUEUED_REMOTE'

    def _get_queue_stats(self) -> dict:
        """Retorna estatísticas da fila."""
        with self.active_queue_lock:
            stats = {
                'total': len(self.active_queue),
                'ready_local': sum(1 for i in self.active_queue if i['status'] == 'READY_LOCAL'),
                'downloading': sum(1 for i in self.active_queue if i['status'] == 'DOWNLOADING'),
                'queued_remote': sum(1 for i in self.active_queue if i['status'] == 'QUEUED_REMOTE'),
                'failed_retry': sum(1 for i in self.active_queue if i['status'] == 'FAILED_RETRY'),
            }
        with self.folder_tracker_lock:
            stats['active_folders'] = sum(1 for f in self.folder_tracker.values()
                                          if f['status'] in ('ACTIVE', 'COMPLETING'))
            stats['completing_folders'] = sum(1 for f in self.folder_tracker.values()
                                             if f['status'] == 'COMPLETING')
            stats['done_folders'] = sum(1 for f in self.folder_tracker.values()
                                        if f['status'] == 'DONE')
        return stats

    def warm_start_local_queue(self):
        """
        v2.1: Startup rápido sem mass-probe.
        1. LIMPA ready_queue antiga
        2. Carrega lista de pastas (não arquivos)
        3. Preenche fila com primeiros 100 arquivos
        """
        self.root.after(0, lambda: self.log("=" * 60, "info"))
        self.root.after(0, lambda: self.log("QUEUE-FIRST STARTUP v2.1", "info"))
        self.root.after(0, lambda: self.log("Target queue: 100 files | Zero mass-probe", "info"))

        # v2.1: LIMPAR ready_queue antiga para garantir fila pequena
        cleared = 0
        while not self.ready_queue.empty():
            try:
                self.ready_queue.get_nowait()
                cleared += 1
            except Exception:
                break
        if cleared > 0:
            self.root.after(0, lambda n=cleared: self.log(f"Cleared {n} items from old queue", "info"))

        # Limpar também o set de tracking
        self._queue_items_set.clear()
        self._in_ready_queue.clear()

        start_time = time.time()

        # Carregar lista de pastas (rápido - não lista arquivos)
        self._load_pending_folders()

        # Preencher fila até o target
        added = self._refill_queue_incremental()

        elapsed = time.time() - start_time

        stats = self._get_queue_stats()
        self.root.after(0, lambda s=stats, t=elapsed: self.log(
            f"Queue ready: {s['ready_local']} local, {s['queued_remote']} cloud | {t:.1f}s", "success"))

        # Transferir para ready_queue (compatibilidade com sistema existente)
        self._sync_to_ready_queue()

        self.root.after(0, lambda: self.log("=" * 60, "info"))

    def _sync_to_ready_queue(self):
        """
        v2.2: Sincroniza active_queue com ready_queue para compatibilidade.
        LIMITA a ready_queue ao QUEUE_TARGET_SIZE.
        Só adiciona itens READY_LOCAL que não estão já na fila.
        Uses _in_ready_queue set to track membership without draining the queue
        (draining causes a race condition where scan_and_process sees qsize=0).
        Also prunes already-processed items from active_queue so _refill can add new files.
        """
        current_ready_size = self.ready_queue.qsize()

        if current_ready_size >= self.QUEUE_TARGET_SIZE:
            return

        added = 0
        max_to_add = self.QUEUE_TARGET_SIZE - current_ready_size
        processed_paths = set()

        with self.active_queue_lock:
            for item in self.active_queue:
                if added >= max_to_add:
                    break

                if item['status'] == 'READY_LOCAL':
                    path = item['path']
                    path_str = str(path)

                    # Skip if already in ready_queue
                    if path_str in self._in_ready_queue:
                        continue

                    # Skip and mark for removal if already processed
                    if self.is_processed(path):
                        processed_paths.add(path_str)
                        continue

                    # Skip if output already exists on disk
                    h265_out = Path(item['folder']) / 'h265' / path.name
                    try:
                        if h265_out.exists() and h265_out.stat().st_size > 100000:
                            processed_paths.add(path_str)
                            continue
                    except Exception:
                        pass

                    folder = item['folder']
                    h264_folder = Path(folder) / 'h264'
                    has_h264 = h264_folder.exists()
                    priority = 1 if has_h264 else 0
                    self.ready_queue.put((path, item['size'], priority))
                    self._in_ready_queue.add(path_str)
                    added += 1

            # Prune processed items from active_queue to free slots for _refill
            if processed_paths:
                self.active_queue = [item for item in self.active_queue
                                     if str(item['path']) not in processed_paths]
                self._queue_items_set -= processed_paths

    def ready_queue_worker(self):
        """
        v2.0: Queue-first, folder-complete, zero mass-probe

        Mantém a fila com ~100 arquivos sem varredura global.
        Só adiciona mais quando cai para 99 ou menos.
        Prioriza completar pastas antes de começar outras.
        """
        last_log_time = 0
        last_snapshot_time = time.time()
        SNAPSHOT_INTERVAL = 60  # Save every 60 seconds

        while self.ready_queue_worker_running and self.running:
            try:
                now = time.time()

                # === Verificar downloads em progresso ===
                self._check_download_completion()

                # Sincronizar novos READY_LOCAL para ready_queue
                self._sync_to_ready_queue()

                # === Refill: só quando fila cai abaixo do threshold ===
                current_ready = self.ready_queue.qsize()

                if current_ready < self.QUEUE_REFILL_THRESHOLD:
                    # Primeiro, tentar refill incremental (pastas já conhecidas + novas)
                    added = self._refill_queue_incremental()

                    if added > 0:
                        self._sync_to_ready_queue()
                        self.root.after(0, lambda n=added: self.log(
                            f"Refill: +{n} files added to queue", "info"))

                    # Se ainda não tem arquivos locais suficientes, disparar downloads
                    stats = self._get_queue_stats()
                    if stats['ready_local'] < 10 and stats['queued_remote'] > 0:
                        max_downloads = min(10, self.max_pending_downloads)
                        triggered = self._trigger_downloads_incremental(max_downloads)
                        if triggered > 0:
                            self.root.after(0, lambda n=triggered: self.log(
                                f"Triggered {n} cloud downloads", "info"))

                # === Log status periodicamente ===
                if now - last_log_time >= 30:
                    stats = self._get_queue_stats()
                    self.root.after(0, lambda s=stats: self.log(
                        f"Queue: {s['ready_local']} local | {s['downloading']} downloading | "
                        f"{s['queued_remote']} remote | Folders: {s['active_folders']} active, "
                        f"{s['completing_folders']} completing", "info"))
                    last_log_time = now

                # === Salvar snapshot periodicamente ===
                if now - last_snapshot_time >= SNAPSHOT_INTERVAL:
                    self.save_queue_snapshot()
                    last_snapshot_time = now

                # Esperar antes do próximo ciclo
                current_ready = self.ready_queue.qsize()
                wait_time = 2 if current_ready < 20 else 5
                for _ in range(wait_time * 2):
                    if not self.running or not self.ready_queue_worker_running:
                        break
                    time.sleep(0.5)

            except Exception as e:
                self.root.after(0, lambda err=e: self.log(f"Queue worker error: {err}", "warning"))
                time.sleep(5)

    def probed_queue_worker(self):
        """
        Background worker that pre-probes files from ready_queue.
        Keeps PROBED_BUFFER_SIZE files ready for instant transcode.
        This eliminates the 30+ second gap between transcodes.
        """
        while self.probed_queue_worker_running and self.running:
            try:
                # Check if probed_queue needs more files
                probed_size = self.probed_queue.qsize()

                if probed_size < self.PROBED_BUFFER_SIZE and not self.ready_queue.empty():
                    # Get next file from ready_queue
                    try:
                        item = self.ready_queue.get_nowait()
                        video_path = item[0]
                        file_size = item[1]
                        self._in_ready_queue.discard(str(video_path))
                    except Exception:
                        time.sleep(0.5)
                        continue

                    # Skip if file no longer exists or already processed
                    if not video_path.exists() or self.is_processed(video_path):
                        continue

                    # Check if output already exists BEFORE expensive wait/probe
                    output_check = video_path.parent / 'h265' / video_path.name
                    if output_check.exists():
                        try:
                            if output_check.stat().st_size > 100000:
                                self.mark_processed(video_path, str(output_check), "skipped_exists",
                                                  file_size, output_check.stat().st_size)
                                continue
                        except Exception:
                            pass

                    # Wait for file to be ready (downloaded)
                    if not self.wait_for_file_ready(video_path, estimated_size=file_size):
                        continue

                    # PRE-PROBE: This is the expensive part we're doing ahead of time
                    probe_data = self.probe_video(video_path)
                    if not probe_data:
                        continue

                    # Check if already HEVC
                    if self.is_hevc(probe_data):
                        self.mark_processed(video_path, "", "skipped_hevc", file_size, 0)
                        continue

                    # Check bitrate
                    bitrate = self.get_bitrate(probe_data, file_size)
                    if bitrate > 0 and bitrate < 8:
                        self.mark_processed(video_path, "", "skipped_lowbitrate", file_size, 0)
                        continue

                    # Check if output exists
                    output_path = video_path.parent / 'h265' / video_path.name
                    if output_path.exists():
                        self.mark_processed(video_path, str(output_path), "skipped_exists",
                                          file_size, output_path.stat().st_size)
                        continue

                    # All checks passed - add to probed queue with probe data
                    self.probed_queue.put((video_path, file_size, probe_data))

                else:
                    # Probed queue is full or ready_queue is empty, wait
                    time.sleep(0.5)

            except Exception as e:
                time.sleep(1)

    def process_loop(self):
        """Main processing loop with auto-recovery for daemon mode."""
        consecutive_errors = 0
        max_consecutive_errors = 10
        first_iteration = True

        while self.running:
            try:
                # Wait while paused
                while self.paused and self.running:
                    time.sleep(0.5)

                if not self.running:
                    break

                # On first iteration, wait briefly for queue to populate
                # (probed_queue_worker needs a moment to probe first file)
                if first_iteration:
                    first_iteration = False
                    # Wait up to 10s for probed queue to have at least 1 file
                    for _ in range(20):
                        if self.probed_queue.qsize() > 0 or not self.ready_queue.empty():
                            break
                        time.sleep(0.5)

                self.scan_and_process()
                consecutive_errors = 0  # Reset on success

                # Adaptive wait: short if queue has files, longer if idle
                queue_has_work = not self.ready_queue.empty() or self.probed_queue.qsize() > 0
                wait_secs = 3 if queue_has_work else 15
                for _ in range(wait_secs):
                    if not self.running:
                        break
                    while self.paused and self.running:
                        time.sleep(0.5)
                    time.sleep(1)

            except Exception as e:
                consecutive_errors += 1
                self.root.after(0, lambda err=e, n=consecutive_errors: self.log(
                    f"Process loop error ({n}/{max_consecutive_errors}): {err}", "error"))

                if consecutive_errors >= max_consecutive_errors:
                    self.root.after(0, lambda: self.log(
                        "Too many consecutive errors. Pausing for 5 minutes...", "error"))
                    time.sleep(300)  # 5 minute cooldown
                    consecutive_errors = 0
                else:
                    time.sleep(10)  # Brief pause before retry

        # Reset UI when stopped
        self.root.after(0, lambda: self.current_file_label.config(text="Idle"))
        self.root.after(0, lambda: self.progress_var.set(0))
        self.root.after(0, lambda: self.start_btn.config(state=tk.NORMAL))
        self.root.after(0, lambda: self.pause_btn.config(state=tk.DISABLED, text="⏸ PAUSE"))
        self.root.after(0, lambda: self.stop_btn.config(state=tk.DISABLED))

    def _wav_processing_loop(self):
        """Parallel WAV→MP3 loop. Runs alongside video transcoding using CPU only."""
        while self.running:
            try:
                # Wait while paused
                while self.paused and self.running:
                    time.sleep(0.5)

                if not self.running:
                    break

                folders = self.get_watch_folders()
                if folders:
                    processed = self.process_audio_files(folders[0])
                    if processed > 0:
                        self.root.after(0, lambda n=processed: self.log(
                            f"WAV batch done: {n} files converted", "success"))

                # Wait 60 seconds between WAV scans (lighter duty than video)
                for _ in range(60):
                    if not self.running:
                        break
                    time.sleep(1)

            except Exception as e:
                self.root.after(0, lambda err=e: self.log(
                    f"WAV loop error: {err}", "error"))
                time.sleep(30)

    def scan_and_process(self):
        """Process files from the ready queue. No scanning - ready_queue_worker handles that."""
        folders = self.get_watch_folders()
        if not folders:
            self.root.after(0, lambda: self.log("No valid watch folders found", "error"))
            return

        # Check disk space before starting (use first folder)
        free_gb = self.get_free_disk_space(folders[0])
        if free_gb < 5:  # Less than 5GB free
            self.root.after(0, lambda g=free_gb: self.log(
                f"Low disk space ({g:.1f} GB free). Waiting...", "warning"))
            return

        # Clean up stale pending downloads
        self._cleanup_pending_downloads()

        # Track files processed in this scan
        files_processed_this_scan = 0
        queue_size = self.ready_queue.qsize()
        probed_size = self.probed_queue.qsize()

        if queue_size == 0 and probed_size == 0:
            # Only log if we just became idle
            if self._last_scan_had_work:
                self.root.after(0, lambda: self.log(
                    "Ready queue empty - waiting for downloads...", "info"))
                self._last_scan_had_work = False
            self.root.after(0, lambda: self.current_file_label.config(text="Waiting for ready files..."))
            return

        self._last_scan_had_work = True
        self.root.after(0, lambda q=queue_size, p=probed_size: self.log(
            f"Queues: {p} pre-probed, {q} ready"))

        # Process files - PREFER probed_queue (instant) over ready_queue (needs probe)
        idx = 0
        while self.running:
            # Check disk space (use first watch folder)
            free_gb = self.get_free_disk_space(folders[0])
            if free_gb < 5:
                self.root.after(0, lambda g=free_gb: self.log(
                    f"Low disk space ({g:.1f} GB). Pausing...", "warning"))
                break

            # Try probed_queue first (INSTANT - no probe needed!)
            video_path = None
            file_size = 0
            probe_data = None

            try:
                item = self.probed_queue.get_nowait()
                if not isinstance(item, (tuple, list)) or len(item) < 3:
                    continue
                video_path, file_size, probe_data = item[0], item[1], item[2]
            except queue.Empty:
                pass
            except Exception:
                continue

            # probed_queue was empty, try ready_queue
            if video_path is None:
                try:
                    item = self.ready_queue.get_nowait()
                    if not isinstance(item, (tuple, list)) or len(item) < 2:
                        continue
                    video_path, file_size = item[0], item[1]
                    self._in_ready_queue.discard(str(video_path))
                except queue.Empty:
                    break
                except Exception:
                    continue

            if video_path is None:
                break

            # Double-check file is still valid
            if not video_path.exists() or self.is_processed(video_path):
                continue

            idx += 1
            total_queued = self.ready_queue.qsize() + self.probed_queue.qsize()
            self.root.after(0, lambda i=idx, q=total_queued:
                self.current_file_label.config(text=f"Processing {i} (queue: {q})"))

            # Wrap individual file processing in try/except so one bad file
            # doesn't crash the entire loop and corrupt the queue state
            try:
                if probe_data:
                    self.process_file_preprobed(video_path, probe_data, queue_pos=idx,
                                               queue_total=queue_size, file_size=file_size)
                else:
                    self.process_file(video_path, queue_pos=idx, queue_total=queue_size, file_size=file_size)
            except Exception as file_err:
                self.root.after(0, lambda p=video_path.name, e=file_err: self.log(
                    f"Error processing {p}: {e} — skipping", "error"))
                try:
                    self.mark_processed(video_path, "", "error", file_size, 0)
                except Exception:
                    pass

            files_processed_this_scan += 1

        # Notify user if we finished processing files and queue is empty
        if files_processed_this_scan > 0:
            self.notify_queue_finished()

    def process_audio_files(self, base_folder: Path) -> int:
        """
        Process WAV files in 'Audio Source Files' folders.
        Converts WAV to MP3 192kbps, verifies, then deletes original.
        WAV folder deletion is scheduled only after ALL files in each folder are done.
        Returns number of files processed.
        """
        if not self.running and not self.wav_running:
            return 0

        # Find WAV files ONLY in "Audio Source Files" folders
        wav_files = []
        for audio_folder in base_folder.rglob('Audio Source Files'):
            if not audio_folder.is_dir():
                continue
            for wav_path in list(audio_folder.glob('*.wav')) + list(audio_folder.glob('*.WAV')):
                if wav_path.name.startswith('._'):
                    continue
                # Skip if MP3 already exists (prevents duplicate processing)
                mp3_path = wav_path.with_suffix('.mp3')
                if mp3_path.exists():
                    if not self.is_processed(wav_path):
                        self.mark_processed(wav_path, str(mp3_path), "skipped_exists",
                                          wav_path.stat().st_size, mp3_path.stat().st_size)
                    continue
                if not self.is_processed(wav_path):
                    try:
                        size = wav_path.stat().st_size
                        wav_files.append((wav_path, size))
                    except Exception:
                        pass

        if not wav_files:
            return 0

        # Sort by size (smaller first)
        wav_files.sort(key=lambda x: x[1])

        self.root.after(0, lambda n=len(wav_files): self.log(
            f"Found {n} WAV files in Audio Source Files folders", "info"))

        # Track which folders had successful conversions
        folders_with_conversions = set()

        processed = 0
        for wav_path, size in wav_files:
            if not self.running and not self.wav_running:
                break

            # Check disk space
            free_gb = self.get_free_disk_space(base_folder)
            if free_gb < 2:  # WAV to MP3 needs less space
                self.root.after(0, lambda: self.log(
                    "Low disk space, pausing audio conversion", "warning"))
                break

            # Wait for file to be ready (downloaded from Dropbox)
            if not self.wait_for_file_ready(wav_path, estimated_size=size):
                continue

            self.root.after(0, lambda p=wav_path.name: self.log(
                f"Converting WAV: {p}", "info"))

            # Convert WAV to MP3
            if self.convert_wav_to_mp3(wav_path):
                processed += 1
                folders_with_conversions.add(wav_path.parent)

        # Schedule WAV folder deletion AFTER all files in each folder are done
        for folder in folders_with_conversions:
            wav_folder = folder / 'wav'
            if wav_folder.exists():
                # Check if any WAV files in the parent folder still lack MP3 counterparts
                remaining = [f for f in list(folder.glob('*.wav')) + list(folder.glob('*.WAV'))
                            if not f.name.startswith('._') and not f.with_suffix('.mp3').exists()]
                if not remaining:
                    self._schedule_wav_deletion_for_folder(wav_folder)
                else:
                    self.root.after(0, lambda n=len(remaining), p=folder.name: self.log(
                        f"WAV folder not scheduled for deletion: {n} files still pending in {p}", "info"))

        if processed > 0:
            self.root.after(0, lambda n=processed: self.log(
                f"Audio conversion complete: {n} files", "success"))

        return processed

    def convert_wav_to_mp3(self, wav_path: Path) -> bool:
        """
        Convert a WAV file to MP3 192kbps.
        Creates backup in 'wav' folder, converts, verifies, then deletes original.
        Returns True if successful.
        """
        try:
            # Create wav backup folder and mp3 output path
            wav_folder = wav_path.parent / 'wav'
            wav_folder.mkdir(parents=True, exist_ok=True)

            mp3_path = wav_path.with_suffix('.mp3')
            temp_mp3 = mp3_path.with_suffix('.mp3.tmp')

            # Skip if MP3 already exists
            if mp3_path.exists():
                self.root.after(0, lambda: self.log(
                    "MP3 already exists, skipping", "info"))
                self.mark_processed(wav_path, str(mp3_path), "skipped_exists",
                                  wav_path.stat().st_size, mp3_path.stat().st_size)
                return False

            # Get original file size
            input_size = wav_path.stat().st_size

            # Check if file is accessible (not online-only)
            if input_size < 1000:  # WAV files should be larger
                self.root.after(0, lambda: self.log(
                    f"WAV file too small (online-only?): {wav_path.name}", "warning"))
                return False

            # Try to read a bit of the file to ensure it's accessible
            try:
                with open(wav_path, 'rb') as f:
                    f.read(1024)
            except Exception as e:
                self.root.after(0, lambda: self.log(
                    f"WAV not accessible (cloud?): {wav_path.name}", "warning"))
                return False

            # Build FFmpeg command for WAV to MP3 conversion
            # Use -f mp3 to specify output format explicitly (needed for .tmp extension)
            cmd = [
                'ffmpeg', '-hide_banner', '-y',
                '-i', str(wav_path),
                '-codec:a', 'libmp3lame',
                '-b:a', '192k',
                '-f', 'mp3',
                str(temp_mp3)
            ]

            # Run FFmpeg (CPU only — doesn't compete with QSV video encoding)
            result = subprocess.run(
                cmd, capture_output=True, text=True,
                encoding='utf-8', errors='replace', timeout=300  # 5 min timeout
            )

            if result.returncode != 0:
                # Log actual error for debugging
                err_msg = result.stderr.split('\n')[-2] if result.stderr else "Unknown error"
                self.root.after(0, lambda e=err_msg: self.log(
                    f"FFmpeg failed: {e[:80]}", "error"))
                if temp_mp3.exists():
                    temp_mp3.unlink()
                self.mark_processed(wav_path, "", "error", input_size, 0)
                return False

            # Verify MP3 is valid
            if not self._verify_codec(temp_mp3, ('mp3',), stream_type='a:0'):
                self.root.after(0, lambda: self.log(
                    f"MP3 verification failed: {wav_path.name}", "error"))
                if temp_mp3.exists():
                    temp_mp3.unlink()
                self.mark_processed(wav_path, "", "error", input_size, 0)
                return False

            # Rename temp to final
            temp_mp3.rename(mp3_path)
            output_size = mp3_path.stat().st_size

            # Calculate savings
            reduction = (1 - output_size / input_size) * 100 if input_size > 0 else 0
            self.root.after(0, lambda r=reduction: self.log(
                f"MP3 created, {r:.1f}% smaller", "success"))

            # Move original WAV to backup folder
            wav_backup_path = wav_folder / wav_path.name
            shutil.move(str(wav_path), str(wav_backup_path))
            self.root.after(0, lambda: self.log(
                "WAV moved to backup folder", "info"))

            # Mark as processed (folder deletion is handled by process_audio_files)
            self.mark_processed(wav_path, str(mp3_path), "done", input_size, output_size)

            return True

        except Exception as e:
            self.root.after(0, lambda err=e: self.log(
                f"Error converting {wav_path.name}: {err}", "error"))
            return False

    def _schedule_wav_deletion_for_folder(self, wav_folder: Path):
        """
        Schedule WAV FOLDER deletion after 3 minutes (in background thread).
        Called from process_audio_files ONLY after all files in the folder are converted.
        """
        # Track folders already scheduled to avoid duplicates
        if not hasattr(self, '_scheduled_wav_folders'):
            self._scheduled_wav_folders = set()

        folder_key = str(wav_folder)
        if folder_key in self._scheduled_wav_folders:
            return

        self._scheduled_wav_folders.add(folder_key)
        parent_folder = wav_folder.parent  # Where MP3 files should be

        def delete_folder_after_delay():
            time.sleep(3 * 60)  # Wait 3 minutes for Dropbox to sync

            try:
                if not wav_folder.exists():
                    self._scheduled_wav_folders.discard(folder_key)
                    return

                # Calculate stats before deleting
                wav_files = list(wav_folder.glob('*.wav')) + list(wav_folder.glob('*.WAV'))
                if not wav_files:
                    self._scheduled_wav_folders.discard(folder_key)
                    return

                num_files = len(wav_files)
                total_size = sum(f.stat().st_size for f in wav_files)
                total_gb = total_size / (1024**3)

                shutil.rmtree(wav_folder)
                self.root.after(0, lambda p=wav_folder, n=num_files, g=total_gb: self.log(
                    f"WAV folder deleted: {n} files, {g:.2f}GB freed", "success"))

                # Log deletion to mp3 feito.txt
                mp3_folder = parent_folder / 'mp3'
                mp3_folder.mkdir(parents=True, exist_ok=True)
                log_file = mp3_folder / "mp3 feito.txt"
                try:
                    with open(log_file, 'a', encoding='utf-8') as f:
                        f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | WAV FOLDER DELETED | {num_files} files | {total_gb:.2f}GB freed\n")
                except Exception:
                    pass

                self._scheduled_wav_folders.discard(folder_key)

            except Exception as e:
                self.root.after(0, lambda err=e: self.log(
                    f"Could not delete WAV folder: {err}", "warning"))
                self._scheduled_wav_folders.discard(folder_key)

        self.root.after(0, lambda: self.log(
            f"WAV folder deletion scheduled for 3 min: {wav_folder.parent.name}", "info"))
        threading.Thread(target=delete_folder_after_delay, daemon=True).start()

    def _all_h264_have_h265(self, h264_folder: Path) -> bool:
        """Check if ALL video files in h264 folder already have h265 counterparts in parent."""
        parent_folder = h264_folder.parent
        h264_files = list(h264_folder.glob('*.mp4')) + list(h264_folder.glob('*.MP4')) + \
                     list(h264_folder.glob('*.mov')) + list(h264_folder.glob('*.MOV'))
        if not h264_files:
            return False
        return all((parent_folder / f.name).exists() for f in h264_files)

    def _delete_h264_folder(self, h264_folder: Path):
        """
        Delete h264 folder with retry on win32/permission errors.
        Retries at 2, 5, 30, 60, 90, 300 minutes if deletion fails.
        """
        parent_folder = h264_folder.parent
        folder_key = str(h264_folder)
        retry_delays = [2 * 60, 5 * 60, 30 * 60, 60 * 60, 90 * 60, 300 * 60]  # seconds

        for attempt in range(len(retry_delays) + 1):
            try:
                if not h264_folder.exists():
                    self.root.after(0, lambda: self.log(
                        f"H264 folder already gone: {h264_folder.name}", "info"))
                    return True

                folder_size = sum(f.stat().st_size for f in h264_folder.rglob('*') if f.is_file())
                folder_size_gb = folder_size / (1024**3)
                file_count = sum(1 for f in h264_folder.rglob('*') if f.is_file())

                # In offline mode, create txt marker instead of deleting
                if self.offline_mode.get():
                    marker_file = h264_folder / "h264 ok for deletion.txt"
                    with open(marker_file, 'w', encoding='utf-8') as f:
                        f.write(f"Verified: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                        f.write(f"Files: {file_count}\n")
                        f.write(f"Size: {folder_size_gb:.2f} GB\n")
                        f.write(f"PC: {self.pc_name}\n")
                        f.write("This folder is safe to delete.\n")
                    self.root.after(0, lambda p=h264_folder, n=file_count, s=folder_size_gb: self.log(
                        f"H264 marked for deletion: {n} files, {s:.2f} GB - {p.name}", "info"))
                    return True

                # Delete entire folder at once
                shutil.rmtree(h264_folder)
                self.root.after(0, lambda sz=folder_size: self.record_deletion(sz))
                self.root.after(0, lambda p=h264_folder, n=file_count, s=folder_size_gb: self.log(
                    f"✅ H264 FOLDER DELETED: {n} files, {s:.2f} GB freed - {p.name}", "success"))

                # Log deletion timestamp to h265 feito.txt
                h265_folder = parent_folder / 'h265'
                h265_folder.mkdir(parents=True, exist_ok=True)
                log_file = h265_folder / "h265 feito.txt"
                try:
                    with open(log_file, 'a', encoding='utf-8') as f:
                        f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | H264 FOLDER DELETED\n")
                except Exception:
                    pass
                return True

            except Exception as del_err:
                if attempt < len(retry_delays):
                    delay_min = retry_delays[attempt] // 60
                    self.root.after(0, lambda err=del_err, d=delay_min, a=attempt+1: self.log(
                        f"Could not delete h264 folder (attempt {a}): {err} — retrying in {d} min", "warning"))
                    time.sleep(retry_delays[attempt])
                else:
                    self.root.after(0, lambda err=del_err: self.log(
                        f"Could not delete h264 folder after all retries: {err}", "error"))
                    return False

        return False

    def _schedule_h264_deletion(self, h264_path: Path, h265_path: Path):
        """
        Schedule h264 FOLDER deletion (in background thread).
        If all h264 files already have h265 counterparts, deletes immediately.
        Otherwise waits 20 minutes for Dropbox to sync, then deletes.
        Retries on win32/permission errors at 2, 5, 30, 60, 90, 300 min.
        """
        h264_folder = h264_path.parent
        parent_folder = h264_folder.parent  # Where h265 files should be

        # Track folders already scheduled to avoid duplicates
        if not hasattr(self, '_scheduled_h264_folders'):
            self._scheduled_h264_folders = set()

        folder_key = str(h264_folder)
        if folder_key in self._scheduled_h264_folders:
            return

        self._scheduled_h264_folders.add(folder_key)

        def delete_folder_thread():
            try:
                # Check if all h264 files already have h265 counterparts
                if self._all_h264_have_h265(h264_folder):
                    self.root.after(0, lambda: self.log(
                        f"All h265 files exist — deleting h264 folder now: {h264_folder.name}", "info"))
                else:
                    # Wait 20 minutes for Dropbox to sync the new h265 file
                    self.root.after(0, lambda: self.log(
                        f"H264 folder deletion scheduled for 20 min: {h264_folder.name}", "info"))
                    time.sleep(20 * 60)

                self._delete_h264_folder(h264_folder)
            finally:
                self._scheduled_h264_folders.discard(folder_key)

        threading.Thread(target=delete_folder_thread, daemon=True).start()

    def run_all_cleanups(self):
        """Run all cleanup operations sequentially in one thread."""
        if getattr(self, '_cleanup_running', False):
            self.log("Cleanup already in progress, please wait...", "warning")
            return

        folder = Path(self.watch_folder.get())
        if not folder.exists():
            self.log(f"Folder not found: {folder}", "error")
            return

        self._cleanup_running = True

        def all_cleanups_thread():
            try:
                self._cleanup_h264(folder)
                self._cleanup_old_folders(folder, 'Proxies', 60, "Proxy",
                                          file_filter=lambda f: f.suffix.lower() == '.mov' and '_Proxy' in f.name)
                self._cleanup_old_folders(folder, 'Adobe Premiere Pro Video Previews', 30, "Premiere Preview")
            finally:
                self._cleanup_running = False

        threading.Thread(target=all_cleanups_thread, daemon=True).start()

    def _cleanup_h264(self, folder: Path):
        """Delete h264 folders older than 60 days where all files have valid h265 counterparts."""
        self.root.after(0, lambda: self.log("Scanning for orphaned h264 folders...", "info"))

        h264_folders = [f for f in folder.rglob('h264') if f.is_dir()]
        if not h264_folders:
            self.root.after(0, lambda: self.log("No h264 folders found", "info"))
            return

        self.root.after(0, lambda n=len(h264_folders): self.log(f"Found {n} h264 folders to check", "info"))

        deleted_count = 0
        total_freed_gb = 0
        total_files_deleted = 0
        now = time.time()
        sixty_days = 60 * 24 * 60 * 60

        for h264_folder in h264_folders:
            try:
                parent = h264_folder.parent
                h264_files = list(h264_folder.glob('*.mp4')) + list(h264_folder.glob('*.MP4'))
                if not h264_files:
                    continue

                # Use st_mtime — st_ctime resets when files are copied/synced via Dropbox
                if (now - max(f.stat().st_mtime for f in h264_files)) < sixty_days:
                    continue

                # Verify ALL h264 files have valid h265 counterparts (exist and > 10KB)
                if not all(
                    (parent / f.name).exists() and (parent / f.name).stat().st_size >= 10000
                    for f in h264_files
                ):
                    continue

                folder_size = sum(f.stat().st_size for f in h264_folder.rglob('*') if f.is_file())
                file_count = len(h264_files)

                shutil.rmtree(h264_folder)
                self.root.after(0, lambda sz=folder_size: self.record_deletion(sz))

                deleted_count += 1
                total_freed_gb += folder_size / (1024**3)
                total_files_deleted += file_count

                self.root.after(0, lambda p=parent.name, n=file_count, s=folder_size / (1024**3): self.log(
                    f"Deleted h264: {n} files, {s:.2f} GB freed - {p}", "success"))

                # Log to h265 feito.txt
                log_file = parent / 'h265' / "h265 feito.txt"
                log_file.parent.mkdir(parents=True, exist_ok=True)
                try:
                    with open(log_file, 'a', encoding='utf-8') as f:
                        f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | H264 FOLDER DELETED (cleanup)\n")
                except Exception:
                    pass

            except Exception as e:
                self.root.after(0, lambda err=e, p=h264_folder: self.log(
                    f"Could not delete {p.name}: {err}", "warning"))

        if deleted_count > 0:
            self.root.after(0, lambda d=deleted_count, f=total_files_deleted, g=total_freed_gb: self.log(
                f"H264 cleanup: {d} folders, {f} files, {g:.2f} GB freed", "success"))
        else:
            self.root.after(0, lambda: self.log("No orphaned h264 folders to delete (all < 60 days or missing h265)", "info"))

    def _cleanup_old_folders(self, root_folder: Path, folder_name: str, max_age_days: int,
                             label: str, file_filter=None):
        """
        Generic cleanup: find folders by name, delete if oldest file > max_age_days.
        file_filter: optional callable(Path) -> bool to select which files determine age.
                     If None, all files in the folder are used.
        """
        self.root.after(0, lambda: self.log(f"Scanning for old {label} folders...", "info"))

        folders = [f for f in root_folder.rglob(folder_name) if f.is_dir()]
        if not folders:
            self.root.after(0, lambda: self.log(f"No {label} folders found", "info"))
            return

        self.root.after(0, lambda n=len(folders): self.log(f"Found {n} {label} folders to check", "info"))

        deleted_count = 0
        total_freed_gb = 0
        total_files_deleted = 0
        now = time.time()
        max_age_seconds = max_age_days * 24 * 60 * 60

        for target_folder in folders:
            try:
                all_files = [f for f in target_folder.rglob('*') if f.is_file()]
                age_files = [f for f in all_files if file_filter(f)] if file_filter else all_files

                if not age_files:
                    continue

                newest_mtime = max(f.stat().st_mtime for f in age_files)
                age_days = (now - newest_mtime) / (24 * 60 * 60)
                if (now - newest_mtime) < max_age_seconds:
                    continue

                folder_size = sum(f.stat().st_size for f in all_files)
                file_count = len(all_files)

                shutil.rmtree(target_folder)
                self.root.after(0, lambda sz=folder_size: self.record_deletion(sz))

                deleted_count += 1
                total_freed_gb += folder_size / (1024**3)
                total_files_deleted += file_count

                self.root.after(0, lambda p=target_folder.parent.name, n=file_count, s=folder_size / (1024**3), d=int(age_days): self.log(
                    f"Deleted {label} ({d} days old): {n} files, {s:.2f} GB freed - {p}", "success"))

            except Exception as e:
                self.root.after(0, lambda err=e, p=target_folder: self.log(
                    f"Could not delete {p}: {err}", "warning"))

        if deleted_count > 0:
            self.root.after(0, lambda d=deleted_count, f=total_files_deleted, g=total_freed_gb: self.log(
                f"{label} cleanup: {d} folders, {f} files, {g:.2f} GB freed", "success"))
        else:
            self.root.after(0, lambda: self.log(f"No old {label} folders to delete (all < {max_age_days} days)", "info"))

    def _verify_codec(self, file_path: Path, expected_codecs: tuple,
                       stream_type: str = 'v:0', log: bool = False) -> bool:
        """Verify a media file's codec using ffprobe.

        Args:
            file_path: Path to the media file.
            expected_codecs: Tuple of acceptable codec names (lowercase).
            stream_type: 'v:0' for video, 'a:0' for audio.
            log: If True, log success/failure to the GUI.
        """
        try:
            if not file_path.exists():
                return False
            if file_path.stat().st_size < 1000:
                return False

            result = subprocess.run(
                ['ffprobe', '-v', 'error', '-select_streams', stream_type,
                 '-show_entries', 'stream=codec_name', '-of', 'csv=p=0',
                 str(file_path)],
                capture_output=True, text=True,
                encoding='utf-8', errors='replace', timeout=30
            )

            codec = result.stdout.strip().lower()
            matched = codec in expected_codecs
            if log:
                if matched:
                    self.root.after(0, lambda: self.log(f"Output verified: {codec}", "info"))
                else:
                    self.root.after(0, lambda c=codec: self.log(
                        f"Verification failed: codec={c}", "warning"))
            return matched

        except Exception as e:
            if log:
                self.root.after(0, lambda err=e: self.log(
                    f"Verification error: {err}", "warning"))
            return False

    def _record_transcode_speed(self, bytes_processed: int, seconds_taken: float):
        """Record a transcode for hourly speed calculation."""
        now = time.time()
        self._hourly_transcode_records.append((now, bytes_processed, seconds_taken))
        # Remove records older than 1 hour
        one_hour_ago = now - 3600
        self._hourly_transcode_records = [r for r in self._hourly_transcode_records if r[0] > one_hour_ago]

    def get_hourly_speed_gbh(self) -> float:
        """Calculate transcoding speed in GB/h based on last hour of work."""
        if not self._hourly_transcode_records:
            return 0.0
        now = time.time()
        one_hour_ago = now - 3600
        # Filter to last hour
        recent = [r for r in self._hourly_transcode_records if r[0] > one_hour_ago]
        if not recent:
            return 0.0
        total_bytes = sum(r[1] for r in recent)
        total_seconds = sum(r[2] for r in recent)
        if total_seconds <= 0:
            return 0.0
        # GB per hour = (bytes / seconds) * 3600 / (1024^3)
        bytes_per_second = total_bytes / total_seconds
        gb_per_hour = (bytes_per_second * 3600) / (1024**3)
        return gb_per_hour

    def record_deletion(self, bytes_deleted: int):
        """Record a deletion event for tracking GB freed."""
        now = time.time()
        self._deletion_records.append((now, bytes_deleted))
        # Keep records for 30 days max
        thirty_days_ago = now - (30 * 24 * 60 * 60)
        self._deletion_records = [r for r in self._deletion_records if r[0] > thirty_days_ago]
        # Persist to disk
        self._save_deletion_records()

    def _save_deletion_records(self):
        """Save deletion records to disk."""
        try:
            self.DELETION_RECORDS_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(self.DELETION_RECORDS_FILE, 'w', encoding='utf-8') as f:
                json.dump(self._deletion_records, f)
        except Exception:
            pass  # Silently fail if can't save

    def _load_deletion_records(self):
        """Load deletion records from disk."""
        try:
            if self.DELETION_RECORDS_FILE.exists():
                with open(self.DELETION_RECORDS_FILE, 'r', encoding='utf-8') as f:
                    records = json.load(f)
                    # Validate and filter: keep only records from last 30 days
                    now = time.time()
                    thirty_days_ago = now - (30 * 24 * 60 * 60)
                    self._deletion_records = [
                        (ts, bytes_del) for ts, bytes_del in records
                        if isinstance(ts, (int, float)) and isinstance(bytes_del, (int, float))
                        and ts > thirty_days_ago
                    ]
        except Exception:
            self._deletion_records = []  # Reset if can't load

    def get_deleted_gb_today(self) -> float:
        """Get total GB deleted today."""
        from datetime import datetime
        today = datetime.now().date()
        total_bytes = 0
        for ts, bytes_del in self._deletion_records:
            record_date = datetime.fromtimestamp(ts).date()
            if record_date == today:
                total_bytes += bytes_del
        return total_bytes / (1024**3)

    def get_deleted_gb_last_days(self, days: int = 7) -> float:
        """Get total GB deleted in the last N days."""
        cutoff = time.time() - (days * 24 * 60 * 60)
        total_bytes = sum(r[1] for r in self._deletion_records if r[0] > cutoff)
        return total_bytes / (1024**3)

    def get_free_disk_space(self, path: Path) -> float:
        """Get free disk space in GB for the drive containing path."""
        try:
            import ctypes
            free_bytes = ctypes.c_ulonglong(0)
            ctypes.windll.kernel32.GetDiskFreeSpaceExW(
                str(path), None, None, ctypes.pointer(free_bytes)
            )
            return free_bytes.value / (1024**3)
        except Exception:
            return 999  # Assume enough space if can't check

    def wait_for_file_ready(self, file_path: Path, timeout_minutes: int = 60, estimated_size: int = 0) -> bool:
        """
        Check if a file is ready (fully downloaded from Dropbox).
        If file is online-only, triggers download and returns False immediately
        so the program can continue with other files.
        Returns True if file is ready, False if not ready yet.

        Download queue management:
        - Tracks pending downloads to avoid triggering too many at once
        - Checks available disk space vs pending download sizes
        - Limits max concurrent pending downloads
        """
        path_str = str(file_path)

        try:
            # First check if file exists
            if not file_path.exists():
                self.root.after(0, lambda: self.log(
                    f"File not found, skipping", "warning"))
                self._remove_from_pending_downloads(path_str)
                return False

            # Get file size - this works even for online-only files
            current_size = file_path.stat().st_size

            # If file is very small, it's probably a placeholder (online-only)
            if current_size < 10000:  # Less than 10KB
                # In offline mode, just skip cloud files
                if self.offline_mode.get():
                    return False

                # Check if we can trigger another download (space + count limits)
                if not self._can_trigger_download(estimated_size):
                    pending_count, pending_gb = self._get_pending_download_stats()
                    self.root.after(0, lambda c=pending_count, g=pending_gb: self.log(
                        f"Download queue full ({c} files, {g:.1f} GB pending), skipping...", "info"))
                    return False

                self._add_to_pending_downloads(path_str, estimated_size)
                pending_count, pending_gb = self._get_pending_download_stats()
                self.root.after(0, lambda c=pending_count, g=pending_gb: self.log(
                    f"Online-only file, queued for download ({c} pending, {g:.1f} GB)", "info"))
                self._trigger_dropbox_download(file_path)
                return False  # Skip for now, will retry next scan

            # File has real content - remove from pending if it was there
            self._remove_from_pending_downloads(path_str)

            # Try to read some bytes to verify file is accessible
            try:
                with open(file_path, 'rb') as f:
                    f.read(1024)  # Read 1KB
            except OSError as e:
                if e.errno == 22:  # Invalid argument - online-only file
                    # In offline mode, just skip cloud files
                    if self.offline_mode.get():
                        return False

                    # Check if we can trigger another download
                    if not self._can_trigger_download(estimated_size):
                        pending_count, pending_gb = self._get_pending_download_stats()
                        self.root.after(0, lambda c=pending_count, g=pending_gb: self.log(
                            f"Download queue full ({c} files, {g:.1f} GB pending), skipping...", "info"))
                        return False

                    self._add_to_pending_downloads(path_str, estimated_size)
                    pending_count, pending_gb = self._get_pending_download_stats()
                    self.root.after(0, lambda c=pending_count, g=pending_gb: self.log(
                        f"Cloud file detected, queued for download ({c} pending, {g:.1f} GB)", "info"))
                    self._trigger_dropbox_download(file_path)
                    return False  # Skip for now, will retry next scan
                raise

            # Quick stability check (reduced from 2s since precheck already validated)
            time.sleep(0.3)
            new_size = file_path.stat().st_size

            if current_size == new_size and current_size > 10000:
                # File is stable and ready
                return True
            else:
                # Still downloading - keep in pending list
                progress_mb = new_size / (1024**2)
                self.root.after(0, lambda p=progress_mb: self.log(
                    f"File still downloading ({p:.1f} MB), moving to next...", "info"))
                return False

        except PermissionError:
            # File is being used by Dropbox - skip for now
            self.root.after(0, lambda: self.log(
                f"File locked by Dropbox, moving to next...", "info"))
            return False

        except Exception as e:
            # For other errors, skip this file for now
            self._remove_from_pending_downloads(path_str)
            self.root.after(0, lambda err=e: self.log(
                f"Cannot access file: {err} - skipping", "warning"))
            return False

    def _can_trigger_download(self, new_file_size: int) -> bool:
        """
        Check if we can trigger another download based on:
        1. Number of pending downloads (max 10)
        2. Available disk space vs pending + new download sizes
        """
        with self.pending_downloads_lock:
            # Check count limit
            if len(self.pending_downloads) >= self.max_pending_downloads:
                return False

            # Check space limit
            pending_bytes = sum(self.pending_downloads.values())
            total_needed = pending_bytes + new_file_size
            total_needed_gb = total_needed / (1024**3)

            folder = Path(self.watch_folder.get())
            free_gb = self.get_free_disk_space(folder)

            # Need at least min_free_space_gb after pending downloads complete
            if free_gb - total_needed_gb < self.min_free_space_gb:
                return False

            return True

    def _add_to_pending_downloads(self, path_str: str, size_bytes: int) -> None:
        """Add a file to pending downloads tracking."""
        with self.pending_downloads_lock:
            self.pending_downloads[path_str] = size_bytes

    def _remove_from_pending_downloads(self, path_str: str) -> None:
        """Remove a file from pending downloads tracking."""
        with self.pending_downloads_lock:
            self.pending_downloads.pop(path_str, None)

    def _get_pending_download_stats(self) -> tuple:
        """Get (count, total_gb) of pending downloads."""
        with self.pending_downloads_lock:
            count = len(self.pending_downloads)
            total_bytes = sum(self.pending_downloads.values())
            return count, total_bytes / (1024**3)

    def _update_download_status_ui(self):
        """Update the download status display in the UI. Runs every 2 seconds."""
        try:
            with self.pending_downloads_lock:
                count = len(self.pending_downloads)
                total_bytes = sum(self.pending_downloads.values())
                total_gb = total_bytes / (1024**3)

                if count == 0:
                    self.download_queue_label.config(
                        text="No pending downloads",
                        foreground="gray"
                    )
                    self.download_files_label.config(text="")
                else:
                    # Calculate download progress and estimate ETA
                    downloaded_bytes = 0
                    downloading_files = []

                    for path_str, expected_size in self.pending_downloads.items():
                        try:
                            file_path = Path(path_str)
                            if file_path.exists():
                                current_size = file_path.stat().st_size
                                if current_size > 10000:  # File is downloading
                                    downloaded_bytes += current_size
                                    pct = (current_size / expected_size * 100) if expected_size > 0 else 0
                                    downloading_files.append(f"{file_path.name} ({pct:.0f}%)")
                                else:
                                    downloading_files.append(f"{file_path.name} (waiting)")
                            else:
                                downloading_files.append(f"{Path(path_str).name} (queued)")
                        except Exception:
                            downloading_files.append(f"{Path(path_str).name} (queued)")

                    # Calculate ETA based on average Dropbox speed (~5-20 MB/s typical)
                    remaining_bytes = total_bytes - downloaded_bytes
                    remaining_gb = remaining_bytes / (1024**3)

                    # Estimate: assume 10 MB/s average download speed
                    avg_speed_mbps = 10
                    eta_seconds = remaining_bytes / (avg_speed_mbps * 1024 * 1024)
                    eta_str = ""
                    if eta_seconds > 3600:
                        eta_str = f"~{eta_seconds/3600:.1f}h"
                    elif eta_seconds > 60:
                        eta_str = f"~{eta_seconds/60:.0f}min"
                    else:
                        eta_str = f"~{eta_seconds:.0f}s"

                    self.download_queue_label.config(
                        text=f"⬇️ {count} files downloading ({total_gb:.2f} GB) - ETA: {eta_str}",
                        foreground="blue"
                    )

                    # Show up to 5 filenames
                    if downloading_files:
                        display_files = downloading_files[:5]
                        if len(downloading_files) > 5:
                            display_files.append(f"... and {len(downloading_files) - 5} more")
                        self.download_files_label.config(text="  " + ", ".join(display_files))
                    else:
                        self.download_files_label.config(text="")

        except Exception as e:
            pass  # Don't crash if UI update fails

        # Schedule next update in 2 seconds
        self.root.after(2000, self._update_download_status_ui)

    def _cleanup_pending_downloads(self) -> None:
        """
        Clean up pending downloads list:
        - Remove files that have been fully downloaded (size > 10KB and readable)
        - Remove files that no longer exist
        """
        with self.pending_downloads_lock:
            if not self.pending_downloads:
                return

            completed = []
            removed = []

            for path_str, expected_size in list(self.pending_downloads.items()):
                try:
                    file_path = Path(path_str)
                    if not file_path.exists():
                        removed.append(path_str)
                        continue

                    # Check if file has real content (not placeholder)
                    current_size = file_path.stat().st_size
                    if current_size > 10000:  # More than 10KB
                        # Try to read to confirm it's accessible
                        try:
                            with open(file_path, 'rb') as f:
                                f.read(1024)
                            completed.append(path_str)
                        except OSError:
                            pass  # Still downloading or cloud-only
                except Exception:
                    removed.append(path_str)

            # Remove completed and missing files from pending list
            for path_str in completed + removed:
                self.pending_downloads.pop(path_str, None)

            if completed or removed:
                remaining = len(self.pending_downloads)
                self.root.after(0, lambda c=len(completed), r=len(removed), rem=remaining:
                    self.log(f"Download queue: {c} completed, {r} removed, {rem} pending", "info"))

    def _is_cloud_only_file(self, file_path: Path) -> bool:
        """
        Check if a file is cloud-only (online-only) in Dropbox WITHOUT triggering download.
        Uses attrib command to check file attributes safely.
        Returns True if file is cloud-only, False if local.
        """
        try:
            # Use attrib to check file attributes - this doesn't trigger download
            result = subprocess.run(
                ['attrib', str(file_path)],
                capture_output=True, text=True,
                encoding='utf-8', errors='replace', timeout=5
            )
            if result.returncode == 0:
                # Output format: "     O          P    path" or similar
                # O = Offline (cloud-only), P = Pinned (local), U = Unpinned
                attrs = result.stdout.strip()
                # Check for 'O' attribute (Offline/cloud-only) or 'U' without 'P' (unpinned)
                # Files with 'O' or 'U' but not 'P' are cloud-only
                if ' O ' in attrs or (' U ' in attrs and ' P ' not in attrs):
                    return True
                # Also check by file size - very small files are placeholders
                try:
                    size = file_path.stat().st_size
                    if size < 1000:  # Less than 1KB is definitely a placeholder
                        return True
                except Exception:
                    pass
            return False
        except Exception:
            # If attrib fails, fall back to size check only
            try:
                size = file_path.stat().st_size
                return size < 1000  # Placeholder files are tiny
            except Exception:
                return True  # Assume cloud if we can't check

    def _trigger_dropbox_download(self, file_path: Path):
        """
        Try to trigger Dropbox to download a cloud-only file.
        Uses attrib to set file as pinned (always available).
        """
        try:
            # Use attrib to pin file (request download) - fast, no PowerShell
            subprocess.Popen(
                ['attrib', '-U', '+P', str(file_path)],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
        except Exception:
            pass

    def process_file_preprobed(self, input_path: Path, probe_data: dict, queue_pos: int = 0,
                                queue_total: int = 0, file_size: int = 0):
        """
        Process a file that was already probed (INSTANT - no wait, no probe).
        This is called for files from the probed_queue.
        """
        queue_str = f"[{queue_pos}/{queue_total}] " if queue_pos else ""
        self.root.after(0, lambda q=queue_str: self.current_file_label.config(
            text=f"{q}Processing: {input_path.name}"))
        self.root.after(0, lambda q=queue_str: self.log(f"{q}Processing: {input_path.name} (pre-probed)"))

        size_gb = input_path.stat().st_size / (1024**3)
        self.root.after(0, lambda s=size_gb: self.current_file_label.config(
            text=f"Processing: {input_path.name} ({s:.2f} GB)"))

        # Output path
        output_folder = input_path.parent / 'h265'
        output_path = output_folder / input_path.name

        if output_path.exists():
            self.root.after(0, lambda: self.log("Output already exists, skipping", "info"))
            self.mark_processed(input_path, str(output_path), "skipped_exists",
                              input_path.stat().st_size, output_path.stat().st_size)
            return

        # Transcode with encoder fallback (QSV/NVENC → CPU)
        output_folder.mkdir(parents=True, exist_ok=True)
        temp_path = output_path.with_suffix(output_path.suffix + '.tmp')
        duration = self.get_duration(probe_data)

        success, transcode_start, last_error = self._encode_with_fallback(input_path, temp_path, probe_data, duration)

        if not success:
            if self.running:
                if self._is_permanent_error(last_error):
                    self.root.after(0, lambda e=last_error[:80]: self.log(
                        f"Permanent error, skipping: {e}", "warning"))
                    self.mark_processed(input_path, "", "skipped_permanent_error", 0, 0)
                else:
                    self.root.after(0, lambda: self.log("All encoders failed", "error"))
                    self.mark_processed(input_path, "", "error", 0, 0)
            return

        self._finish_successful_transcode(input_path, output_path, temp_path, transcode_start)

    def process_file(self, input_path: Path, queue_pos: int = 0, queue_total: int = 0, file_size: int = 0):
        """Process a single file."""
        process_start = time.time()
        queue_str = f"[{queue_pos}/{queue_total}] " if queue_pos else ""
        self.root.after(0, lambda q=queue_str: self.current_file_label.config(
            text=f"{q}Processing: {input_path.name}"))
        self.root.after(0, lambda q=queue_str: self.log(f"{q}Processing: {input_path.name}"))

        # Wait for file to be fully downloaded from Dropbox (with download queue balancing)
        if not self.wait_for_file_ready(input_path, estimated_size=file_size):
            return

        wait_time = time.time() - process_start

        size_gb = input_path.stat().st_size / (1024**3)
        self.root.after(0, lambda: self.current_file_label.config(
            text=f"Processing: {input_path.name} ({size_gb:.2f} GB)"))

        # Probe video
        probe_start = time.time()
        probe_data = self.probe_video(input_path)
        probe_time = time.time() - probe_start

        if not probe_data:
            self.root.after(0, lambda: self.log("Could not probe video", "error"))
            self.mark_processed(input_path, "", "error", 0, 0)
            return

        # Log timing if significant delay
        total_prep_time = time.time() - process_start
        if total_prep_time > 5:
            self.root.after(0, lambda w=wait_time, p=probe_time, t=total_prep_time: self.log(
                f"Prep time: {t:.1f}s (wait:{w:.1f}s, probe:{p:.1f}s)", "info"))

        # Check if already HEVC
        if self.is_hevc(probe_data):
            self.root.after(0, lambda: self.log("Already HEVC, skipping", "info"))
            self.mark_processed(input_path, "", "skipped_hevc", input_path.stat().st_size, 0)
            return

        # Check if already well-compressed (low bitrate)
        # Files with bitrate < 8 Mbps are already efficiently compressed
        file_size = input_path.stat().st_size
        bitrate = self.get_bitrate(probe_data, file_size)
        if bitrate > 0 and bitrate < 8:  # Less than 8 Mbps
            self.root.after(0, lambda b=bitrate: self.log(
                f"Already well-compressed ({b:.1f} Mbps), skipping", "info"))
            self.mark_processed(input_path, "", "skipped_lowbitrate", file_size, 0)
            return

        # Output path
        output_folder = input_path.parent / 'h265'
        output_path = output_folder / input_path.name

        if output_path.exists():
            self.root.after(0, lambda: self.log("Output already exists, skipping", "info"))
            self.mark_processed(input_path, str(output_path), "skipped_exists",
                              input_path.stat().st_size, output_path.stat().st_size)
            return

        # Transcode with encoder fallback (QSV/NVENC → CPU)
        output_folder.mkdir(parents=True, exist_ok=True)
        temp_path = output_path.with_suffix(output_path.suffix + '.tmp')
        duration = self.get_duration(probe_data)

        success, transcode_start, last_error = self._encode_with_fallback(input_path, temp_path, probe_data, duration)

        if success:
            self._finish_successful_transcode(input_path, output_path, temp_path, transcode_start, duration)
        else:
            if self._is_permanent_error(last_error):
                self.root.after(0, lambda e=last_error[:80]: self.log(
                    f"Permanent error, skipping: {e}", "warning"))
                self.mark_processed(input_path, "", "skipped_permanent_error", 0, 0)
            else:
                self.root.after(0, lambda: self.log("All encoders failed!", "error"))
                self.mark_processed(input_path, "", "error", 0, 0)

        # Cleanup
        self.current_process = None
        self.root.after(0, lambda: self.progress_var.set(0))
        self.root.after(0, lambda: self.progress_label.config(text=""))
        self.root.after(0, lambda: self.current_file_label.config(text="Idle"))

    # Error patterns that indicate permanent failures (retrying won't help)
    PERMANENT_ERROR_PATTERNS = [
        'invalid data found',
        'corrupt',
        'moov atom not found',
        'invalid nal',
        'no decoder',
        'could not find codec',
        'not a video',
        'invalid argument',
        'unrecognized option',
        'protocol not found',
        'no such file or directory',
    ]

    def _is_permanent_error(self, error_msg: str) -> bool:
        """Check if an FFmpeg error indicates a permanent failure."""
        error_lower = error_msg.lower()
        return any(pattern in error_lower for pattern in self.PERMANENT_ERROR_PATTERNS)

    def _encode_with_fallback(self, input_path: Path, temp_path: Path,
                              probe_data: dict, duration: float) -> tuple:
        """
        Try encoding with current encoder, fallback to CPU if it fails.
        Returns (success: bool, transcode_start_time: float, last_error: str).
        """
        encoder = self.encoder.get()
        encoders_to_try = [encoder]
        if encoder != 'cpu':
            encoders_to_try.append('cpu')

        transcode_start = time.time()
        last_error = ""

        for try_encoder in encoders_to_try:
            if not self.running:
                break

            self.root.after(0, lambda e=try_encoder: self.log(f"Encoding with {e}..."))
            cmd = self.build_ffmpeg_command(input_path, temp_path, encoder=try_encoder, probe_data=probe_data)
            self.root.after(0, lambda: self.progress_var.set(0))

            success, error_msg = self._run_ffmpeg(cmd, duration)

            if success and temp_path.exists():
                if self._verify_codec(temp_path, ('hevc', 'h265'), log=True):
                    return (True, transcode_start, "")
                self.root.after(0, lambda e=try_encoder: self.log(
                    f"Output verification failed with {e}", "warning"))
                temp_path.unlink(missing_ok=True)
            else:
                last_error = error_msg or "Unknown error"
                if error_msg:
                    last_err = error_msg.split('\n')[-1] if error_msg else "Unknown"
                    self.root.after(0, lambda e=try_encoder, err=last_err: self.log(
                        f"{e} failed: {err[:100]}", "warning"))
                # If permanent error, skip remaining encoders (they'll fail too)
                if self._is_permanent_error(last_error):
                    self.root.after(0, lambda: self.log(
                        "Permanent error detected — skipping file", "warning"))
                    temp_path.unlink(missing_ok=True)
                    break
                if try_encoder != encoders_to_try[-1]:
                    self.root.after(0, lambda: self.log("Trying fallback encoder...", "info"))
                temp_path.unlink(missing_ok=True)

        return (False, transcode_start, last_error)

    def _finish_successful_transcode(self, input_path: Path, output_path: Path, temp_path: Path,
                                     transcode_start_time: float, duration: float = 0):
        """
        Handle successful transcode: rename temp, move files, log, cleanup.
        Extracted to share between process_file and process_file_preprobed.
        """
        output_folder = output_path.parent

        self._move_with_retry(temp_path, output_path)
        input_size = input_path.stat().st_size
        output_size = output_path.stat().st_size
        reduction = (1 - output_size/input_size) * 100
        transcode_time = time.time() - transcode_start_time
        input_size_gb = input_size / (1024**3)

        self.root.after(0, lambda r=reduction, t=transcode_time, s=input_size_gb: self.log(
            f"Done! {s:.2f} GB → {r:.1f}% smaller in {t:.0f}s", "success"))

        # Track for hourly speed calculation
        self._record_transcode_speed(input_size, transcode_time)

        # Reorganize files
        try:
            h264_folder = input_path.parent / 'h264'
            h264_folder.mkdir(parents=True, exist_ok=True)
            h264_backup_path = h264_folder / input_path.name

            self._move_with_retry(input_path, h264_backup_path)
            self.root.after(0, lambda: self.log(
                f"Moved original to h264/{input_path.name}", "info"))

            final_path = input_path
            self._move_with_retry(output_path, final_path)
            self.root.after(0, lambda: self.log(
                f"Moved H.265 to original location", "info"))

            self.mark_processed(h264_backup_path, str(final_path), "done", input_size, output_size,
                               duration=duration, transcode_time=transcode_time)
            self.write_success_log(h264_backup_path, final_path, input_size, output_size)
            self.write_h265_done_log(output_folder, input_path.name, input_size, output_size)

            # v2.0: Atualizar folder_tracker
            self._mark_item_done(input_path)

            if self.auto_delete_h264.get():
                self._schedule_h264_deletion(h264_backup_path, final_path)

        except Exception as move_err:
            self.root.after(0, lambda e=move_err: self.log(
                f"File reorganization failed: {e}", "error"))
            self.mark_processed(input_path, str(output_path), "done", input_size, output_size,
                               duration=duration, transcode_time=transcode_time)
            self.write_success_log(input_path, output_path, input_size, output_size)
            # v2.0: Atualizar folder_tracker mesmo em caso de erro de reorganização
            self._mark_item_done(input_path)

    def probe_video(self, path: Path) -> dict:
        """Get video info."""
        try:
            cmd = ['ffprobe', '-v', 'quiet', '-print_format', 'json',
                   '-show_format', '-show_streams', str(path)]
            result = subprocess.run(cmd, capture_output=True, text=True,
                                       encoding='utf-8', errors='replace', timeout=30)
            if result.returncode == 0:
                return json.loads(result.stdout)
        except Exception:
            pass
        return None

    def is_hevc(self, probe_data: dict) -> bool:
        """Check if video is HEVC."""
        for stream in probe_data.get('streams', []):
            if stream.get('codec_type') == 'video':
                codec = stream.get('codec_name', '').lower()
                return codec in ('hevc', 'h265')
        return False

    def get_bitrate(self, probe_data: dict, file_size: int) -> float:
        """
        Get video bitrate in Mbps from probe data.
        Returns bitrate in Mbps (megabits per second).
        """
        try:
            # Try to get bitrate from format
            if 'format' in probe_data and 'bit_rate' in probe_data['format']:
                bitrate = int(probe_data['format']['bit_rate'])
                return bitrate / 1_000_000  # Convert to Mbps

            # Try to get bitrate from video stream
            for stream in probe_data.get('streams', []):
                if stream.get('codec_type') == 'video' and 'bit_rate' in stream:
                    bitrate = int(stream['bit_rate'])
                    return bitrate / 1_000_000  # Convert to Mbps

            # Calculate from file size and duration
            duration = self.get_duration(probe_data)
            if duration > 0 and file_size > 0:
                # file_size is in bytes, duration in seconds
                # bitrate = (bytes * 8) / seconds = bits/second
                bitrate = (file_size * 8) / duration
                return bitrate / 1_000_000  # Convert to Mbps
        except (ValueError, TypeError, ZeroDivisionError):
            pass
        return 0

    def get_duration(self, probe_data: dict) -> float:
        """Get video duration in seconds from probe data."""
        try:
            # Try format duration first
            if 'format' in probe_data and 'duration' in probe_data['format']:
                return float(probe_data['format']['duration'])
            # Try stream duration
            for stream in probe_data.get('streams', []):
                if stream.get('codec_type') == 'video' and 'duration' in stream:
                    return float(stream['duration'])
        except (ValueError, TypeError):
            pass
        return 0

    def parse_ffmpeg_time(self, line: str) -> float:
        """Parse time from FFmpeg output line. Returns seconds or -1 if not found."""
        try:
            # Match time=HH:MM:SS.mm or time=SS.mm
            match = re.search(r'time=(\d+):(\d+):(\d+)\.(\d+)', line)
            if match:
                h, m, s, ms = match.groups()
                return int(h) * 3600 + int(m) * 60 + int(s) + int(ms) / 100
            # Try simpler format time=SS.mm
            match = re.search(r'time=(\d+)\.(\d+)', line)
            if match:
                s, ms = match.groups()
                return int(s) + int(ms) / 100
        except Exception:
            pass
        return -1

    def _format_eta(self, seconds: float) -> str:
        """Format seconds into human-readable ETA string."""
        if seconds < 0:
            return "calculating..."
        if seconds < 60:
            return f"{int(seconds)}s"
        elif seconds < 3600:
            m, s = divmod(int(seconds), 60)
            return f"{m}m {s}s"
        else:
            h, remainder = divmod(int(seconds), 3600)
            m, s = divmod(remainder, 60)
            return f"{h}h {m}m"

    def _run_ffmpeg(self, cmd: list, duration: float) -> tuple:
        """
        Run FFmpeg command and track progress.
        Returns (success: bool, error_msg: str)
        """
        try:
            self.root.after(0, lambda: self.log(f"CMD: {' '.join(cmd[:6])}...", "info"))

            self.current_process = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, encoding='utf-8', errors='replace'
            )
            process = self.current_process

            last_lines = []
            start_time = time.time()
            last_video_time = 0

            for line in process.stdout:
                if not self.running:
                    process.terminate()
                    return False, "Stopped by user"

                last_lines.append(line.strip())
                if len(last_lines) > 10:
                    last_lines.pop(0)

                if 'time=' in line:
                    current_time = self.parse_ffmpeg_time(line)
                    if duration > 0 and current_time >= 0:
                        progress = min(100, (current_time / duration) * 100)
                        self.root.after(0, lambda p=progress: self.progress_var.set(p))

                        # Calculate ETA
                        elapsed = time.time() - start_time
                        if current_time > 0 and elapsed > 2:  # After 2 seconds
                            speed = current_time / elapsed  # video seconds per real second
                            remaining_video = duration - current_time
                            if speed > 0:
                                eta_seconds = remaining_video / speed
                                eta_str = self._format_eta(eta_seconds)
                                self.root.after(0, lambda p=progress, e=eta_str:
                                    self.progress_label.config(text=f"{p:.1f}% - ETA: {e}"))
                            else:
                                self.root.after(0, lambda p=progress:
                                    self.progress_label.config(text=f"{p:.1f}%"))
                        else:
                            self.root.after(0, lambda p=progress:
                                self.progress_label.config(text=f"{p:.1f}%"))
                    else:
                        self.root.after(0, lambda l=line: self.progress_label.config(
                            text=l.strip()[-80:]))

            process.wait()

            if process.returncode == 0:
                return True, ""
            else:
                error_msg = "\n".join(last_lines[-5:]) if last_lines else "Unknown error"
                return False, error_msg

        except Exception as e:
            return False, str(e)
        finally:
            self.current_process = None


    def is_10bit(self, probe_data: dict) -> bool:
        """Check if video is 10-bit."""
        try:
            for stream in probe_data.get('streams', []):
                if stream.get('codec_type') == 'video':
                    pix_fmt = stream.get('pix_fmt', '').lower()
                    bits = stream.get('bits_per_raw_sample', '')
                    # Common 10-bit pixel formats: yuv420p10le, p010le, yuv422p10le, etc.
                    # Must check for '10' followed by 'le' or 'be' or at end, not just '10' anywhere
                    if 'p10' in pix_fmt or '10le' in pix_fmt or '10be' in pix_fmt:
                        return True
                    # Check bits_per_raw_sample
                    if bits:
                        try:
                            if int(bits) >= 10:
                                return True
                        except ValueError:
                            pass
        except Exception:
            pass
        return False

    def build_ffmpeg_command(self, input_path: Path, output_path: Path, encoder: str = None, probe_data: dict = None) -> list:
        """Build FFmpeg command with 10-bit and metadata preservation."""
        if encoder is None:
            encoder = self.encoder.get()
        cq = self.cq_value.get()

        # Base command with metadata preservation:
        # -map 0:v = copy video stream
        # -map 0:a? = copy audio if exists (? = optional)
        # -map_metadata 0 = copy all metadata from input
        # -movflags use_metadata_tags = preserve additional metadata tags
        base_cmd = [
            'ffmpeg', '-hide_banner', '-y', '-i', str(input_path),
            '-map', '0:v',  # Map video stream
            '-map', '0:a?',  # Map audio if exists (optional)
            '-map_metadata', '0',  # Copy all metadata
            '-movflags', '+use_metadata_tags+faststart',  # Preserve metadata + web optimization
        ]

        if encoder == 'nvenc':
            # NVENC auto-detects bit depth, no need for profile (causes compatibility issues)
            video_opts = ['-c:v', 'hevc_nvenc', '-preset', 'p5', '-rc:v', 'vbr', '-cq:v', str(cq)]
        elif encoder == 'qsv':
            video_opts = ['-c:v', 'hevc_qsv', '-preset', 'medium', '-global_quality:v', str(cq)]
        else:  # cpu (libx265)
            # libx265 auto-detects bit depth from input
            video_opts = ['-c:v', 'libx265', '-preset', 'medium', '-crf', str(cq)]

        # Audio: copy without re-encoding
        audio_opts = ['-c:a', 'copy']

        # Output format
        output_opts = ['-f', 'mp4', str(output_path)]

        return base_cmd + video_opts + audio_opts + output_opts


def main():
    root = tk.Tk()

    # Try to use a modern theme
    try:
        root.tk.call("source", "azure.tcl")
        root.tk.call("set_theme", "light")
    except Exception:
        pass

    app = TranscoderGUI(root)
    root.mainloop()


if __name__ == '__main__':
    main()
