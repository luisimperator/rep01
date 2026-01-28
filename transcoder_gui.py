#!/usr/bin/env python3
"""
HeavyDrops Transcoder v1.0

Dropbox Video Transcoder - GUI Version
Simple graphical interface for local folder transcoding.

Features:
- H.264 to H.265/HEVC video transcoding
- Hardware acceleration: NVIDIA NVENC, Intel QSV, CPU fallback
- Dropbox integration with online-only file handling
- Auto-organizes files: h264/ backup folder, h265 to original location
- Marks backups as online-only to free local space
- Queue management: smaller files first, disk space monitoring
- Progress bar with ETA, queue counter
- START/PAUSE/STOP controls
- Beep notification when queue finishes
"""

VERSION = "1.2.6"

import socket
import subprocess
import sys
import time
import json
import re
import sqlite3
import shutil
import threading
from pathlib import Path
from datetime import datetime
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext

# Windows-specific for beep sound
try:
    import winsound
    HAS_WINSOUND = True
except ImportError:
    HAS_WINSOUND = False


class TranscoderGUI:
    # Settings file path
    SETTINGS_FILE = Path(r"C:\transcoder\settings.json")

    def __init__(self, root):
        self.root = root
        self.root.title(f"HeavyDrops Transcoder v{VERSION} - H.264 → H.265")
        self.root.geometry("900x700")
        self.root.minsize(800, 600)

        # State
        self.running = False
        self.paused = False
        self.wav_running = False
        self.current_process = None  # Current FFmpeg process
        self.worker_thread = None
        self.current_file = None
        self.db_conn = None
        self.files_in_batch = 0  # Track files processed in current batch

        # Default settings
        self.watch_folder = tk.StringVar(value=r"D:\HeavyDrops Dropbox\HeavyDrops\App h265 Converter")
        self.log_folder = tk.StringVar(value=r"D:\HeavyDrops Dropbox\HeavyDrops\App h265 Converter\logs")
        self.min_size_gb = tk.DoubleVar(value=0)
        self.encoder = tk.StringVar(value="nvenc")
        self.cq_value = tk.IntVar(value=24)
        self.auto_delete_h264 = tk.BooleanVar(value=False)  # Delete h264 backups after verification

        # Stats
        self.files_processed = tk.IntVar(value=0)
        self.total_saved_gb = tk.DoubleVar(value=0)

        # Load saved settings
        self.load_settings()

        self.setup_ui()
        self.setup_database()
        self.load_stats()
        self.check_ffmpeg()

        # Save settings when window closes
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    def setup_ui(self):
        """Create the UI."""
        # Main container
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # === SETTINGS FRAME ===
        settings_frame = ttk.LabelFrame(main_frame, text="Settings", padding="10")
        settings_frame.pack(fill=tk.X, pady=(0, 10))

        # Watch folder
        ttk.Label(settings_frame, text="Watch Folder:").grid(row=0, column=0, sticky=tk.W, pady=5)
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

        self.current_file_label = ttk.Label(progress_frame, text="Idle", font=("", 9))
        self.current_file_label.pack(fill=tk.X)

        # Progress bar with percentage
        self.progress_var = tk.DoubleVar(value=0)
        self.progress_bar = ttk.Progressbar(progress_frame, mode='determinate', variable=self.progress_var, maximum=100)
        self.progress_bar.pack(fill=tk.X, pady=(5, 0))

        self.progress_label = ttk.Label(progress_frame, text="", font=("", 8))
        self.progress_label.pack(fill=tk.X, pady=(5, 0))

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
            result = subprocess.run(['ffmpeg', '-version'], capture_output=True, text=True, timeout=10)
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
        """Open folder browser dialog."""
        folder = filedialog.askdirectory(initialdir=self.watch_folder.get())
        if folder:
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
                'auto_delete_h264': self.auto_delete_h264.get()
            }
            with open(self.SETTINGS_FILE, 'w', encoding='utf-8') as f:
                json.dump(settings, f, indent=2)
        except Exception:
            pass  # Silently fail if can't save

    def on_close(self):
        """Handle window close."""
        self.save_settings()
        self.running = False
        self.root.destroy()

    def _move_with_retry(self, src: Path, dst: Path, max_retries: int = 5):
        """
        Move file with retry logic and exponential backoff.
        Handles file locks from FFmpeg/Dropbox (can hold locks for 20-60s).
        Backoff: 2s → 5s → 10s → 20s → 30s
        """
        delays = [2, 5, 10, 20, 30]  # Exponential backoff
        last_error = None
        for attempt in range(max_retries):
            try:
                shutil.move(str(src), str(dst))
                return  # Success
            except PermissionError as e:
                last_error = e
                if attempt < max_retries - 1:
                    delay = delays[min(attempt, len(delays)-1)]
                    self.root.after(0, lambda a=attempt+1, d=delay: self.log(
                        f"File locked, retry {a}/{max_retries-1} in {d}s...", "info"))
                    time.sleep(delay)
            except Exception as e:
                raise e  # Re-raise non-permission errors immediately

        # All retries failed
        raise last_error

    def set_dropbox_online_only(self, file_path: Path):
        """
        Mark a file as online-only in Dropbox to free up local space.
        Uses Windows attrib command to set Unpinned attribute.
        """
        try:
            # Use attrib to mark as Unpinned (+U) and remove Pinned (-P)
            # This tells Dropbox to make the file online-only
            result = subprocess.run(
                ['attrib', '+U', '-P', str(file_path)],
                capture_output=True, text=True, timeout=30
            )
            if result.returncode == 0:
                self.root.after(0, lambda p=file_path.name: self.log(
                    f"Marked as online-only: {p}", "info"))
                return True
            else:
                self.root.after(0, lambda: self.log(
                    f"Could not set online-only (attrib failed)", "warning"))
        except FileNotFoundError:
            self.root.after(0, lambda: self.log(
                "attrib command not found", "warning"))
        except Exception as e:
            self.root.after(0, lambda err=e: self.log(
                f"Error setting online-only: {err}", "warning"))
        return False

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
            except:
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
        except:
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
            video_extensions = ['.mp4', '.MP4']

            all_videos = []
            h264_backups = []
            h265_outputs = []

            # Scan all video files (skip ._ metadata files from macOS/ATEM)
            for ext in video_extensions:
                for f in folder.rglob(f'*{ext}'):
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
                    except:
                        pass

            # Get database stats
            cursor = self.db_conn.execute(
                "SELECT COUNT(*), SUM(input_size), SUM(output_size) FROM processed WHERE status = 'done'"
            )
            db_row = cursor.fetchone()
            db_count = db_row[0] or 0
            db_input_total = db_row[1] or 0
            db_output_total = db_row[2] or 0

            # Get orphan entries (files in DB but no longer exist)
            cursor = self.db_conn.execute("SELECT input_path FROM processed WHERE status = 'done'")
            orphan_count = 0
            for row in cursor:
                if not Path(row[0]).exists():
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
            confirmed_h264 = []  # We can't confirm codec without triggering download

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
                            capture_output=True, text=True, timeout=5,
                            creationflags=subprocess.CREATE_NO_WINDOW if hasattr(subprocess, 'CREATE_NO_WINDOW') else 0
                        )
                        # attrib output format: "A  U        C:\path\file.mp4"
                        # U = Unpinned (online-only), P = Pinned (local)
                        attrs = result.stdout.strip()[:20] if result.stdout else ""
                        if 'U' in attrs and 'P' not in attrs:
                            cloud_pending.append((f, size))
                        else:
                            local_pending.append((f, size))
                    except:
                        # If attrib fails, assume local based on size
                        local_pending.append((f, size))
                except:
                    cloud_pending.append((f, size))

            # Calculate sizes
            total_pending_size = sum(s for _, s in pending_videos)
            local_pending_size = sum(s for _, s in local_pending)
            cloud_pending_size = sum(s for _, s in cloud_pending)
            confirmed_h264_size = sum(s for _, s in confirmed_h264)
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
        """Add message to log."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.insert(tk.END, f"[{timestamp}] {message}\n", tag)
        self.log_text.see(tk.END)

    def setup_database(self):
        """Initialize database."""
        db_path = Path(r"C:\transcoder\transcoder_gui.db")
        db_path.parent.mkdir(parents=True, exist_ok=True)

        self.db_conn = sqlite3.connect(str(db_path), check_same_thread=False)
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

    def load_stats(self):
        """Load stats from database."""
        cursor = self.db_conn.execute(
            "SELECT COUNT(*), SUM(input_size - output_size) FROM processed WHERE status = 'done'"
        )
        row = cursor.fetchone()
        self.files_processed.set(row[0] or 0)
        saved = (row[1] or 0) / (1024**3)
        self.total_saved_gb.set(round(saved, 2))

    def is_processed(self, path: Path) -> bool:
        """Check if file was already processed."""
        # First check database
        cursor = self.db_conn.execute(
            "SELECT status FROM processed WHERE input_path = ?", (str(path),)
        )
        row = cursor.fetchone()
        if row is not None and row[0] in ('done', 'skipped_hevc', 'skipped_exists'):
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
                except:
                    pass

            return False
        except:
            return False

    def mark_processed(self, input_path: Path, output_path: str, status: str,
                      input_size: int = 0, output_size: int = 0):
        """Mark file as processed."""
        self.db_conn.execute("""
            INSERT OR REPLACE INTO processed
            (input_path, output_path, status, input_size, output_size, processed_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (str(input_path), output_path, status, input_size, output_size,
              datetime.now().isoformat()))
        self.db_conn.commit()
        self.load_stats()

    def reset_failed(self):
        """Reset failed files so they can be retried."""
        cursor = self.db_conn.execute("SELECT COUNT(*) FROM processed WHERE status = 'error'")
        count = cursor.fetchone()[0]
        if count == 0:
            messagebox.showinfo("Info", "No failed files to reset.")
            return
        if messagebox.askyesno("Confirm", f"Reset {count} failed files for retry?"):
            self.db_conn.execute("DELETE FROM processed WHERE status = 'error'")
            self.db_conn.commit()
            self.log(f"Reset {count} failed files for retry", "success")

    def clear_history(self):
        """Clear processing history."""
        if messagebox.askyesno("Confirm", "Clear all processing history?"):
            self.db_conn.execute("DELETE FROM processed")
            self.db_conn.commit()
            self.load_stats()
            self.log("History cleared", "warning")

    def start_wav_conversion(self):
        """Start WAV→MP3 conversion for files in 'Audio Source Files' folders."""
        if self.running:
            messagebox.showwarning("Em execução", "Pare o processo atual antes de iniciar conversão WAV.")
            return

        folder = Path(self.watch_folder.get())
        if not folder.exists():
            messagebox.showerror("Error", "Watch folder not found!")
            return

        self.log("Starting WAV→MP3 conversion...", "info")
        self.running = True
        self.wav_running = True
        self.start_btn.config(state=tk.DISABLED)
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
                self.running = False
                self.wav_running = False
                self.root.after(0, lambda: self.start_btn.config(state=tk.NORMAL))
                self.root.after(0, lambda: self.stop_wav_btn.config(state=tk.DISABLED))
                self.root.after(0, lambda: self.current_file_label.config(text="Idle"))
                self.notify_queue_finished()

        threading.Thread(target=wav_worker, daemon=True).start()

    def stop_wav_conversion(self):
        """Stop WAV→MP3 conversion immediately."""
        self.running = False
        self.wav_running = False

        # Kill FFmpeg process if running
        if self.current_process:
            try:
                self.current_process.terminate()
                self.log("FFmpeg process terminated", "warning")
            except:
                pass

        # Reset UI
        self.start_btn.config(state=tk.NORMAL)
        self.stop_wav_btn.config(state=tk.DISABLED)
        self.current_file_label.config(text="Idle")
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
                # Skip wav backup folder
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

                # If file is very small, it's probably online-only
                if size < 1000:
                    cloud_files += 1
                    continue

                # Try to read 1 byte from the file
                try:
                    with open(wav_path, 'rb') as f:
                        f.read(1)
                    already_local += 1
                except OSError as e:
                    if e.errno == 22:  # Invalid argument - cloud file
                        cloud_files += 1
                        if triggered_size + size <= available_for_download:
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

        # Report results
        triggered_gb = triggered_size / (1024**3)
        msg = f"WAV Scan: {already_local} local, {triggered} downloading ({triggered_gb:.1f}GB)"
        if skipped_space > 0:
            msg += f", {skipped_space} skipped (no space)"
        self.root.after(0, lambda m=msg: self.log(m, "success"))

    def toggle_processing(self):
        """Start processing."""
        if not self.running:
            self.running = True
            self.paused = False
            self.start_btn.config(state=tk.DISABLED)
            self.pause_btn.config(state=tk.NORMAL, text="⏸ PAUSE")
            self.stop_btn.config(state=tk.NORMAL)
            self.progress_var.set(0)
            self.worker_thread = threading.Thread(target=self.process_loop, daemon=True)
            self.worker_thread.start()
            self.log("Started monitoring", "success")

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
        """Stop all encoding immediately."""
        self.running = False
        self.paused = False

        # Kill FFmpeg process if running
        if self.current_process:
            try:
                self.current_process.terminate()
                self.log("FFmpeg process terminated", "warning")
            except:
                pass

        self.start_btn.config(state=tk.NORMAL)
        self.pause_btn.config(state=tk.DISABLED, text="⏸ PAUSE")
        self.stop_btn.config(state=tk.DISABLED)
        self.progress_var.set(0)
        self.current_file_label.config(text="Idle")
        self.progress_label.config(text="")
        self.log("Stopped all encoding", "warning")

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
        folder = Path(self.watch_folder.get())

        if not folder.exists():
            self.root.after(0, lambda: self.log(f"Folder not found: {folder}", "error"))
            return

        self.root.after(0, lambda: self.log(f"Scanning {folder} to trigger downloads..."))

        # Check available disk space - reserve 10GB minimum
        free_gb = self.get_free_disk_space(folder)
        available_for_download = max(0, (free_gb - 10) * 1024**3)  # Convert to bytes, keep 10GB free

        if free_gb < 15:
            self.root.after(0, lambda g=free_gb: self.log(
                f"Low disk space ({g:.1f} GB). Limiting downloads.", "warning"))

        # Find video files (only .mp4, skip ._ metadata files from macOS/ATEM)
        video_files = []
        for ext in ['.mp4', '.MP4']:
            for f in folder.rglob(f'*{ext}'):
                # Skip h265/h264 folders, and macOS/ATEM metadata files starting with ._
                if 'h265' not in str(f).lower() and 'h264' not in str(f).lower() and not f.name.startswith('._'):
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

                # If file is very small, it's probably online-only
                if size < 1000:
                    cloud_files += 1
                    # Don't trigger download for tiny placeholders - we don't know real size
                    continue

                # Try to read 1 byte from the file - this triggers Dropbox download
                try:
                    with open(video_path, 'rb') as f:
                        f.read(1)
                    already_local += 1
                except OSError as e:
                    if e.errno == 22:  # Invalid argument - cloud file
                        cloud_files += 1
                        # Check if we have space for this file
                        if triggered_size + size <= available_for_download:
                            self._trigger_dropbox_download(video_path)
                            triggered += 1
                            triggered_size += size
                        else:
                            skipped_space += 1
                    else:
                        raise

            except PermissionError:
                # File is being synced by Dropbox
                triggered += 1
            except Exception:
                # Silent - don't spam log with errors for each file
                cloud_files += 1

        # Report results
        triggered_gb = triggered_size / (1024**3)
        msg = f"Scan: {already_local} local, {triggered} downloading ({triggered_gb:.1f}GB)"
        if skipped_space > 0:
            msg += f", {skipped_space} skipped (no space)"
        self.root.after(0, lambda m=msg: self.log(m, "success"))

    def process_loop(self):
        """Main processing loop."""
        while self.running:
            # Wait while paused
            while self.paused and self.running:
                time.sleep(0.5)

            if not self.running:
                break

            self.scan_and_process()

            for _ in range(30):  # Wait 30 seconds between scans
                if not self.running:
                    break
                # Also check pause during wait
                while self.paused and self.running:
                    time.sleep(0.5)
                time.sleep(1)

        # Reset UI when stopped
        self.root.after(0, lambda: self.current_file_label.config(text="Idle"))
        self.root.after(0, lambda: self.progress_var.set(0))
        self.root.after(0, lambda: self.start_btn.config(state=tk.NORMAL))
        self.root.after(0, lambda: self.pause_btn.config(state=tk.DISABLED, text="⏸ PAUSE"))
        self.root.after(0, lambda: self.stop_btn.config(state=tk.DISABLED))

    def scan_and_process(self):
        """Scan folder and process files."""
        folder = Path(self.watch_folder.get())

        if not folder.exists():
            self.root.after(0, lambda: self.log(f"Folder not found: {folder}", "error"))
            return

        # Check disk space before starting
        free_gb = self.get_free_disk_space(folder)
        if free_gb < 5:  # Less than 5GB free
            self.root.after(0, lambda g=free_gb: self.log(
                f"Low disk space ({g:.1f} GB free). Waiting...", "warning"))
            return

        self.root.after(0, lambda: self.log(f"Scanning {folder}..."))

        # Find video files (only .mp4, skip ._ metadata files from macOS/ATEM)
        video_files = []
        for ext in ['.mp4', '.MP4']:
            for f in folder.rglob(f'*{ext}'):
                # Skip h265/h264 folders, and macOS/ATEM metadata files starting with ._
                if 'h265' not in str(f).lower() and 'h264' not in str(f).lower() and not f.name.startswith('._'):
                    video_files.append(f)

        # Filter to only unprocessed files and sort by size (smaller first)
        pending_files = []
        for f in video_files:
            if not self.is_processed(f):
                try:
                    size = f.stat().st_size
                    if size / (1024**3) >= self.min_size_gb.get():
                        pending_files.append((f, size))
                except:
                    pass

        # Smart sorting: prioritize folders closer to completion
        # This helps free up space faster (h264 folders are deleted when complete)
        pending_files = self._sort_by_folder_completion(pending_files)

        total_pending = len(pending_files)
        self.root.after(0, lambda t=total_pending, a=len(video_files): self.log(
            f"Found {a} videos, {t} pending"))

        # Track files processed in this scan
        files_processed_this_scan = 0

        for idx, (video_path, file_size) in enumerate(pending_files):
            if not self.running:
                break

            # Check disk space before each file
            free_gb = self.get_free_disk_space(folder)
            if free_gb < 5:
                self.root.after(0, lambda g=free_gb: self.log(
                    f"Low disk space ({g:.1f} GB). Pausing...", "warning"))
                break

            # Update queue counter
            self.root.after(0, lambda i=idx+1, t=total_pending:
                self.current_file_label.config(text=f"Queue: {i}/{t}"))

            self.process_file(video_path, queue_pos=idx+1, queue_total=total_pending)
            files_processed_this_scan += 1

        # Process WAV files in "Audio Source Files" folders
        audio_processed = self.process_audio_files(folder)
        files_processed_this_scan += audio_processed

        # Notify user if we finished processing files and queue is empty
        if files_processed_this_scan > 0:
            self.notify_queue_finished()

    def process_audio_files(self, base_folder: Path) -> int:
        """
        Process WAV files in 'Audio Source Files' folders.
        Converts WAV to MP3 192kbps, verifies, then deletes original.
        Returns number of files processed.
        """
        if not self.running:
            return 0

        # Find WAV files ONLY in "Audio Source Files" folders
        wav_files = []
        for wav_path in base_folder.rglob('*.wav'):
            # Skip macOS metadata files
            if wav_path.name.startswith('._'):
                continue
            # Check if it's in an "Audio Source Files" folder
            if 'Audio Source Files' in str(wav_path):
                if not self.is_processed(wav_path):
                    try:
                        size = wav_path.stat().st_size
                        wav_files.append((wav_path, size))
                    except:
                        pass

        # Also check .WAV extension
        for wav_path in base_folder.rglob('*.WAV'):
            if wav_path.name.startswith('._'):
                continue
            if 'Audio Source Files' in str(wav_path):
                if not self.is_processed(wav_path):
                    try:
                        size = wav_path.stat().st_size
                        wav_files.append((wav_path, size))
                    except:
                        pass

        if not wav_files:
            return 0

        # Sort by size (smaller first)
        wav_files.sort(key=lambda x: x[1])

        self.root.after(0, lambda n=len(wav_files): self.log(
            f"Found {n} WAV files in Audio Source Files folders", "info"))

        processed = 0
        for wav_path, size in wav_files:
            if not self.running:
                break

            # Check disk space
            free_gb = self.get_free_disk_space(base_folder)
            if free_gb < 2:  # WAV to MP3 needs less space
                self.root.after(0, lambda: self.log(
                    "Low disk space, pausing audio conversion", "warning"))
                break

            # Wait for file to be ready (downloaded from Dropbox)
            if not self.wait_for_file_ready(wav_path):
                continue

            self.root.after(0, lambda p=wav_path.name: self.log(
                f"Converting WAV: {p}", "info"))

            # Convert WAV to MP3
            if self.convert_wav_to_mp3(wav_path):
                processed += 1

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

            self.root.after(0, lambda: self.current_file_label.config(
                text=f"Converting: {wav_path.name}"))

            # Run FFmpeg
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=300  # 5 min timeout
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
            if not self._verify_mp3(temp_mp3):
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

            # Schedule deletion in background (don't block processing)
            self._schedule_wav_deletion(wav_backup_path, mp3_path)

            # Mark as processed
            self.mark_processed(wav_path, str(mp3_path), "done", input_size, output_size)

            # Reset UI
            self.root.after(0, lambda: self.current_file_label.config(text="Idle"))

            return True

        except Exception as e:
            self.root.after(0, lambda err=e: self.log(
                f"Error converting {wav_path.name}: {err}", "error"))
            return False

    def _schedule_wav_deletion(self, wav_path: Path, mp3_path: Path):
        """
        Schedule WAV FOLDER deletion after 30 seconds (in background thread).
        Only deletes when ALL files in the folder have valid MP3 versions.
        """
        wav_folder = wav_path.parent

        # Track folders already scheduled to avoid duplicates
        if not hasattr(self, '_scheduled_wav_folders'):
            self._scheduled_wav_folders = set()

        # Skip if this folder is already scheduled
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

                # Get all WAV files in wav folder
                wav_files = list(wav_folder.glob('*.wav')) + list(wav_folder.glob('*.WAV'))

                if not wav_files:
                    self._scheduled_wav_folders.discard(folder_key)
                    return

                # Verify ALL WAV files have valid MP3 counterparts
                all_verified = True
                for wav_file in wav_files:
                    mp3_file = parent_folder / wav_file.with_suffix('.mp3').name

                    # Check MP3 exists
                    if not mp3_file.exists():
                        self.root.after(0, lambda f=wav_file.name: self.log(
                            f"MP3 not found for {f}, keeping wav folder", "warning"))
                        all_verified = False
                        break

                    # Verify MP3 is valid
                    if not self._verify_mp3(mp3_file):
                        self.root.after(0, lambda f=wav_file.name: self.log(
                            f"MP3 verification failed for {f}, keeping wav folder", "warning"))
                        all_verified = False
                        break

                    # Check file size is reasonable (MP3 should be at least 1KB)
                    if mp3_file.stat().st_size < 1000:
                        self.root.after(0, lambda f=wav_file.name: self.log(
                            f"MP3 too small for {f}, keeping wav folder", "warning"))
                        all_verified = False
                        break

                # All checks passed - delete entire wav folder
                if all_verified:
                    # Calculate stats before deleting
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
                    except:
                        pass

                self._scheduled_wav_folders.discard(folder_key)

            except Exception as e:
                self.root.after(0, lambda err=e: self.log(
                    f"Could not delete WAV folder: {err}", "warning"))
                self._scheduled_wav_folders.discard(folder_key)

        # Run in background thread
        threading.Thread(target=delete_folder_after_delay, daemon=True).start()

    def _schedule_h264_deletion(self, h264_path: Path, h265_path: Path):
        """
        Schedule h264 FOLDER deletion after 20 minutes (in background thread).
        Only deletes when ALL files in the folder have valid h265 versions.
        """
        h264_folder = h264_path.parent

        # Track folders already scheduled to avoid duplicates
        if not hasattr(self, '_scheduled_h264_folders'):
            self._scheduled_h264_folders = set()

        # Skip if this folder is already scheduled
        folder_key = str(h264_folder)
        if folder_key in self._scheduled_h264_folders:
            return

        self._scheduled_h264_folders.add(folder_key)
        parent_folder = h264_folder.parent  # Where h265 files should be

        def delete_folder_after_delay():
            # Wait 20 minutes for Dropbox to sync
            time.sleep(20 * 60)  # 20 minutes

            try:
                if not h264_folder.exists():
                    self._scheduled_h264_folders.discard(folder_key)
                    return

                # Get all video files in h264 folder
                h264_files = list(h264_folder.glob('*.mp4')) + list(h264_folder.glob('*.MP4'))

                if not h264_files:
                    self._scheduled_h264_folders.discard(folder_key)
                    return

                # Verify ALL h264 files have valid h265 counterparts
                all_verified = True
                for h264_file in h264_files:
                    h265_file = parent_folder / h264_file.name

                    # Check h265 exists
                    if not h265_file.exists():
                        self.root.after(0, lambda f=h264_file.name: self.log(
                            f"H265 not found for {f}, keeping h264 folder", "warning"))
                        all_verified = False
                        break

                    # Verify h265 is playable
                    if not self._verify_output(h265_file):
                        self.root.after(0, lambda f=h264_file.name: self.log(
                            f"H265 verification failed for {f}, keeping h264 folder", "warning"))
                        all_verified = False
                        break

                    # Check file size is reasonable
                    if h265_file.stat().st_size < 10000:
                        self.root.after(0, lambda f=h264_file.name: self.log(
                            f"H265 too small for {f}, keeping h264 folder", "warning"))
                        all_verified = False
                        break

                # All checks passed - delete entire h264 folder
                if all_verified:
                    import shutil
                    shutil.rmtree(h264_folder)
                    self.root.after(0, lambda p=h264_folder: self.log(
                        f"H264 folder deleted (all verified): {p.name}", "success"))

                    # Log deletion timestamp to h265 feito.txt
                    h265_folder = parent_folder / 'h265'
                    h265_folder.mkdir(parents=True, exist_ok=True)
                    log_file = h265_folder / "h265 feito.txt"
                    try:
                        with open(log_file, 'a', encoding='utf-8') as f:
                            f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | H264 FOLDER DELETED\n")
                    except:
                        pass

                self._scheduled_h264_folders.discard(folder_key)

            except Exception as e:
                self.root.after(0, lambda err=e: self.log(
                    f"Could not delete h264 folder: {err}", "warning"))
                self._scheduled_h264_folders.discard(folder_key)

        # Run in background thread
        self.root.after(0, lambda: self.log(
            f"H264 folder deletion scheduled for 20 min: {h264_folder}", "info"))
        threading.Thread(target=delete_folder_after_delay, daemon=True).start()

    def _verify_mp3(self, mp3_path: Path) -> bool:
        """Verify MP3 file is valid using ffprobe."""
        try:
            if not mp3_path.exists():
                return False
            if mp3_path.stat().st_size < 1000:  # Less than 1KB
                return False

            result = subprocess.run(
                ['ffprobe', '-v', 'error', '-select_streams', 'a:0',
                 '-show_entries', 'stream=codec_name', '-of', 'csv=p=0',
                 str(mp3_path)],
                capture_output=True, text=True, timeout=30
            )

            codec = result.stdout.strip().lower()
            return codec == 'mp3'

        except Exception:
            return False

    def get_free_disk_space(self, path: Path) -> float:
        """Get free disk space in GB for the drive containing path."""
        try:
            import ctypes
            free_bytes = ctypes.c_ulonglong(0)
            ctypes.windll.kernel32.GetDiskFreeSpaceExW(
                str(path), None, None, ctypes.pointer(free_bytes)
            )
            return free_bytes.value / (1024**3)
        except:
            return 999  # Assume enough space if can't check

    def _sort_by_folder_completion(self, pending_files: list) -> list:
        """
        Sort files prioritizing folders that are closer to completion.
        This helps free up h264 backup folders faster (they're deleted when all files are done).

        Strategy:
        1. Count pending files per folder
        2. Sort by (folder_pending_count, file_size)
        3. Folders with fewer pending files are processed first
        """
        if not pending_files:
            return pending_files

        from collections import defaultdict

        # Group by parent folder and count pending
        folder_pending_count = defaultdict(int)
        for f, size in pending_files:
            folder = f.parent
            folder_pending_count[folder] += 1

        # Log folder analysis
        folders_info = [(folder, count) for folder, count in folder_pending_count.items()]
        folders_info.sort(key=lambda x: x[1])  # Sort by count for logging

        if len(folders_info) > 1:
            almost_done = [f"{f.name}({c})" for f, c in folders_info[:3] if c <= 5]
            if almost_done:
                self.root.after(0, lambda a=almost_done: self.log(
                    f"Priority folders (almost done): {', '.join(a)}", "info"))

        # Sort files by (folder_pending_count, file_size)
        # Folders with fewer pending files come first, then smaller files
        sorted_files = sorted(
            pending_files,
            key=lambda x: (folder_pending_count[x[0].parent], x[1])
        )

        return sorted_files

    def wait_for_file_ready(self, file_path: Path, timeout_minutes: int = 60) -> bool:
        """
        Check if a file is ready (fully downloaded from Dropbox).
        If file is online-only, triggers download and returns False immediately
        so the program can continue with other files.
        Returns True if file is ready, False if not ready yet.
        """
        try:
            # First check if file exists
            if not file_path.exists():
                self.root.after(0, lambda: self.log(
                    f"File not found, skipping", "warning"))
                return False

            # Get file size - this works even for online-only files
            current_size = file_path.stat().st_size

            # If file is very small, it's probably a placeholder (online-only)
            if current_size < 10000:  # Less than 10KB
                self.root.after(0, lambda: self.log(
                    f"Online-only file, triggering download and moving to next...", "info"))
                self._trigger_dropbox_download(file_path)
                return False  # Skip for now, will retry next scan

            # Try to read some bytes to verify file is accessible
            try:
                with open(file_path, 'rb') as f:
                    f.read(1024)  # Read 1KB
            except OSError as e:
                if e.errno == 22:  # Invalid argument - online-only file
                    self.root.after(0, lambda: self.log(
                        f"Cloud file detected, triggering download and moving to next...", "info"))
                    self._trigger_dropbox_download(file_path)
                    return False  # Skip for now, will retry next scan
                raise

            # Check if size is stable (file finished downloading)
            time.sleep(2)
            new_size = file_path.stat().st_size

            if current_size == new_size and current_size > 10000:
                # File is stable and ready
                return True
            else:
                # Still downloading - skip and come back later
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
            self.root.after(0, lambda err=e: self.log(
                f"Cannot access file: {err} - skipping", "warning"))
            return False

    def _trigger_dropbox_download(self, file_path: Path):
        """
        Try to trigger Dropbox to download a cloud-only file.
        Uses multiple methods since different Dropbox versions behave differently.
        """
        try:
            # Method 1: Use attrib to remove Unpinned attribute (request download)
            subprocess.run(
                ['attrib', '-U', '+P', str(file_path)],
                capture_output=True, timeout=10
            )
        except:
            pass

        try:
            # Method 2: Use PowerShell to access the file (triggers download)
            subprocess.run(
                ['powershell', '-Command', f'Get-Content -Path "{file_path}" -TotalCount 1 -ErrorAction SilentlyContinue'],
                capture_output=True, timeout=30
            )
        except:
            pass

    def process_file(self, input_path: Path, queue_pos: int = 0, queue_total: int = 0):
        """Process a single file."""
        queue_str = f"[{queue_pos}/{queue_total}] " if queue_pos else ""
        self.root.after(0, lambda q=queue_str: self.current_file_label.config(
            text=f"{q}Processing: {input_path.name}"))
        self.root.after(0, lambda q=queue_str: self.log(f"{q}Processing: {input_path.name}"))

        # Wait for file to be fully downloaded from Dropbox
        if not self.wait_for_file_ready(input_path):
            return

        size_gb = input_path.stat().st_size / (1024**3)
        self.root.after(0, lambda: self.current_file_label.config(
            text=f"Processing: {input_path.name} ({size_gb:.2f} GB)"))

        # Probe video
        probe_data = self.probe_video(input_path)
        if not probe_data:
            self.root.after(0, lambda: self.log("Could not probe video", "error"))
            self.mark_processed(input_path, "", "error", 0, 0)
            return

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

        # Transcode
        output_folder.mkdir(parents=True, exist_ok=True)
        temp_path = output_path.with_suffix(output_path.suffix + '.tmp')

        # Get video duration for progress calculation
        duration = self.get_duration(probe_data)

        # Try encoding with current encoder, fallback to CPU if fails
        encoder = self.encoder.get()
        encoders_to_try = [encoder]
        if encoder != 'cpu':
            encoders_to_try.append('cpu')  # Add CPU as fallback

        encoding_success = False
        for try_encoder in encoders_to_try:
            if not self.running:
                break

            self.root.after(0, lambda e=try_encoder: self.log(f"Encoding with {e}..."))
            cmd = self.build_ffmpeg_command(input_path, temp_path, encoder=try_encoder, probe_data=probe_data)

            # Reset progress bar
            self.root.after(0, lambda: self.progress_var.set(0))

            success, error_msg = self._run_ffmpeg(cmd, duration)

            if success and temp_path.exists():
                # Verify output file is valid
                if self._verify_output(temp_path):
                    encoding_success = True
                    break
                else:
                    self.root.after(0, lambda: self.log(
                        f"Output verification failed with {try_encoder}", "warning"))
                    if temp_path.exists():
                        temp_path.unlink()
            else:
                # Log actual FFmpeg error for debugging
                if error_msg:
                    # Get last line of error (most relevant)
                    last_err = error_msg.split('\n')[-1] if error_msg else "Unknown"
                    self.root.after(0, lambda e=try_encoder, err=last_err: self.log(
                        f"Encoding failed with {e}: {err[:100]}", "warning"))
                else:
                    self.root.after(0, lambda e=try_encoder: self.log(
                        f"Encoding failed with {e}", "warning"))
                if try_encoder != encoders_to_try[-1]:
                    self.root.after(0, lambda: self.log("Trying fallback encoder...", "info"))
                if temp_path.exists():
                    temp_path.unlink()

        if encoding_success:
            temp_path.rename(output_path)
            input_size = input_path.stat().st_size
            output_size = output_path.stat().st_size
            reduction = (1 - output_size/input_size) * 100

            self.root.after(0, lambda r=reduction: self.log(
                f"Done! {r:.1f}% smaller", "success"))

            # Reorganize files:
            # 1. Move original H.264 to h264/ folder
            # 2. Move H.265 from h265/ to original location
            # Using shutil.move() with retry to handle file locks (FFmpeg/Dropbox)
            try:
                h264_folder = input_path.parent / 'h264'
                h264_folder.mkdir(parents=True, exist_ok=True)
                h264_backup_path = h264_folder / input_path.name

                # Move original to h264/ with retry
                self._move_with_retry(input_path, h264_backup_path)
                self.root.after(0, lambda: self.log(
                    f"Moved original to h264/{input_path.name}", "info"))

                # Mark h264 backup as online-only to free up local space
                self.set_dropbox_online_only(h264_backup_path)

                # Move h265 output to original location with retry
                final_path = input_path  # Same name/location as original
                self._move_with_retry(output_path, final_path)
                self.root.after(0, lambda: self.log(
                    f"Moved H.265 to original location", "info"))

                # Update output_path for logging
                self.mark_processed(h264_backup_path, str(final_path), "done", input_size, output_size)
                self.write_success_log(h264_backup_path, final_path, input_size, output_size)
                self.write_h265_done_log(output_folder, input_path.name, input_size, output_size)

                # Schedule h264 backup deletion if enabled
                if self.auto_delete_h264.get():
                    self._schedule_h264_deletion(h264_backup_path, final_path)

            except Exception as move_err:
                self.root.after(0, lambda e=move_err: self.log(
                    f"File reorganization failed: {e}", "error"))
                # Still mark as done since encoding succeeded
                self.mark_processed(input_path, str(output_path), "done", input_size, output_size)
                self.write_success_log(input_path, output_path, input_size, output_size)
        else:
            self.root.after(0, lambda: self.log("All encoders failed!", "error"))
            self.mark_processed(input_path, "", "error", 0, 0)

        # Cleanup
        self.current_process = None
        self.root.after(0, lambda: self.progress_var.set(0))
        self.root.after(0, lambda: self.progress_label.config(text=""))
        self.root.after(0, lambda: self.current_file_label.config(text="Idle"))

    def probe_video(self, path: Path) -> dict:
        """Get video info."""
        try:
            cmd = ['ffprobe', '-v', 'quiet', '-print_format', 'json',
                   '-show_format', '-show_streams', str(path)]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode == 0:
                return json.loads(result.stdout)
        except:
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
        except:
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
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
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

    def _verify_output(self, output_path: Path) -> bool:
        """
        Verify output file is a valid video using ffprobe.
        Returns True if file is valid and playable.
        """
        try:
            # Check file exists and has reasonable size
            if not output_path.exists():
                return False
            if output_path.stat().st_size < 1000:  # Less than 1KB
                return False

            # Use ffprobe to check if file is valid
            result = subprocess.run(
                ['ffprobe', '-v', 'error', '-select_streams', 'v:0',
                 '-show_entries', 'stream=codec_name', '-of', 'csv=p=0',
                 str(output_path)],
                capture_output=True, text=True, timeout=30
            )

            # Should return 'hevc' or 'h265' for valid output
            codec = result.stdout.strip().lower()
            if codec in ('hevc', 'h265'):
                self.root.after(0, lambda: self.log("Output verified: valid HEVC", "info"))
                return True
            else:
                self.root.after(0, lambda c=codec: self.log(
                    f"Verification failed: codec={c}", "warning"))
                return False

        except Exception as e:
            self.root.after(0, lambda err=e: self.log(
                f"Verification error: {err}", "warning"))
            return False

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
        except:
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
    except:
        pass

    app = TranscoderGUI(root)
    root.mainloop()


if __name__ == '__main__':
    main()
