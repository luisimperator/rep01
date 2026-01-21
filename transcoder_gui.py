#!/usr/bin/env python3
"""
Dropbox Video Transcoder - GUI Version

Simple graphical interface for local folder transcoding.
"""

import subprocess
import sys
import time
import json
import re
import sqlite3
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
        self.root.title("HeavyDrops Dropbox Video Transcoder H.264 → H.265")
        self.root.geometry("900x700")
        self.root.minsize(800, 600)

        # State
        self.running = False
        self.paused = False
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
        ttk.Button(control_frame, text="📁 Open Folder", command=self.open_folder).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(control_frame, text="🔄 Reset Failed", command=self.reset_failed).pack(side=tk.LEFT, padx=(0, 10))
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
                'min_size_gb': self.min_size_gb.get()
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

    def write_success_log(self, input_path: Path, output_path: Path, input_size: int, output_size: int):
        """Write successful encoding to log file."""
        try:
            log_folder = Path(self.log_folder.get())
            log_folder.mkdir(parents=True, exist_ok=True)
            log_file = log_folder / "encoding_history.log"

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

    def open_folder(self):
        """Open watch folder in explorer."""
        folder = Path(self.watch_folder.get())
        if folder.exists():
            subprocess.Popen(f'explorer "{folder}"')
        else:
            messagebox.showerror("Error", "Folder does not exist")

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
        cursor = self.db_conn.execute(
            "SELECT status FROM processed WHERE input_path = ?", (str(path),)
        )
        row = cursor.fetchone()
        return row is not None and row[0] in ('done', 'skipped_hevc', 'skipped_exists')

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

        # Find video files
        video_files = []
        for ext in ['.mp4', '.mov', '.MP4', '.MOV', '.mkv', '.MKV', '.avi', '.AVI']:
            for f in folder.rglob(f'*{ext}'):
                if 'h265' not in str(f).lower() and 'h264' not in str(f).lower():
                    video_files.append(f)

        self.root.after(0, lambda: self.log(f"Found {len(video_files)} video files"))

        triggered = 0
        already_local = 0
        cloud_files = 0

        for video_path in video_files:
            try:
                # Check file size first
                size = video_path.stat().st_size

                # If file is very small, it's probably online-only
                if size < 1000:
                    cloud_files += 1
                    self._trigger_dropbox_download(video_path)
                    continue

                # Try to read 1 byte from the file - this triggers Dropbox download
                try:
                    with open(video_path, 'rb') as f:
                        f.read(1)
                    already_local += 1
                except OSError as e:
                    if e.errno == 22:  # Invalid argument - cloud file
                        cloud_files += 1
                        self._trigger_dropbox_download(video_path)
                    else:
                        raise

            except PermissionError:
                # File is being synced by Dropbox
                triggered += 1
            except Exception:
                # Silent - don't spam log with errors for each file
                cloud_files += 1

        self.root.after(0, lambda: self.log(
            f"Scan complete: {already_local} local, {cloud_files} cloud (downloading), {triggered} syncing", "success"))

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

        self.root.after(0, lambda: self.log(f"Scanning {folder}..."))

        # Find video files
        video_files = []
        for ext in ['.mp4', '.mov', '.MP4', '.MOV']:
            for f in folder.rglob(f'*{ext}'):
                if 'h265' not in str(f).lower() and 'h264' not in str(f).lower():
                    video_files.append(f)

        self.root.after(0, lambda: self.log(f"Found {len(video_files)} video files"))

        # Track files processed in this scan
        files_processed_this_scan = 0

        for video_path in video_files:
            if not self.running and self.worker_thread:
                break

            if self.is_processed(video_path):
                continue

            size_gb = video_path.stat().st_size / (1024**3)
            if size_gb < self.min_size_gb.get():
                self.mark_processed(video_path, "", "skipped_small", video_path.stat().st_size, 0)
                continue

            self.process_file(video_path)
            files_processed_this_scan += 1

        # Notify user if we finished processing files and queue is empty
        if files_processed_this_scan > 0:
            self.notify_queue_finished()

    def wait_for_file_ready(self, file_path: Path, timeout_minutes: int = 60) -> bool:
        """
        Wait for a file to be fully downloaded from Dropbox.
        Returns True if file is ready, False if timeout or stopped.
        """
        max_wait = timeout_minutes * 60  # Convert to seconds
        waited = 0
        check_interval = 10  # Check every 10 seconds
        download_triggered = False

        while waited < max_wait and self.running:
            try:
                # First check if file exists and get its size
                if not file_path.exists():
                    self.root.after(0, lambda: self.log(
                        f"File not found, skipping", "warning"))
                    return False

                # Get file size - this works even for online-only files
                current_size = file_path.stat().st_size

                # If file is very small, it's probably a placeholder
                if current_size < 10000:  # Less than 10KB
                    if not download_triggered:
                        self.root.after(0, lambda: self.log(
                            f"Online-only file detected, triggering download...", "info"))
                        self._trigger_dropbox_download(file_path)
                        download_triggered = True
                    self.root.after(0, lambda w=waited: self.log(
                        f"Waiting for Dropbox download... ({w}s)", "info"))
                    time.sleep(check_interval)
                    waited += check_interval
                    continue

                # Try to read some bytes to verify file is accessible
                try:
                    with open(file_path, 'rb') as f:
                        f.read(1024)  # Read 1KB
                except OSError as e:
                    if e.errno == 22:  # Invalid argument - online-only file
                        if not download_triggered:
                            self.root.after(0, lambda: self.log(
                                f"Cloud file detected, triggering download...", "info"))
                            self._trigger_dropbox_download(file_path)
                            download_triggered = True
                        self.root.after(0, lambda w=waited: self.log(
                            f"Waiting for cloud file... ({w}s)", "info"))
                        time.sleep(check_interval)
                        waited += check_interval
                        continue
                    raise

                # Wait a bit and check if size is stable (file finished downloading)
                time.sleep(3)
                new_size = file_path.stat().st_size

                if current_size == new_size and current_size > 10000:
                    # File is stable and ready
                    return True
                else:
                    # Still downloading
                    progress_mb = new_size / (1024**2)
                    self.root.after(0, lambda p=progress_mb: self.log(
                        f"Downloading from Dropbox... {p:.1f} MB", "info"))
                    time.sleep(check_interval)
                    waited += check_interval

            except PermissionError:
                # File is being used by Dropbox - wait
                self.root.after(0, lambda w=waited: self.log(
                    f"File locked by Dropbox, waiting... ({w}s)", "info"))
                time.sleep(check_interval)
                waited += check_interval

            except Exception as e:
                # For other errors, skip this file for now
                self.root.after(0, lambda err=e: self.log(
                    f"Cannot access file: {err} - skipping for now", "warning"))
                return False

        if waited >= max_wait:
            self.root.after(0, lambda: self.log(
                f"Timeout waiting for file download ({timeout_minutes} min)", "error"))
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

    def process_file(self, input_path: Path):
        """Process a single file."""
        self.root.after(0, lambda: self.current_file_label.config(
            text=f"Processing: {input_path.name}"))
        self.root.after(0, lambda: self.log(f"Processing: {input_path.name}"))

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

        cmd = self.build_ffmpeg_command(input_path, temp_path)
        self.root.after(0, lambda: self.log(f"Encoding with {self.encoder.get()}..."))

        # Get video duration for progress calculation
        duration = self.get_duration(probe_data)

        # Reset progress bar
        self.root.after(0, lambda: self.progress_var.set(0))

        try:
            # Log the command for debugging
            self.root.after(0, lambda: self.log(f"CMD: {' '.join(cmd[:6])}...", "info"))

            self.current_process = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
            )
            process = self.current_process

            last_lines = []  # Keep last few lines for error reporting
            for line in process.stdout:
                last_lines.append(line.strip())
                if len(last_lines) > 10:
                    last_lines.pop(0)
                if 'time=' in line:
                    # Parse time and calculate progress
                    current_time = self.parse_ffmpeg_time(line)
                    if duration > 0 and current_time >= 0:
                        progress = min(100, (current_time / duration) * 100)
                        self.root.after(0, lambda p=progress: self.progress_var.set(p))
                        self.root.after(0, lambda p=progress: self.progress_label.config(
                            text=f"{p:.1f}% - {line.strip()[-60:]}"))
                    else:
                        self.root.after(0, lambda l=line: self.progress_label.config(text=l.strip()[-80:]))

            process.wait()

            if process.returncode == 0 and temp_path.exists():
                temp_path.rename(output_path)
                input_size = input_path.stat().st_size
                output_size = output_path.stat().st_size
                reduction = (1 - output_size/input_size) * 100

                self.root.after(0, lambda: self.log(
                    f"Done! {reduction:.1f}% smaller", "success"))

                # Reorganize files:
                # 1. Move original H.264 to h264/ folder
                # 2. Move H.265 from h265/ to original location
                try:
                    h264_folder = input_path.parent / 'h264'
                    h264_folder.mkdir(parents=True, exist_ok=True)
                    h264_backup_path = h264_folder / input_path.name

                    # Move original to h264/
                    input_path.rename(h264_backup_path)
                    self.root.after(0, lambda: self.log(
                        f"Moved original to h264/{input_path.name}", "info"))

                    # Mark h264 backup as online-only to free up local space
                    self.set_dropbox_online_only(h264_backup_path)

                    # Move h265 output to original location
                    final_path = input_path  # Same name/location as original
                    output_path.rename(final_path)
                    self.root.after(0, lambda: self.log(
                        f"Moved H.265 to original location", "info"))

                    # Update output_path for logging
                    self.mark_processed(h264_backup_path, str(final_path), "done", input_size, output_size)
                    self.write_success_log(h264_backup_path, final_path, input_size, output_size)

                except Exception as move_err:
                    self.root.after(0, lambda e=move_err: self.log(
                        f"File reorganization failed: {e}", "error"))
                    # Still mark as done since encoding succeeded
                    self.mark_processed(input_path, str(output_path), "done", input_size, output_size)
                    self.write_success_log(input_path, output_path, input_size, output_size)
            else:
                # Show the actual error from FFmpeg
                error_msg = "\n".join(last_lines[-5:]) if last_lines else "Unknown error"
                self.root.after(0, lambda: self.log(f"Encoding failed (code {process.returncode})", "error"))
                self.root.after(0, lambda e=error_msg: self.log(f"FFmpeg output: {e}", "error"))
                self.mark_processed(input_path, "", "error", 0, 0)
                if temp_path.exists():
                    temp_path.unlink()

        except Exception as e:
            self.root.after(0, lambda: self.log(f"Error: {e}", "error"))
            import traceback
            self.root.after(0, lambda: self.log(f"Traceback: {traceback.format_exc()}", "error"))
            self.mark_processed(input_path, "", "error", 0, 0)

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

    def build_ffmpeg_command(self, input_path: Path, output_path: Path) -> list:
        """Build FFmpeg command."""
        encoder = self.encoder.get()
        cq = self.cq_value.get()

        # -map 0:v = video streams, -map 0:a? = audio (optional, ? means don't fail if no audio)
        # This avoids copying timecode/data tracks that can't go in MP4
        # -f mp4 explicitly sets format (needed for .tmp extension)
        if encoder == 'nvenc':
            return [
                'ffmpeg', '-hide_banner', '-y', '-i', str(input_path),
                '-map', '0:v', '-map', '0:a?', '-map_metadata', '0',
                '-c:v', 'hevc_nvenc', '-preset', 'p5', '-rc:v', 'vbr', '-cq:v', str(cq),
                '-c:a', 'copy', '-f', 'mp4', str(output_path)
            ]
        elif encoder == 'qsv':
            return [
                'ffmpeg', '-hide_banner', '-y', '-hwaccel', 'qsv', '-i', str(input_path),
                '-map', '0:v', '-map', '0:a?', '-map_metadata', '0',
                '-c:v', 'hevc_qsv', '-preset', 'medium', '-global_quality:v', str(cq),
                '-c:a', 'copy', '-f', 'mp4', str(output_path)
            ]
        else:  # cpu
            return [
                'ffmpeg', '-hide_banner', '-y', '-i', str(input_path),
                '-map', '0:v', '-map', '0:a?', '-map_metadata', '0',
                '-c:v', 'libx265', '-preset', 'medium', '-crf', str(cq),
                '-c:a', 'copy', '-f', 'mp4', str(output_path)
            ]


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
