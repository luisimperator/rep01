#!/usr/bin/env python3
"""
Dropbox Video Transcoder - GUI Version

Simple graphical interface for local folder transcoding.
"""

import subprocess
import sys
import time
import json
import sqlite3
import threading
from pathlib import Path
from datetime import datetime
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext


class TranscoderGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Dropbox Video Transcoder - H.264 → H.265")
        self.root.geometry("900x700")
        self.root.minsize(800, 600)

        # State
        self.running = False
        self.worker_thread = None
        self.current_file = None
        self.db_conn = None

        # Default settings
        self.watch_folder = tk.StringVar(value=r"D:\HeavyDrops Dropbox\HeavyDrops\App h265 Converter")
        self.min_size_gb = tk.DoubleVar(value=0)
        self.encoder = tk.StringVar(value="cpu")
        self.cq_value = tk.IntVar(value=24)

        # Stats
        self.files_processed = tk.IntVar(value=0)
        self.total_saved_gb = tk.DoubleVar(value=0)

        self.setup_ui()
        self.setup_database()
        self.load_stats()
        self.check_ffmpeg()

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

        settings_frame.columnconfigure(1, weight=1)

        # === CONTROL FRAME ===
        control_frame = ttk.Frame(main_frame)
        control_frame.pack(fill=tk.X, pady=(0, 10))

        self.start_btn = ttk.Button(control_frame, text="▶ START", command=self.toggle_processing, style="Accent.TButton")
        self.start_btn.pack(side=tk.LEFT, padx=(0, 10))

        self.scan_btn = ttk.Button(control_frame, text="🔍 Scan Now", command=self.scan_once)
        self.scan_btn.pack(side=tk.LEFT, padx=(0, 10))

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

        self.progress_bar = ttk.Progressbar(progress_frame, mode='indeterminate')
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
        """Start or stop processing."""
        if self.running:
            self.running = False
            self.start_btn.config(text="▶ START")
            self.progress_bar.stop()
            self.log("Stopping...", "warning")
        else:
            self.running = True
            self.start_btn.config(text="⏹ STOP")
            self.progress_bar.start()
            self.worker_thread = threading.Thread(target=self.process_loop, daemon=True)
            self.worker_thread.start()
            self.log("Started monitoring", "success")

    def scan_once(self):
        """Run a single scan."""
        if not self.running:
            threading.Thread(target=self.scan_and_process, daemon=True).start()

    def process_loop(self):
        """Main processing loop."""
        while self.running:
            self.scan_and_process()
            for _ in range(30):  # Wait 30 seconds between scans
                if not self.running:
                    break
                time.sleep(1)

        self.root.after(0, lambda: self.current_file_label.config(text="Idle"))
        self.root.after(0, self.progress_bar.stop)

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
                if 'h265' not in str(f).lower():
                    video_files.append(f)

        self.root.after(0, lambda: self.log(f"Found {len(video_files)} video files"))

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

    def process_file(self, input_path: Path):
        """Process a single file."""
        size_gb = input_path.stat().st_size / (1024**3)
        self.root.after(0, lambda: self.current_file_label.config(
            text=f"Processing: {input_path.name} ({size_gb:.2f} GB)"))
        self.root.after(0, lambda: self.log(f"Processing: {input_path.name}"))

        # Check if file is stable
        try:
            size1 = input_path.stat().st_size
            time.sleep(3)
            size2 = input_path.stat().st_size
            if size1 != size2:
                self.root.after(0, lambda: self.log("File still syncing, skipping for now", "warning"))
                return
        except:
            return

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

        try:
            # Log the command for debugging
            self.root.after(0, lambda: self.log(f"CMD: {' '.join(cmd[:6])}...", "info"))

            process = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
            )

            last_lines = []  # Keep last few lines for error reporting
            for line in process.stdout:
                last_lines.append(line.strip())
                if len(last_lines) > 10:
                    last_lines.pop(0)
                if 'time=' in line:
                    self.root.after(0, lambda l=line: self.progress_label.config(text=l.strip()[-80:]))

            process.wait()

            if process.returncode == 0 and temp_path.exists():
                temp_path.rename(output_path)
                input_size = input_path.stat().st_size
                output_size = output_path.stat().st_size
                reduction = (1 - output_size/input_size) * 100

                self.root.after(0, lambda: self.log(
                    f"Done! {reduction:.1f}% smaller", "success"))
                self.mark_processed(input_path, str(output_path), "done", input_size, output_size)
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

    def build_ffmpeg_command(self, input_path: Path, output_path: Path) -> list:
        """Build FFmpeg command."""
        encoder = self.encoder.get()
        cq = self.cq_value.get()

        # Use -f mp4 to explicitly set format (needed for .tmp extension)
        if encoder == 'nvenc':
            return [
                'ffmpeg', '-hide_banner', '-y', '-i', str(input_path),
                '-map', '0', '-map_metadata', '0',
                '-c:v', 'hevc_nvenc', '-preset', 'p5', '-rc:v', 'vbr', '-cq:v', str(cq),
                '-c:a', 'copy', '-c:s', 'copy', '-f', 'mp4', str(output_path)
            ]
        elif encoder == 'qsv':
            return [
                'ffmpeg', '-hide_banner', '-y', '-hwaccel', 'qsv', '-i', str(input_path),
                '-map', '0', '-map_metadata', '0',
                '-c:v', 'hevc_qsv', '-preset', 'medium', '-global_quality:v', str(cq),
                '-c:a', 'copy', '-c:s', 'copy', '-f', 'mp4', str(output_path)
            ]
        else:  # cpu
            return [
                'ffmpeg', '-hide_banner', '-y', '-i', str(input_path),
                '-map', '0', '-map_metadata', '0',
                '-c:v', 'libx265', '-preset', 'medium', '-crf', str(cq),
                '-c:a', 'copy', '-c:s', 'copy', '-f', 'mp4', str(output_path)
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
