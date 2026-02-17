#!/usr/bin/env python3
"""
HeavyDrops Transcoder v4.4 — GUI

Tkinter GUI wrapping the v4.4 Dropbox transcoder backend.
START/PAUSE/STOP, live progress, queue view, log output.
"""

import json
import logging
import os
import queue
import shutil
import signal
import subprocess
import sys
import re
import threading
import time
from datetime import datetime
from pathlib import Path, PurePosixPath

# ---------------------------------------------------------------------------
# Auto-install GUI deps (same pattern as transcode.py)
# ---------------------------------------------------------------------------
def _ensure_deps():
    missing = []
    for pkg in ["dropbox", "yaml"]:
        try:
            __import__(pkg)
        except ImportError:
            missing.append("pyyaml" if pkg == "yaml" else pkg)
    if missing:
        subprocess.check_call([sys.executable, "-m", "pip", "install"] + missing)

_ensure_deps()

# Import backend pieces from transcode.py (same directory)
from transcode import (
    DEFAULT_CONFIG,
    DBX,
    DoneTracker,
    detect_encoder,
    probe,
    build_ffmpeg_cmd,
    validate_output,
    is_eligible,
    h265_output_path,
    fmt_size,
)

import tkinter as tk
from tkinter import ttk, scrolledtext, filedialog, messagebox

VERSION = "4.4"

# ---------------------------------------------------------------------------
# Logging handler that forwards to a queue for the GUI
# ---------------------------------------------------------------------------
class QueueLogHandler(logging.Handler):
    def __init__(self, log_queue: queue.Queue):
        super().__init__()
        self.log_queue = log_queue

    def emit(self, record):
        try:
            msg = self.format(record)
            self.log_queue.put(msg)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Worker — runs transcoding in a background thread
# ---------------------------------------------------------------------------
class TranscodeWorker:
    """Runs the scan-download-transcode-upload loop in a background thread."""

    def __init__(self, cfg: dict, log_queue: queue.Queue):
        self.cfg = cfg
        self.log_queue = log_queue
        self._thread = None
        self._stop = threading.Event()
        self._pause = threading.Event()
        self._pause.set()  # not paused initially

        # Observable state for the GUI
        self.state = "idle"  # idle / scanning / downloading / transcoding / uploading
        self.current_file = ""
        self.current_file_size = 0
        self.queue_count = 0
        self.queue_total = 0
        self.progress_pct = 0.0
        self.speed_str = ""
        self.files_processed = 0
        self.total_saved_bytes = 0
        self.errors = 0
        self.ffmpeg_proc = None
        self._lock = threading.Lock()

    # -- control --
    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._pause.set()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def pause(self):
        self._pause.clear()

    def resume(self):
        self._pause.set()

    @property
    def paused(self):
        return not self._pause.is_set()

    def stop(self):
        self._stop.set()
        self._pause.set()  # unblock if paused
        proc = self.ffmpeg_proc
        if proc:
            try:
                proc.terminate()
            except Exception:
                pass
        if self._thread:
            self._thread.join(timeout=15)

    @property
    def running(self):
        return self._thread is not None and self._thread.is_alive()

    # -- log helper --
    def _log(self, msg, level="info"):
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_queue.put(f"[{ts}] {msg}")

    # -- main loop --
    def _run(self):
        cfg = self.cfg
        try:
            self.state = "init"
            self._log(f"Connecting to Dropbox...")
            dbx = DBX(cfg["dropbox_token"])
            done = DoneTracker(cfg["done_file"])
            staging = Path(cfg["staging_dir"])
            staging.mkdir(parents=True, exist_ok=True)

            encoder = detect_encoder(cfg["encoder"])
            self._log(f"Encoder: {encoder}")
            self._log(f"Root: {cfg['dropbox_root']}")
            self._log(f"Quality: CQ {cfg['cq']}")
            self._log(f"Done files: {len(done.done)}")
            self._log(f"Delete originals: {cfg['delete_original']}")
            self._log("Ready.")

            while not self._stop.is_set():
                # Wait if paused
                self._pause.wait()
                if self._stop.is_set():
                    break

                # SCAN
                self.state = "scanning"
                self.current_file = ""
                self.progress_pct = 0
                self._log("Scanning Dropbox...")

                file_queue = []
                try:
                    for path, size in dbx.list_videos(cfg["dropbox_root"], cfg["extensions"]):
                        if self._stop.is_set():
                            break
                        if done.is_done(path):
                            continue
                        if not is_eligible(path, size, cfg):
                            continue
                        file_queue.append((path, size))
                        if len(file_queue) >= cfg["queue_size"]:
                            break
                except Exception as e:
                    self._log(f"Scan error: {e}")
                    self.errors += 1
                    self._wait(60)
                    continue

                if not file_queue:
                    self._log(f"No files to process. Waiting {cfg['scan_interval']}s...")
                    self.state = "idle"
                    self._wait(cfg["scan_interval"])
                    continue

                self.queue_total = len(file_queue)
                self._log(f"Queue: {self.queue_total} files")

                # PROCESS QUEUE
                for i, (path, size) in enumerate(file_queue):
                    if self._stop.is_set():
                        break
                    self._pause.wait()
                    if self._stop.is_set():
                        break

                    self.queue_count = i + 1
                    self.current_file = PurePosixPath(path).name
                    self.current_file_size = size
                    self.progress_pct = 0
                    name = self.current_file

                    self._log(f"[{i+1}/{self.queue_total}] {path} ({fmt_size(size)})")

                    ok = self._process_one(dbx, path, size, encoder, cfg, done, staging)
                    if ok:
                        self.files_processed += 1

                    # Disk space check
                    try:
                        usage = shutil.disk_usage(staging)
                        free_gb = usage.free / (1024 ** 3)
                        if free_gb < 20:
                            self._log(f"Low disk: {free_gb:.1f}GB free — pausing 5min")
                            self._wait(300)
                    except Exception:
                        pass

            self.state = "idle"
            self.current_file = ""
            self._log("Stopped.")
        except Exception as e:
            self._log(f"FATAL: {e}")
            self.state = "error"

    def _process_one(self, dbx, path, size, encoder, cfg, done, staging):
        """Process one file. Returns True on success."""
        import hashlib
        name = PurePosixPath(path).name
        work_dir = staging / hashlib.md5(path.encode()).hexdigest()[:12]
        input_file = work_dir / f"in_{name}"
        output_file = work_dir / f"out_{name}"
        out_dbx = h265_output_path(path)

        try:
            work_dir.mkdir(parents=True, exist_ok=True)

            # 1. Check if output already exists
            if dbx.exists(out_dbx):
                self._log(f"  Output exists, skipping")
                done.mark_done(path)
                return True

            # 2. Download
            self.state = "downloading"
            self.progress_pct = 0
            self._log(f"  Downloading...")
            t0 = time.time()
            dbx.download(path, input_file)
            dl_time = time.time() - t0
            self._log(f"  Downloaded in {dl_time:.0f}s")

            # 3. Probe
            info = probe(input_file)
            if not info:
                self._log(f"  Probe failed, skipping")
                done.mark_done(path)
                return False

            if info["codec"] in ("hevc", "h265", "hev1", "hvc1"):
                self._log(f"  Already HEVC, skipping")
                done.mark_done(path)
                return True

            if info["codec"] not in ("h264", "avc", "avc1"):
                self._log(f"  Codec is {info['codec']}, not h264 — skipping")
                done.mark_done(path)
                return True

            # 4. Transcode
            self.state = "transcoding"
            self.progress_pct = 0
            self._log(f"  Transcoding {info['codec']} -> hevc ({info['duration']:.0f}s)...")
            t0 = time.time()
            ok = self._run_transcode(input_file, output_file, encoder, cfg["cq"], info)
            enc_time = time.time() - t0

            if not ok:
                # Retry with audio re-encode
                if info["has_audio"]:
                    self._log(f"  Retrying with audio re-encode...")
                    output_file.unlink(missing_ok=True)
                    ok = self._run_transcode(input_file, output_file, encoder, cfg["cq"], info, reencode_audio=True)
                    enc_time = time.time() - t0

            if not ok:
                self._log(f"  Transcode FAILED")
                self.errors += 1
                return False

            # 5. Validate
            if not validate_output(output_file, info["duration"]):
                self._log(f"  Validation FAILED")
                self.errors += 1
                return False

            in_size = input_file.stat().st_size
            out_size = output_file.stat().st_size
            ratio = (1 - out_size / in_size) * 100 if in_size > 0 else 0
            self.total_saved_bytes += (in_size - out_size)
            self._log(f"  {fmt_size(in_size)} -> {fmt_size(out_size)} ({ratio:.0f}% smaller) in {enc_time:.0f}s")

            # 6. Upload
            self.state = "uploading"
            self.progress_pct = 0
            self._log(f"  Uploading...")
            out_dir = str(PurePosixPath(out_dbx).parent)
            dbx.mkdir(out_dir)
            t0 = time.time()
            dbx.upload(output_file, out_dbx)
            self._log(f"  Uploaded in {time.time() - t0:.0f}s")

            # 7. Delete original
            if cfg["delete_original"]:
                self._log(f"  Deleting original")
                dbx.delete(path)

            done.mark_done(path)
            return True

        except Exception as e:
            self._log(f"  ERROR: {e}")
            self.errors += 1
            return False
        finally:
            if work_dir.exists():
                shutil.rmtree(work_dir, ignore_errors=True)

    def _run_transcode(self, input_path, output_path, encoder, cq, info, reencode_audio=False):
        """Run ffmpeg with progress tracking."""
        cmd = build_ffmpeg_cmd(input_path, output_path, encoder, cq, info)

        if reencode_audio:
            new_cmd = []
            i = 0
            while i < len(cmd):
                if cmd[i] == "-c:a" and i + 1 < len(cmd) and cmd[i + 1] == "copy":
                    new_cmd += ["-c:a", "aac", "-b:a", "320k"]
                    i += 2
                else:
                    new_cmd.append(cmd[i])
                    i += 1
            cmd = new_cmd

        expected_dur = info["duration"]
        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            self.ffmpeg_proc = proc
            for line in proc.stderr:
                if self._stop.is_set():
                    proc.terminate()
                    return False
                m = re.search(r"time=(\d{2}):(\d{2}):(\d{2})\.(\d{2})", line)
                if m:
                    h, mi, s, _ = map(int, m.groups())
                    cur = h * 3600 + mi * 60 + s
                    if expected_dur > 0:
                        self.progress_pct = min(99.9, cur / expected_dur * 100)
                    sp = re.search(r"speed=\s*([\d.]+)x", line)
                    self.speed_str = f"{sp.group(1)}x" if sp else ""
            proc.wait()
            self.ffmpeg_proc = None
            self.progress_pct = 100 if proc.returncode == 0 else 0
            return proc.returncode == 0
        except Exception as e:
            self.ffmpeg_proc = None
            self._log(f"  ffmpeg error: {e}")
            return False

    def _wait(self, seconds):
        """Wait with stop check."""
        for _ in range(seconds):
            if self._stop.is_set():
                break
            time.sleep(1)


# ---------------------------------------------------------------------------
# GUI
# ---------------------------------------------------------------------------
class TranscoderApp:
    def __init__(self, root):
        self.root = root
        self.root.title(f"HeavyDrops Transcoder v{VERSION}")
        self.root.geometry("960x720")
        self.root.minsize(800, 550)

        self.log_queue = queue.Queue()
        self.worker = None

        # Config vars
        self.var_token = tk.StringVar()
        self.var_root = tk.StringVar(value="/Videos")
        self.var_staging = tk.StringVar(value="/tmp/transcode")
        self.var_encoder = tk.StringVar(value="auto")
        self.var_cq = tk.IntVar(value=24)
        self.var_min_size = tk.DoubleVar(value=0)
        self.var_queue_size = tk.IntVar(value=100)
        self.var_scan_interval = tk.IntVar(value=300)
        self.var_delete_original = tk.BooleanVar(value=True)
        self.var_done_file = tk.StringVar(value="done.json")

        # Load config
        self._load_config()

        self._build_ui()
        self._poll_log()
        self._poll_status()

        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    # ---- config ----
    def _load_config(self):
        """Load config.yaml / env vars into tk vars."""
        cfg = dict(DEFAULT_CONFIG)
        for p in ["config.yaml", "config.yml"]:
            if Path(p).exists():
                try:
                    import yaml
                    with open(p) as f:
                        user_cfg = yaml.safe_load(f) or {}
                    cfg.update(user_cfg)
                except Exception:
                    pass
                break

        if not cfg["dropbox_token"]:
            cfg["dropbox_token"] = os.environ.get("DROPBOX_TOKEN", "")

        self.var_token.set(cfg.get("dropbox_token", ""))
        self.var_root.set(cfg.get("dropbox_root", "/Videos"))
        self.var_staging.set(cfg.get("staging_dir", "/tmp/transcode"))
        self.var_encoder.set(cfg.get("encoder", "auto"))
        self.var_cq.set(cfg.get("cq", 24))
        self.var_min_size.set(cfg.get("min_size_gb", 0))
        self.var_queue_size.set(cfg.get("queue_size", 100))
        self.var_scan_interval.set(cfg.get("scan_interval", 300))
        self.var_delete_original.set(cfg.get("delete_original", True))
        self.var_done_file.set(cfg.get("done_file", "done.json"))

    def _get_config(self) -> dict:
        """Build config dict from current UI values."""
        return {
            "dropbox_token": self.var_token.get().strip(),
            "dropbox_root": self.var_root.get().strip(),
            "staging_dir": self.var_staging.get().strip(),
            "encoder": self.var_encoder.get(),
            "cq": self.var_cq.get(),
            "min_size_gb": self.var_min_size.get(),
            "queue_size": self.var_queue_size.get(),
            "scan_interval": self.var_scan_interval.get(),
            "extensions": [".mp4", ".mov"],
            "done_file": self.var_done_file.get().strip(),
            "delete_original": self.var_delete_original.get(),
        }

    # ---- UI ----
    def _build_ui(self):
        style = ttk.Style()
        style.configure("Status.TLabel", font=("Consolas", 9))
        style.configure("Title.TLabel", font=("", 11, "bold"))
        style.configure("Big.TLabel", font=("Consolas", 14, "bold"))

        main = ttk.Frame(self.root, padding=8)
        main.pack(fill=tk.BOTH, expand=True)

        # ── SETTINGS ──
        sf = ttk.LabelFrame(main, text=" Settings ", padding=8)
        sf.pack(fill=tk.X, pady=(0, 6))

        # Row 0: Dropbox token
        ttk.Label(sf, text="Dropbox Token:").grid(row=0, column=0, sticky=tk.W, pady=3)
        tok_frame = ttk.Frame(sf)
        tok_frame.grid(row=0, column=1, sticky=tk.EW, pady=3)
        self.tok_entry = ttk.Entry(tok_frame, textvariable=self.var_token, show="*", width=50)
        self.tok_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self._tok_visible = False
        ttk.Button(tok_frame, text="Show", width=5, command=self._toggle_token).pack(side=tk.LEFT, padx=(4, 0))

        # Row 1: Dropbox root
        ttk.Label(sf, text="Dropbox Root:").grid(row=1, column=0, sticky=tk.W, pady=3)
        ttk.Entry(sf, textvariable=self.var_root, width=50).grid(row=1, column=1, sticky=tk.EW, pady=3)

        # Row 2: Staging dir
        ttk.Label(sf, text="Staging Dir:").grid(row=2, column=0, sticky=tk.W, pady=3)
        stg_frame = ttk.Frame(sf)
        stg_frame.grid(row=2, column=1, sticky=tk.EW, pady=3)
        ttk.Entry(stg_frame, textvariable=self.var_staging, width=50).pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Button(stg_frame, text="Browse", command=self._browse_staging).pack(side=tk.LEFT, padx=(4, 0))

        # Row 3: Encoder + CQ
        ttk.Label(sf, text="Encoder:").grid(row=3, column=0, sticky=tk.W, pady=3)
        enc_frame = ttk.Frame(sf)
        enc_frame.grid(row=3, column=1, sticky=tk.W, pady=3)
        for val, label in [("auto", "Auto"), ("hevc_qsv", "Intel QSV"), ("hevc_nvenc", "NVIDIA NVENC"), ("libx265", "CPU (x265)")]:
            ttk.Radiobutton(enc_frame, text=label, variable=self.var_encoder, value=val).pack(side=tk.LEFT, padx=(0, 12))
        ttk.Label(enc_frame, text="  CQ:").pack(side=tk.LEFT)
        ttk.Spinbox(enc_frame, from_=15, to=35, textvariable=self.var_cq, width=4).pack(side=tk.LEFT, padx=(4, 0))

        # Row 4: Min size + delete original
        ttk.Label(sf, text="Options:").grid(row=4, column=0, sticky=tk.W, pady=3)
        opt_frame = ttk.Frame(sf)
        opt_frame.grid(row=4, column=1, sticky=tk.W, pady=3)
        ttk.Label(opt_frame, text="Min size (GB):").pack(side=tk.LEFT)
        ttk.Spinbox(opt_frame, from_=0, to=500, textvariable=self.var_min_size, width=6).pack(side=tk.LEFT, padx=(4, 16))
        ttk.Checkbutton(opt_frame, text="Delete original after upload", variable=self.var_delete_original).pack(side=tk.LEFT)

        sf.columnconfigure(1, weight=1)

        # ── CONTROLS ──
        cf = ttk.Frame(main)
        cf.pack(fill=tk.X, pady=(0, 6))

        self.btn_start = ttk.Button(cf, text="START", command=self._on_start, width=10)
        self.btn_start.pack(side=tk.LEFT, padx=(0, 4))
        self.btn_pause = ttk.Button(cf, text="PAUSE", command=self._on_pause, width=10, state=tk.DISABLED)
        self.btn_pause.pack(side=tk.LEFT, padx=(0, 4))
        self.btn_stop = ttk.Button(cf, text="STOP", command=self._on_stop, width=10, state=tk.DISABLED)
        self.btn_stop.pack(side=tk.LEFT, padx=(0, 16))

        # Stats on the right
        stats = ttk.Frame(cf)
        stats.pack(side=tk.RIGHT)
        ttk.Label(stats, text="Processed:").pack(side=tk.LEFT)
        self.lbl_processed = ttk.Label(stats, text="0", style="Big.TLabel")
        self.lbl_processed.pack(side=tk.LEFT, padx=(4, 16))
        ttk.Label(stats, text="Saved:").pack(side=tk.LEFT)
        self.lbl_saved = ttk.Label(stats, text="0 GB", style="Big.TLabel")
        self.lbl_saved.pack(side=tk.LEFT, padx=(4, 0))

        # ── PROGRESS ──
        pf = ttk.LabelFrame(main, text=" Progress ", padding=8)
        pf.pack(fill=tk.X, pady=(0, 6))

        # Status line
        self.lbl_state = ttk.Label(pf, text="Idle", style="Status.TLabel")
        self.lbl_state.pack(anchor=tk.W)

        # Current file
        self.lbl_file = ttk.Label(pf, text="", style="Status.TLabel", wraplength=900)
        self.lbl_file.pack(anchor=tk.W, pady=(2, 0))

        # Progress bar
        self.progress_var = tk.DoubleVar(value=0)
        self.progress_bar = ttk.Progressbar(pf, variable=self.progress_var, maximum=100)
        self.progress_bar.pack(fill=tk.X, pady=(6, 0))

        # Progress detail
        self.lbl_detail = ttk.Label(pf, text="", style="Status.TLabel")
        self.lbl_detail.pack(anchor=tk.W, pady=(2, 0))

        # ── LOG ──
        lf = ttk.LabelFrame(main, text=" Log ", padding=4)
        lf.pack(fill=tk.BOTH, expand=True)

        self.log_text = scrolledtext.ScrolledText(lf, wrap=tk.WORD, font=("Consolas", 9),
                                                   bg="#1e1e1e", fg="#d4d4d4",
                                                   insertbackground="#d4d4d4",
                                                   state=tk.DISABLED, height=12)
        self.log_text.pack(fill=tk.BOTH, expand=True)

        # Tag for coloring
        self.log_text.tag_configure("error", foreground="#f44747")
        self.log_text.tag_configure("success", foreground="#6a9955")
        self.log_text.tag_configure("info", foreground="#d4d4d4")

    # ---- token visibility ----
    def _toggle_token(self):
        self._tok_visible = not self._tok_visible
        self.tok_entry.config(show="" if self._tok_visible else "*")

    def _browse_staging(self):
        d = filedialog.askdirectory()
        if d:
            self.var_staging.set(d)

    # ---- controls ----
    def _on_start(self):
        cfg = self._get_config()
        if not cfg["dropbox_token"]:
            messagebox.showerror("Missing Token", "Set your Dropbox token first.")
            return

        if self.worker and self.worker.running:
            if self.worker.paused:
                self.worker.resume()
                self._set_running_ui()
            return

        self.worker = TranscodeWorker(cfg, self.log_queue)
        self.worker.start()
        self._set_running_ui()

    def _on_pause(self):
        if not self.worker or not self.worker.running:
            return
        if self.worker.paused:
            self.worker.resume()
            self.btn_pause.config(text="PAUSE")
            self.lbl_state.config(text="Running")
        else:
            self.worker.pause()
            self.btn_pause.config(text="RESUME")
            self.lbl_state.config(text="Paused")

    def _on_stop(self):
        if self.worker:
            self._append_log("[GUI] Stopping...")
            self.worker.stop()
        self._set_idle_ui()

    def _set_running_ui(self):
        self.btn_start.config(state=tk.DISABLED)
        self.btn_pause.config(state=tk.NORMAL, text="PAUSE")
        self.btn_stop.config(state=tk.NORMAL)
        self.lbl_state.config(text="Running")
        # Disable settings
        for w in self.root.winfo_children():
            self._toggle_settings(w, False)

    def _set_idle_ui(self):
        self.btn_start.config(state=tk.NORMAL)
        self.btn_pause.config(state=tk.DISABLED, text="PAUSE")
        self.btn_stop.config(state=tk.DISABLED)
        self.lbl_state.config(text="Idle")
        self.lbl_file.config(text="")
        self.progress_var.set(0)
        self.lbl_detail.config(text="")
        # Re-enable settings
        for w in self.root.winfo_children():
            self._toggle_settings(w, True)

    def _toggle_settings(self, widget, enable):
        """Recursively enable/disable settings widgets (skip control buttons and log)."""
        pass  # Settings locking is optional, keep it simple

    # ---- polling ----
    def _poll_log(self):
        """Drain log queue into the text widget."""
        batch = []
        try:
            while True:
                batch.append(self.log_queue.get_nowait())
        except queue.Empty:
            pass

        if batch:
            self.log_text.config(state=tk.NORMAL)
            for msg in batch:
                tag = "info"
                if "ERROR" in msg or "FAIL" in msg or "FATAL" in msg:
                    tag = "error"
                elif "Done:" in msg or "Uploaded" in msg or "skipping" in msg.lower():
                    tag = "success"
                self.log_text.insert(tk.END, msg + "\n", tag)
            self.log_text.see(tk.END)
            self.log_text.config(state=tk.DISABLED)

        self.root.after(200, self._poll_log)

    def _poll_status(self):
        """Update progress UI from worker state."""
        w = self.worker
        if w and w.running:
            # State label
            state_map = {
                "init": "Initializing...",
                "scanning": "Scanning Dropbox...",
                "downloading": "Downloading",
                "transcoding": "Transcoding",
                "uploading": "Uploading",
                "idle": "Waiting for next scan...",
                "error": "Error",
            }
            state_text = state_map.get(w.state, w.state)
            if w.queue_total > 0:
                state_text += f"  [{w.queue_count}/{w.queue_total}]"
            if w.paused:
                state_text = "PAUSED — " + state_text
            self.lbl_state.config(text=state_text)

            # File
            if w.current_file:
                self.lbl_file.config(text=f"{w.current_file}  ({fmt_size(w.current_file_size)})")
            else:
                self.lbl_file.config(text="")

            # Progress bar
            if w.state == "transcoding":
                self.progress_var.set(w.progress_pct)
                detail = f"{w.progress_pct:.1f}%"
                if w.speed_str:
                    detail += f"  speed: {w.speed_str}"
                self.lbl_detail.config(text=detail)
            elif w.state in ("downloading", "uploading"):
                self.progress_bar.config(mode="indeterminate")
                self.progress_bar.start(15)
                self.lbl_detail.config(text=state_text)
            else:
                self.progress_bar.stop()
                self.progress_bar.config(mode="determinate")
                self.progress_var.set(0)
                self.lbl_detail.config(text="")

            # Fix: stop indeterminate when switching away
            if w.state == "transcoding":
                self.progress_bar.stop()
                self.progress_bar.config(mode="determinate")

            # Stats
            self.lbl_processed.config(text=str(w.files_processed))
            saved_gb = w.total_saved_bytes / (1024 ** 3)
            self.lbl_saved.config(text=f"{saved_gb:.2f} GB")
        elif w and not w.running:
            self._set_idle_ui()
            self.worker = None

        self.root.after(500, self._poll_status)

    def _append_log(self, msg):
        self.log_queue.put(msg)

    # ---- close ----
    def _on_close(self):
        if self.worker and self.worker.running:
            if not messagebox.askokcancel("Quit", "Transcoding is running. Stop and quit?"):
                return
            self.worker.stop()
        self.root.destroy()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    root = tk.Tk()
    app = TranscoderApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
