"""
In-memory live activity registry.

The dispatcher/database track *what jobs exist*; this module tracks *what each
worker is doing right now* so the dashboard can show real-time progress
(percent, throughput, ETA) without writing to SQLite on every callback.

A single process-wide ActivityRegistry instance is shared by all workers and
the scanner. Each worker owns one slot keyed by its name; each progress
callback overwrites that slot. The scanner has its own slot for the current
walk position. The API server reads snapshots and serves them as JSON.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class WorkerActivity:
    worker: str
    stage: str               # "download" | "transcode" | "upload"
    job_id: int
    path: str                # dropbox path of the file being processed
    started_at: float        # time.time() at process_job entry
    bytes_done: int = 0
    bytes_total: int = 0
    time_sec: float = 0.0    # transcode: encoded video seconds so far
    duration_sec: float = 0.0  # transcode: total expected video duration
    fps: float = 0.0
    speed: float = 0.0       # transcode: ffmpeg "speed" multiplier
    bitrate_kbps: float = 0.0
    encoder: str = ""        # "hevc_qsv" | "hevc_nvenc" | "libx265" | ""
    last_update: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        elapsed = max(0.0, time.time() - self.started_at)
        pct = 0.0
        eta_sec: Optional[float] = None
        if self.stage == "transcode" and self.duration_sec > 0 and self.time_sec > 0:
            pct = min(100.0, 100.0 * self.time_sec / self.duration_sec)
            if self.speed > 0:
                eta_sec = max(0.0, (self.duration_sec - self.time_sec) / self.speed)
        elif self.bytes_total > 0 and self.bytes_done > 0:
            pct = min(100.0, 100.0 * self.bytes_done / self.bytes_total)
            if elapsed > 0:
                rate = self.bytes_done / elapsed
                if rate > 0:
                    eta_sec = max(0.0, (self.bytes_total - self.bytes_done) / rate)
        return {
            "worker": self.worker,
            "stage": self.stage,
            "job_id": self.job_id,
            "path": self.path,
            "started_at": self.started_at,
            "elapsed_sec": elapsed,
            "bytes_done": self.bytes_done,
            "bytes_total": self.bytes_total,
            "time_sec": self.time_sec,
            "duration_sec": self.duration_sec,
            "fps": self.fps,
            "speed": self.speed,
            "bitrate_kbps": self.bitrate_kbps,
            "encoder": self.encoder,
            "percent": pct,
            "eta_sec": eta_sec,
            "last_update": self.last_update,
        }


@dataclass
class ScannerActivity:
    mode: str = "idle"             # "idle" | "bulk" | "delta"
    current_path: str = ""
    entries_seen: int = 0
    started_at: float = 0.0
    last_update: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "mode": self.mode,
            "current_path": self.current_path,
            "entries_seen": self.entries_seen,
            "started_at": self.started_at,
            "elapsed_sec": (time.time() - self.started_at) if self.started_at else 0.0,
            "last_update": self.last_update,
        }


class ActivityRegistry:
    """Thread-safe live progress registry shared across workers + scanner."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._workers: dict[str, WorkerActivity] = {}
        self._scanner = ScannerActivity()

    # -- worker slots ------------------------------------------------------

    def begin(self, worker: str, stage: str, job_id: int, path: str,
              bytes_total: int = 0, duration_sec: float = 0.0) -> None:
        with self._lock:
            self._workers[worker] = WorkerActivity(
                worker=worker,
                stage=stage,
                job_id=job_id,
                path=path,
                started_at=time.time(),
                bytes_total=bytes_total,
                duration_sec=duration_sec,
            )

    def update(self, worker: str, **fields) -> None:
        with self._lock:
            wa = self._workers.get(worker)
            if wa is None:
                return
            for k, v in fields.items():
                if hasattr(wa, k) and v is not None:
                    setattr(wa, k, v)
            wa.last_update = time.time()

    def end(self, worker: str) -> None:
        with self._lock:
            self._workers.pop(worker, None)

    def workers_snapshot(self) -> list[dict]:
        with self._lock:
            return [wa.to_dict() for wa in self._workers.values()]

    # -- scanner slot ------------------------------------------------------

    def scan_begin(self, mode: str) -> None:
        with self._lock:
            self._scanner = ScannerActivity(
                mode=mode,
                started_at=time.time(),
            )

    def scan_update(self, current_path: str | None = None,
                    entries_seen: int | None = None) -> None:
        with self._lock:
            if current_path is not None:
                self._scanner.current_path = current_path
            if entries_seen is not None:
                self._scanner.entries_seen = entries_seen
            self._scanner.last_update = time.time()

    def scan_end(self) -> None:
        with self._lock:
            self._scanner = ScannerActivity()

    def scan_snapshot(self) -> dict:
        with self._lock:
            return self._scanner.to_dict()


# Process-wide singleton. Workers and scanner import this directly.
REGISTRY = ActivityRegistry()
