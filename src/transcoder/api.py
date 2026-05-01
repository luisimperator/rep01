"""
Local HTTP API for status monitoring and control.

A minimal ThreadingHTTPServer bound to loopback exposes a handful of JSON
endpoints plus a self-contained HTML dashboard. No auth: loopback-only.

Endpoints
---------
GET  /               — HTML dashboard (auto-refreshing)
GET  /api/status     — daemon health + scan/update/queue/disk overview
GET  /api/jobs       — job rows (query: ?state=NEW&limit=50)
GET  /api/metrics    — aggregated counts + queue depths
POST /api/pause      — pause dispatcher (in-flight jobs still finish)
POST /api/resume     — resume dispatcher
POST /api/scan-now   — short-circuit the scan_loop sleep
POST /api/retry-failed — reset FAILED → RETRY_WAIT
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlparse

from dataclasses import dataclass, field

from .database import Database, JobState
from .progress import REGISTRY as ACTIVITY
from .updater import read_status as read_update_status

if TYPE_CHECKING:
    from .config import Config
    from .dispatcher import JobDispatcher

logger = logging.getLogger(__name__)


DASHBOARD_HTML = (Path(__file__).parent / "dashboard.html").read_text(encoding="utf-8")


class ApiServer:
    """Runs a ThreadingHTTPServer in a background thread until shutdown()."""

    def __init__(
        self,
        config: "Config",
        db: Database,
        dispatcher: "JobDispatcher",
        scan_trigger: threading.Event,
        started_at_epoch: float,
    ) -> None:
        self.config = config
        self.db = db
        self.dispatcher = dispatcher
        self.scan_trigger = scan_trigger
        self.started_at_epoch = started_at_epoch
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        # Holds the most recent reorganize-existing run (preview or actual).
        # Lifecycle: created lazily by /api/reorganize/run, polled by
        # /api/reorganize/status until done.
        self._reorganize_run: ReorganizeRun | None = None
        self._reorganize_lock = threading.Lock()

    def start(self) -> None:
        if not self.config.api.enabled:
            logger.info("api: disabled via config")
            return
        handler_cls = _build_handler(self)
        try:
            self._server = ThreadingHTTPServer(
                (self.config.api.bind, self.config.api.port),
                handler_cls,
            )
        except OSError as e:
            logger.error("api: failed to bind %s:%d — %s",
                         self.config.api.bind, self.config.api.port, e)
            return

        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="api-server",
            daemon=True,
        )
        self._thread.start()
        logger.info("api: listening on http://%s:%d",
                    self.config.api.bind, self.config.api.port)

    def shutdown(self) -> None:
        if self._server is not None:
            try:
                self._server.shutdown()
                self._server.server_close()
            except Exception:
                logger.exception("api: error during shutdown")
            self._server = None


# ----------------------------------------------------------- request handlers

def _build_handler(api: ApiServer):
    """Close over the ApiServer so the handler can reach the daemon bits."""

    class _Handler(BaseHTTPRequestHandler):
        # Keep stdlib's chatter out of our log file.
        def log_message(self, fmt: str, *args: Any) -> None:
            return

        def do_GET(self) -> None:  # noqa: N802 (stdlib naming)
            route, qs = _split(self.path)
            if route == "/":
                return self._send_html(DASHBOARD_HTML)
            if route == "/api/status":
                return self._send_json(_status_payload(api))
            if route == "/api/jobs":
                return self._send_json(_jobs_payload(api, qs))
            if route == "/api/metrics":
                return self._send_json(_metrics_payload(api))
            if route == "/api/active":
                return self._send_json(_active_payload(api))
            if route == "/api/log":
                return self._send_json(_log_payload(api, qs))
            if route == "/api/stats":
                return self._send_json(_stats_payload(api))
            if route == "/api/settings":
                return self._send_json(_settings_payload(api))
            if route == "/api/reorganize/status":
                return self._send_json(_reorganize_status_payload(api))
            if route == "/api/dropbox/list":
                return self._send_json(_dropbox_list_payload(api, qs))
            if route == "/healthz":
                return self._send_text("ok")
            self.send_error(404, "not found")

        def do_POST(self) -> None:  # noqa: N802
            route, _ = _split(self.path)
            if route == "/api/pause":
                api.dispatcher.pause()
                return self._send_json({"ok": True, "paused": True})
            if route == "/api/resume":
                api.dispatcher.resume()
                return self._send_json({"ok": True, "paused": False})
            if route == "/api/scan-now":
                api.scan_trigger.set()
                return self._send_json({"ok": True, "triggered": True})
            if route == "/api/retry-failed":
                count = api.db.reset_failed_jobs()
                return self._send_json({"ok": True, "reset": count})
            if route == "/api/settings":
                body = self._read_json_body()
                try:
                    result = _apply_settings(api, body)
                    return self._send_json({"ok": True, **result})
                except ValueError as e:
                    return self._send_json({"ok": False, "error": str(e)})
            if route == "/api/reorganize/preview":
                body = self._read_json_body() or {}
                return self._send_json(_reorganize_preview(api, body))
            if route == "/api/reorganize/run":
                body = self._read_json_body() or {}
                return self._send_json(_reorganize_run(api, body))
            if route == "/api/restart":
                # Schedule a graceful exit; the Task Scheduler will restart us.
                threading.Timer(1.0, lambda: os._exit(0)).start()
                return self._send_json({"ok": True, "restarting_in_sec": 1})
            self.send_error(404, "not found")

        def _read_json_body(self) -> dict:
            try:
                length = int(self.headers.get("Content-Length") or "0")
            except ValueError:
                length = 0
            if length <= 0:
                return {}
            raw = self.rfile.read(length)
            try:
                return json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                return {}

        # -------- response helpers
        def _send_json(self, payload: dict) -> None:
            body = json.dumps(payload, default=str).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_text(self, text: str) -> None:
            body = text.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_html(self, html: str) -> None:
            body = html.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return _Handler


def _split(path: str) -> tuple[str, dict[str, list[str]]]:
    parsed = urlparse(path)
    return parsed.path, parse_qs(parsed.query)


# ---------------------------------------------------------------- payload util

def _status_payload(api: ApiServer) -> dict:
    stats = api.db.get_stats()
    update = read_update_status(api.db)
    scan_state = api.db.get_scan_state(api.config.dropbox_root)
    depths = api.dispatcher.queue_depths()
    disk = _disk_snapshot(api)

    uptime_sec = max(0.0, time.time() - api.started_at_epoch)
    state_counts = stats.get("state_counts", {})

    return {
        "ok": True,
        "pid": os.getpid(),
        "uptime_sec": uptime_sec,
        "uptime_human": _human_duration(uptime_sec),
        "scan": {
            "dropbox_root": scan_state.dropbox_root,
            "mode": "delta" if scan_state.bulk_pass_complete else "bulk",
            "bulk_pass_complete": scan_state.bulk_pass_complete,
            "entries_seen": scan_state.entries_seen,
            "bulk_started_at": scan_state.bulk_started_at,
            "bulk_completed_at": scan_state.bulk_completed_at,
            "last_delta_at": scan_state.last_delta_at,
            "last_error": getattr(getattr(api, "daemon", None), "last_scan_error", None),
            "last_error_at": getattr(getattr(api, "daemon", None), "last_scan_error_at", None),
            "namespace": getattr(getattr(getattr(api, "daemon", None), "dropbox", None), "namespace", None),
        },
        "dispatcher": {
            "paused": api.dispatcher.is_paused(),
            "download": depths["download"],
            "transcode": depths["transcode"],
            "upload": depths["upload"],
            "active": depths["active"],
        },
        "disk": disk,
        "jobs": {
            "total": stats.get("total_jobs", 0),
            "done": state_counts.get(JobState.DONE.value, 0),
            "failed": state_counts.get(JobState.FAILED.value, 0),
            "state_counts": state_counts,
            "total_bytes_done": stats.get("total_bytes_done", 0),
            "avg_transcode_seconds": stats.get("avg_transcode_seconds", 0),
        },
        "update": {
            "current_version": update.current_version,
            "latest_tag": update.latest_tag,
            "update_available": update.update_available,
            "checked_at": update.checked_at,
            "error": update.error,
        },
    }


def _jobs_payload(api: ApiServer, qs: dict[str, list[str]]) -> dict:
    raw_state = (qs.get("state") or [None])[0]
    raw_limit = (qs.get("limit") or ["50"])[0]
    try:
        limit = max(1, min(500, int(raw_limit)))
    except ValueError:
        limit = 50

    states: set[JobState] | None = None
    if raw_state:
        try:
            states = {JobState(raw_state)}
        except ValueError:
            states = None

    rows = api.db.list_queue(states=states, limit=limit)
    return {
        "ok": True,
        "count": len(rows),
        "jobs": [
            {
                "id": r.id,
                "state": r.state.value,
                "dropbox_path": r.dropbox_path,
                "dropbox_size": r.dropbox_size,
                "retry_count": r.retry_count,
                "output_path": r.output_path,
                "error_message": r.error_message,
                "encoder_used": r.encoder_used,
                "updated_at": r.updated_at,
            }
            for r in rows
        ],
    }


def _metrics_payload(api: ApiServer) -> dict:
    stats = api.db.get_stats()
    depths = api.dispatcher.queue_depths()
    return {
        "ok": True,
        "queue_depths": depths,
        "state_counts": stats.get("state_counts", {}),
        "total_bytes_done": stats.get("total_bytes_done", 0),
        "avg_transcode_seconds": stats.get("avg_transcode_seconds", 0),
        "disk_reserved_bytes": api.db.total_reserved_bytes(),
    }


def _disk_snapshot(api: ApiServer) -> dict:
    try:
        staging = Path(api.config.local_staging_dir)
        usage = shutil.disk_usage(staging) if staging.exists() else None
    except OSError:
        usage = None

    return {
        "staging_dir": str(api.config.local_staging_dir),
        "reserved_bytes": api.db.total_reserved_bytes(),
        "budget_enabled": api.config.disk_budget.enabled,
        "max_staging_bytes": api.config.disk_budget.max_staging_bytes,
        "min_free_bytes": api.config.disk_budget.min_free_bytes,
        "free_bytes": usage.free if usage else 0,
        "total_bytes": usage.total if usage else 0,
    }


def _active_payload(api: ApiServer) -> dict:
    """Live snapshot of every worker's current job + the scanner walk position."""
    return {
        "ok": True,
        "workers": ACTIVITY.workers_snapshot(),
        "scanner": ACTIVITY.scan_snapshot(),
    }


def _log_payload(api: ApiServer, qs: dict[str, list[str]]) -> dict:
    """Tail of the daemon log file. Capped at 2000 lines to keep payloads small."""
    try:
        n = max(1, min(2000, int((qs.get("lines") or ["200"])[0])))
    except ValueError:
        n = 200

    log_file = Path(api.config.log_dir) / "transcoder.log"
    lines: list[str] = []
    if log_file.exists():
        try:
            with open(log_file, "r", encoding="utf-8", errors="replace") as f:
                # Read the whole file — modest size given daily rotation in
                # practice; cheap enough for a 3s polling dashboard.
                lines = f.readlines()[-n:]
        except OSError as e:
            return {"ok": False, "error": str(e), "lines": []}

    return {
        "ok": True,
        "log_file": str(log_file),
        "lines": [ln.rstrip("\n") for ln in lines],
        "count": len(lines),
    }


def _stats_payload(api: ApiServer) -> dict:
    """Today vs. all-time conversion savings, used by the dashboard's stats card."""
    from datetime import datetime, timezone, timedelta
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    week_start = today_start - timedelta(days=7)
    return {
        "ok": True,
        "today": api.db.get_savings_stats(since=today_start),
        "week": api.db.get_savings_stats(since=week_start),
        "all_time": api.db.get_savings_stats(since=None),
        "as_of": now.isoformat(),
    }


# ----------------------------------------------------------------- settings

# Settings exposed via /api/settings. Keep the surface small and well-typed —
# every entry knows how to validate and how to write itself back to config.yaml.
_SETTINGS_KNOBS: dict[str, dict] = {
    "legacy_reorganize": {
        "type": "bool",
        "yaml_key": "legacy_reorganize",
        "label": "Reorganize after upload (h264/ backup, h265 takes original spot)",
    },
    "legacy_reorganize_min_age_days": {
        "type": "int",
        "min": 0,
        "max": 3650,
        "yaml_key": "legacy_reorganize_min_age_days",
        "label": "Skip reorganize for folders touched in the last N days",
    },
    "cq_value": {
        "type": "int",
        "min": 14,
        "max": 36,
        "yaml_key": "cq_value",
        "label": "H.265 quality (lower = higher quality, larger files)",
    },
    "min_size_gb": {
        "type": "float",
        "min": 0.0,
        "max": 1000.0,
        "yaml_key": "min_size_gb",
        "label": "Skip files smaller than N GB",
    },
    "dropbox_root": {
        "type": "path",
        "yaml_key": "dropbox_root",
        "label": "Dropbox folder to monitor (must start with /)",
    },
}


def _settings_payload(api: ApiServer) -> dict:
    """Snapshot of every editable setting plus a few read-only context fields."""
    cfg = api.config
    return {
        "ok": True,
        "settings": {
            "legacy_reorganize": cfg.legacy_reorganize,
            "legacy_reorganize_min_age_days": cfg.legacy_reorganize_min_age_days,
            "dropbox_root": cfg.dropbox_root,
            "cq_value": cfg.cq_value,
            "min_size_gb": cfg.min_size_gb,
        },
        "knobs": {k: {kk: vv for kk, vv in v.items() if kk != "yaml_key"} for k, v in _SETTINGS_KNOBS.items()},
        "context": {
            "encoder_preference": cfg.encoder_preference.value,
            "profile": cfg.profile.value,
            "dropbox_root": cfg.dropbox_root,
            "ffmpeg_path": str(cfg.ffmpeg_path),
            "config_path": _config_path_hint(api),
        },
    }


def _config_path_hint(api: ApiServer) -> str | None:
    """Best-effort guess at where config.yaml lives so /api/settings POST can
    write back. We mirror the lookup in load_config."""
    from pathlib import Path
    candidates = [
        Path("config.yaml"),
        Path("config.yml"),
        Path.home() / ".config" / "transcoder" / "config.yaml",
        Path("/etc/transcoder/config.yaml"),
    ]
    for p in candidates:
        if p.exists():
            return str(p.resolve())
    return None


def _apply_settings(api: ApiServer, body: dict) -> dict:
    """
    Validate `body`, mutate the in-memory Config so changes take effect on
    the next job (no restart needed for the knobs we expose), and persist
    them to config.yaml so they survive restarts.

    Returns {"updated": [...keys...], "config_path": "..."}.
    """
    import re
    from pathlib import Path

    cfg_path = _config_path_hint(api)
    if cfg_path is None:
        raise ValueError("config.yaml not found; cannot persist settings")

    cfg = api.config
    updated: list[str] = []

    for key, raw in body.items():
        knob = _SETTINGS_KNOBS.get(key)
        if knob is None:
            raise ValueError(f"unknown setting: {key}")
        # Coerce + validate
        if knob["type"] == "bool":
            value = bool(raw)
        elif knob["type"] == "int":
            try:
                value = int(raw)
            except (TypeError, ValueError):
                raise ValueError(f"{key} must be an integer")
            if value < knob["min"] or value > knob["max"]:
                raise ValueError(f"{key} must be between {knob['min']} and {knob['max']}")
        elif knob["type"] == "float":
            try:
                value = float(raw)
            except (TypeError, ValueError):
                raise ValueError(f"{key} must be a number")
            if value < knob["min"] or value > knob["max"]:
                raise ValueError(f"{key} must be between {knob['min']} and {knob['max']}")
        elif knob["type"] == "path":
            value = str(raw or "").strip()
            if not value:
                raise ValueError(f"{key} cannot be empty")
            if not value.startswith("/"):
                value = "/" + value
            # Strip trailing slashes — files_list_folder rejects them.
            while len(value) > 1 and value.endswith("/"):
                value = value[:-1]
        else:
            raise ValueError(f"unsupported type for {key}")

        # Mutate in-memory config (Pydantic model_validate would re-validate,
        # but we already validated; setattr keeps it cheap).
        setattr(cfg, key, value)
        updated.append(key)

    # Persist to config.yaml using regex line replacement so comments survive.
    raw = Path(cfg_path).read_text(encoding="utf-8")
    for key in updated:
        knob = _SETTINGS_KNOBS[key]
        yaml_key = knob["yaml_key"]
        new_value = getattr(cfg, key)
        if isinstance(new_value, bool):
            yaml_val = "true" if new_value else "false"
        else:
            yaml_val = str(new_value)
        pattern = re.compile(rf'^{re.escape(yaml_key)}\s*:.*$', re.MULTILINE)
        line = f"{yaml_key}: {yaml_val}"
        if pattern.search(raw):
            raw = pattern.sub(line, raw, count=1)
        else:
            if raw and not raw.endswith("\n"):
                raw += "\n"
            raw += line + "\n"
    Path(cfg_path).write_text(raw, encoding="utf-8")

    logger.info("settings updated via API: %s", updated)
    return {"updated": updated, "config_path": cfg_path}


# ------------------------------------------------------- reorganize-existing


@dataclass
class ReorganizeRun:
    started_at: float
    threshold_days: int
    folder_filter: str | None
    total_pairs: int
    done_pairs: int = 0
    failed_pairs: int = 0
    skipped_folders: int = 0
    current: str = ""
    finished: bool = False
    error: str | None = None
    log: list[str] = field(default_factory=list)

    def push(self, msg: str) -> None:
        # Cap to keep memory bounded — the dashboard re-fetches anyway.
        self.log.append(msg)
        if len(self.log) > 500:
            self.log = self.log[-500:]

    def to_dict(self) -> dict:
        elapsed = max(0.0, time.time() - self.started_at)
        pct = 0.0
        if self.total_pairs > 0:
            pct = 100.0 * (self.done_pairs + self.failed_pairs) / self.total_pairs
        return {
            "started_at": self.started_at,
            "elapsed_sec": elapsed,
            "threshold_days": self.threshold_days,
            "folder_filter": self.folder_filter,
            "total_pairs": self.total_pairs,
            "done_pairs": self.done_pairs,
            "failed_pairs": self.failed_pairs,
            "skipped_folders": self.skipped_folders,
            "percent": pct,
            "current": self.current,
            "finished": self.finished,
            "error": self.error,
            "log_tail": self.log[-50:],
        }


def _reorganize_preview(api: ApiServer, body: dict) -> dict:
    """
    Synchronously walk the tree and return the list of candidate folders +
    settled/active classification. Cheap enough to call inline (no Dropbox
    moves performed).
    """
    from .dropbox_client import make_client_from_config
    from .reorganize import find_unreorganized_pairs, is_folder_settled

    threshold = int(body.get("min_age_days", api.config.legacy_reorganize_min_age_days))
    folder = body.get("folder") or api.config.dropbox_root

    dropbox = make_client_from_config(api.config)
    candidates = find_unreorganized_pairs(dropbox, folder)

    rows = []
    for cand in candidates:
        activity = is_folder_settled(dropbox, cand.parent, threshold)
        rows.append({
            "parent": cand.parent,
            "pairs": len(cand.pairs),
            "names": [p.name for p in cand.pairs[:5]],
            "more": max(0, len(cand.pairs) - 5),
            "settled": activity.settled,
            "days_since_newest": activity.days_since_newest,
        })

    ready = [r for r in rows if r["settled"]]
    deferred = [r for r in rows if not r["settled"]]
    return {
        "ok": True,
        "threshold_days": threshold,
        "folder": folder,
        "ready": ready,
        "deferred": deferred,
        "total_ready_pairs": sum(r["pairs"] for r in ready),
        "total_deferred_pairs": sum(r["pairs"] for r in deferred),
    }


def _reorganize_run(api: ApiServer, body: dict) -> dict:
    """
    Kick off a reorganize sweep in a background thread. Status is polled via
    /api/reorganize/status until finished=true.
    """
    from .dropbox_client import make_client_from_config
    from .reorganize import find_unreorganized_pairs, is_folder_settled, reorganize_pair

    with api._reorganize_lock:
        if api._reorganize_run is not None and not api._reorganize_run.finished:
            return {"ok": False, "error": "a reorganize run is already in progress"}

        threshold = int(body.get("min_age_days", api.config.legacy_reorganize_min_age_days))
        folder = body.get("folder") or api.config.dropbox_root

        run = ReorganizeRun(
            started_at=time.time(),
            threshold_days=threshold,
            folder_filter=folder,
            total_pairs=0,
        )
        api._reorganize_run = run

    def worker() -> None:
        try:
            dropbox = make_client_from_config(api.config)
            run.push(f"Scanning {folder} for unreorganized pairs...")
            candidates = find_unreorganized_pairs(dropbox, folder)
            ready = []
            for cand in candidates:
                activity = is_folder_settled(dropbox, cand.parent, threshold)
                if activity.settled:
                    ready.append(cand)
                else:
                    run.skipped_folders += 1
            run.total_pairs = sum(len(c.pairs) for c in ready)
            run.push(f"Found {run.total_pairs} pair(s) in {len(ready)} settled folder(s); "
                     f"{run.skipped_folders} active folder(s) deferred.")

            for cand in ready:
                run.push(f"--- {cand.parent} ({len(cand.pairs)} pairs) ---")
                for pair in cand.pairs:
                    run.current = f"{cand.parent}/{pair.name}"
                    try:
                        reorganize_pair(
                            dropbox, cand.parent, pair.name,
                            int(pair.original.size or 0),
                            int(pair.h265.size or 0),
                        )
                        run.push(f"  + {pair.name}")
                        run.done_pairs += 1
                    except Exception as e:
                        run.push(f"  x {pair.name}: {e}")
                        run.failed_pairs += 1

            run.current = ""
            run.push(f"Done. {run.done_pairs} reorganized, {run.failed_pairs} failed.")
        except Exception as e:
            logger.exception("reorganize-existing run crashed")
            run.error = str(e)
            run.push(f"ERROR: {e}")
        finally:
            run.finished = True

    threading.Thread(target=worker, name="reorganize-runner", daemon=True).start()
    return {"ok": True, "run": run.to_dict()}


def _dropbox_list_payload(api: ApiServer, qs: dict[str, list[str]]) -> dict:
    """List Dropbox folders/files at a path so the dashboard can let the user
    browse instead of typing dropbox_root by hand. Falls back to the parent
    folder when the requested path doesn't exist, so a typo'd config still
    surfaces something useful."""
    from pathlib import PurePosixPath
    from .dropbox_client import (
        make_client_from_config,
        DropboxNotFoundError,
        DropboxAuthError,
    )

    requested = (qs.get("path") or [""])[0] or "/"
    if not api.config.has_dropbox_auth():
        return {"ok": False, "error": "dropbox auth not configured. Run hd auth."}

    try:
        dropbox = make_client_from_config(api.config)
    except DropboxAuthError as e:
        return {"ok": False, "error": str(e)}

    def _list(p: str) -> list[dict]:
        return dropbox.list_subfolders(p)

    try:
        entries = _list(requested)
        return {
            "ok": True,
            "path": requested,
            "entries": entries,
            "fallback": False,
            "namespace": dropbox.namespace,
        }
    except DropboxNotFoundError:
        # Try the closest existing parent so the user sees neighbors and can
        # spot the right name.
        candidate = requested.rstrip("/")
        while candidate and candidate != "/":
            candidate = str(PurePosixPath(candidate).parent)
            if candidate == ".":
                candidate = "/"
            try:
                entries = _list(candidate)
                return {
                    "ok": True,
                    "path": candidate,
                    "entries": entries,
                    "fallback": True,
                    "missing": requested,
                    "namespace": dropbox.namespace,
                }
            except DropboxNotFoundError:
                continue
        # Last resort: list the account root
        entries = _list("/")
        return {
            "ok": True,
            "path": "/",
            "entries": entries,
            "fallback": True,
            "missing": requested,
            "namespace": dropbox.namespace,
        }
    except Exception as e:
        return {"ok": False, "error": str(e), "path": requested}


def _reorganize_status_payload(api: ApiServer) -> dict:
    run = api._reorganize_run
    return {"ok": True, "run": run.to_dict() if run else None}


def _human_duration(seconds: float) -> str:
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    mins, sec = divmod(seconds, 60)
    if mins < 60:
        return f"{mins}m {sec}s"
    hours, mins = divmod(mins, 60)
    if hours < 24:
        return f"{hours}h {mins}m"
    days, hours = divmod(hours, 24)
    return f"{days}d {hours}h"
