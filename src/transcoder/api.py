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

# PWA assets shipped alongside the dashboard. Read once at import time;
# they're tiny and never change at runtime.
_PKG_DIR = Path(__file__).parent
PWA_MANIFEST = (_PKG_DIR / "manifest.json").read_bytes()
PWA_ICON_SVG = (_PKG_DIR / "icon.svg").read_bytes()
PWA_APPLE_ICON = (_PKG_DIR / "apple-touch-icon.png").read_bytes()


# Task Scheduler task name registered by installer/tasks/HeavyDropsDaemon.xml.
# `/api/restart` shells out to schtasks against this name; matches the manual
# update one-liner the operator uses (`schtasks /End` then `/Run`).
_SCHEDULED_TASK_NAME = "HeavyDropsDaemon"


def _trigger_task_scheduler_restart(gap_sec: int = 10) -> None:
    """Spawn a detached helper that re-launches the daemon via Task Scheduler.

    Sequence the helper runs:
      1. ~2s pause: let the daemon's os._exit complete and Task Scheduler
         mark the task as Ready.
      2. `schtasks /End`: belt-and-braces — kills any orphan ffmpeg.exe or
         python.exe that might have survived from this task instance.
      3. `gap_sec` pause: this is the critical wait. The OS needs time to
         release the API port (9123) and finalize the ffmpeg child cleanup
         before a fresh instance binds. Without it the new daemon races
         on bind and refuses to come up — running /Run twice manually is
         the symptom we're fixing.
      4. `schtasks /Run`: starts a fresh instance via Task Scheduler.

    Critical: the helper must escape the Job Object that Task Scheduler put
    this process into, otherwise `schtasks /End` would terminate the helper
    too. CREATE_BREAKAWAY_FROM_JOB + DETACHED_PROCESS does that on Windows.

    No-op on non-Windows (development). If the scheduled task isn't
    registered (e.g. operator runs the daemon by hand), `/End` and `/Run`
    will fail silently and the daemon just exits — same UX as before.
    """
    import subprocess
    import sys

    if sys.platform != "win32":
        logger.info("restart trigger skipped (not Windows)")
        return

    DETACHED_PROCESS = 0x00000008
    CREATE_NEW_PROCESS_GROUP = 0x00000200
    CREATE_BREAKAWAY_FROM_JOB = 0x01000000

    cmd = (
        "Start-Sleep -Seconds 2; "
        f"schtasks /End /TN {_SCHEDULED_TASK_NAME} | Out-Null; "
        f"Start-Sleep -Seconds {int(gap_sec)}; "
        f"schtasks /Run /TN {_SCHEDULED_TASK_NAME} | Out-Null"
    )

    try:
        subprocess.Popen(
            ["powershell", "-NoProfile", "-WindowStyle", "Hidden", "-Command", cmd],
            creationflags=DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_BREAKAWAY_FROM_JOB,
            close_fds=True,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            cwd="C:\\",
        )
        logger.info(f"restart helper spawned: end+{gap_sec}s+run {_SCHEDULED_TASK_NAME}")
    except Exception as e:
        logger.error(f"failed to spawn restart helper: {e}")


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
        # Dropbox storage usage cache. Hitting users_get_space_usage on
        # every dashboard refresh is wasteful; the value moves slowly so
        # a 5min TTL is plenty.
        self._storage_cache: dict | None = None
        self._storage_cache_at: float = 0.0
        self._storage_cache_ttl_sec: float = 300.0
        self._storage_cache_lock = threading.Lock()

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

        # Allow do_GET / do_POST to inject Set-Cookie on the next response.
        _set_cookie_value: str | None = None

        # ---------------- auth ----------------

        def _is_authorized(self, route: str, qs: dict[str, list[str]]) -> bool:
            """Token check. No-op when no token is configured (local-only
            mode). When a token is set, every route except /healthz needs
            one of: Bearer header / ?token=X / hd_token cookie."""
            token = (api.config.api.access_token or "").strip()
            if not token:
                return True
            if route == "/healthz":
                return True
            # Bearer header
            auth = self.headers.get("Authorization", "")
            if auth == f"Bearer {token}":
                return True
            # Query param (also stamps a cookie so the user doesn't need to
            # keep ?token=X in the URL after the first visit).
            given = (qs.get("token") or [""])[0]
            if given and given == token:
                self._set_cookie_value = token
                return True
            # Cookie
            cookies = self.headers.get("Cookie", "") or ""
            for crumb in cookies.split(";"):
                k, _, v = crumb.strip().partition("=")
                if k == "hd_token" and v == token:
                    return True
            return False

        def _send_unauthorized(self) -> None:
            """Tiny HTML page with a token paste box. Survives GET/POST."""
            body = (
                b"<!doctype html><meta charset=utf-8>"
                b"<title>HeavyDrops \xe2\x80\x94 token required</title>"
                b"<style>body{font:14px system-ui;margin:3em auto;max-width:420px;background:#0c0d10;color:#e6e8ee}"
                b"input{font:inherit;padding:.5em;width:100%;background:#16181d;color:#eee;border:1px solid #2a2e36;border-radius:5px}"
                b"button{margin-top:.6em;padding:.5em 1em;background:#6cb6ff;border:0;border-radius:5px;cursor:pointer;color:#000;font-weight:600}"
                b"</style>"
                b"<h2>Access token required</h2>"
                b"<p>This daemon is bound to a non-local address. Paste the access token from <code>config.yaml</code> "
                b"(<code>api.access_token</code>) to continue.</p>"
                b"<form method=GET>"
                b"<input name=token autofocus placeholder='access token'>"
                b"<button type=submit>Sign in</button>"
                b"</form>"
            )
            self.send_response(401)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        # ---------------- routing ----------------

        def do_GET(self) -> None:  # noqa: N802 (stdlib naming)
            route, qs = _split(self.path)
            # PWA static assets (manifest, icons) are served WITHOUT auth so
            # the iOS "Add to Home Screen" flow can fetch the icon during
            # installation even before the user pastes the token. They're
            # public, non-sensitive metadata.
            if route == "/manifest.json":
                return self._send_bytes(PWA_MANIFEST, "application/manifest+json")
            if route == "/icon.svg":
                return self._send_bytes(PWA_ICON_SVG, "image/svg+xml")
            if route in ("/apple-touch-icon.png", "/apple-touch-icon-precomposed.png", "/favicon.ico"):
                return self._send_bytes(PWA_APPLE_ICON, "image/png")

            if not self._is_authorized(route, qs):
                return self._send_unauthorized()
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
            if route == "/api/health":
                return self._send_json(_health_payload(api))
            if route == "/api/census-tree":
                return self._send_json(_census_tree_payload(api))
            if route == "/api/census-status":
                return self._send_json(_census_status_payload(api))
            if route == "/api/projection":
                return self._send_json(_projection_payload(api))
            if route == "/api/deep-scan/status":
                return self._send_json(_deep_scan_status_payload(api))
            if route == "/healthz":
                return self._send_text("ok")
            self.send_error(404, "not found")

        def do_POST(self) -> None:  # noqa: N802
            route, qs = _split(self.path)
            if not self._is_authorized(route, qs):
                return self._send_unauthorized()
            if route == "/api/pause":
                api.dispatcher.pause()
                return self._send_json({"ok": True, "paused": True})
            if route == "/api/resume":
                api.dispatcher.resume()
                return self._send_json({"ok": True, "paused": False})
            if route == "/api/scan-now":
                api.scan_trigger.set()
                return self._send_json({"ok": True, "triggered": True})
            if route == "/api/health/run-now":
                agent = getattr(getattr(api, "daemon", None), "self_health", None)
                if agent is None:
                    return self._send_json({"ok": False, "error": "self-health agent not running"})
                agent.trigger_now()
                return self._send_json({"ok": True, "triggered": True})
            if route == "/api/kill-ffmpeg":
                killed = _kill_all_ffmpeg()
                return self._send_json({"ok": True, "killed": killed})
            if route == "/api/retry-failed":
                count = api.db.reset_failed_jobs()
                return self._send_json({"ok": True, "reset": count})
            if route == "/api/cleanup-dotunderscore-now":
                # Manual one-shot sweep across the configured target folders.
                # Useful for cleaning up the backlog of ._ files that arrived
                # before the periodic sweep was wired up, or after the user
                # bulk-uploads from a Mac mid-batch.
                from .dropbox_client import make_client_from_config
                from .reorganize import sweep_dot_underscore_under_root
                cfg = api.config
                try:
                    dropbox = make_client_from_config(cfg)
                    results = sweep_dot_underscore_under_root(
                        dropbox,
                        cfg.dropbox_root,
                        cfg.cleanup_dot_underscore_delete_after_seconds,
                        cfg.dot_underscore_target_folder_names,
                        max_size_bytes=cfg.dot_underscore_max_size_bytes,
                    )
                    total = sum(results.values()) if results else 0
                    return self._send_json({
                        "ok": True,
                        "folders_touched": len(results),
                        "files_quarantined": total,
                        "details": results,
                    })
                except Exception as e:
                    return self._send_json({"ok": False, "error": str(e)})
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
            if route == "/api/census-now":
                worker = getattr(getattr(api, "daemon", None), "census_worker", None)
                if worker is None:
                    return self._send_json({"ok": False, "error": "census worker disabled in config"})
                worker.trigger_now()
                return self._send_json({"ok": True, "triggered": True})
            if route == "/api/deep-scan/start":
                ds = getattr(getattr(api, "daemon", None), "deep_scan", None)
                if ds is None:
                    return self._send_json({"ok": False, "error": "deep-scan worker not initialized"})
                started = ds.start()
                return self._send_json({"ok": True, "started": started, "already_running": not started})
            if route == "/api/deep-scan/cancel":
                ds = getattr(getattr(api, "daemon", None), "deep_scan", None)
                if ds is None:
                    return self._send_json({"ok": False, "error": "deep-scan worker not initialized"})
                ds.cancel()
                return self._send_json({"ok": True, "cancelled": True})
            if route == "/api/restart":
                # Spawn a detached helper (End + 10s gap + Run) before
                # exiting cleanly. The gap is between End and Run because
                # that's when the API port and ffmpeg children actually
                # need to clear — restarting too fast races on the bind.
                # Helper escapes the Task Scheduler job object so /End
                # doesn't kill it.
                _trigger_task_scheduler_restart(gap_sec=10)
                threading.Timer(1.5, lambda: os._exit(0)).start()
                return self._send_json({"ok": True, "restarting_in_sec": 14})
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
        def _stamp_token_cookie(self) -> None:
            """If a query-param login just happened, send a cookie so the
            user doesn't need ?token=X in every URL."""
            if self._set_cookie_value:
                # 30-day cookie, HttpOnly so JS can't read it. Path=/ so it
                # covers /api/* too. SameSite=Lax keeps it from being sent
                # on cross-site requests.
                self.send_header(
                    "Set-Cookie",
                    f"hd_token={self._set_cookie_value}; Path=/; Max-Age=2592000; "
                    f"HttpOnly; SameSite=Lax",
                )
                self._set_cookie_value = None

        def _no_cache_headers(self) -> None:
            # Without these the browser happily serves a stale dashboard.html
            # (or stale /api/* JSON) for hours after a daemon update, and the
            # user keeps seeing the OLD UI even though the version banner
            # says new. Belt-and-braces directives that work across browsers.
            self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
            self.send_header("Pragma", "no-cache")
            self.send_header("Expires", "0")

        def _send_json(self, payload: dict) -> None:
            body = json.dumps(payload, default=str).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self._no_cache_headers()
            self._stamp_token_cookie()
            self.end_headers()
            self.wfile.write(body)

        def _send_text(self, text: str) -> None:
            body = text.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self._no_cache_headers()
            self._stamp_token_cookie()
            self.end_headers()
            self.wfile.write(body)

        def _send_html(self, html: str) -> None:
            body = html.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self._no_cache_headers()
            self._stamp_token_cookie()
            self.end_headers()
            self.wfile.write(body)

        def _send_bytes(self, body: bytes, content_type: str) -> None:
            """Static asset response (manifest, PWA icons, favicon).
            Cacheable for 1h since these assets are tied to the daemon
            version — a fresh install/upgrade will see them via the
            no-cache headers on the dashboard HTML."""
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "public, max-age=3600")
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
    dropbox_storage = _dropbox_storage_snapshot(api)

    uptime_sec = max(0.0, time.time() - api.started_at_epoch)
    state_counts = stats.get("state_counts", {})

    return {
        "ok": True,
        "pid": os.getpid(),
        "uptime_sec": uptime_sec,
        "uptime_human": _human_duration(uptime_sec),
        "api": {
            "bind": api.config.api.bind,
            "port": api.config.api.port,
            "lan_accessible": api.config.api.bind not in ("127.0.0.1", "localhost", "::1"),
            "auth_required": bool((api.config.api.access_token or "").strip()),
            # The token is needed to assemble the share URL. Only surfaced
            # to authorized callers — by the time they're hitting this
            # payload they've already passed the auth check.
            "access_token": api.config.api.access_token or None,
            "lan_addresses": _enumerate_lan_addresses(api.config.api.port),
        },
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
        "dropbox_storage": dropbox_storage,
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


def _dropbox_storage_snapshot(api: ApiServer) -> dict | None:
    """Cached Dropbox space usage. None when daemon has no Dropbox client
    or the call failed. Cache TTL is 5 min — usage moves slowly enough
    that hammering Dropbox on every dashboard refresh would be wasteful.
    """
    target_tb = float(getattr(api.config, "storage_target_tb", 0.0) or 0.0)
    target_bytes = int(target_tb * (1024 ** 4))

    now = time.time()
    with api._storage_cache_lock:
        cached = api._storage_cache
        cache_age = now - api._storage_cache_at
        if cached is not None and cache_age < api._storage_cache_ttl_sec:
            return {
                **cached,
                "target_bytes": target_bytes,
                "target_tb": target_tb,
                "cached_at": api._storage_cache_at,
            }

    dropbox = getattr(getattr(api, "daemon", None), "dropbox", None)
    if dropbox is None:
        return None
    try:
        usage = dropbox.get_space_usage()
    except Exception as e:
        logger.warning(f"dropbox_storage: get_space_usage failed: {e}")
        return None

    snap = {
        "used_bytes": int(usage.get("used") or 0),
        "allocated_bytes": int(usage.get("allocated") or 0) or None,
        "team_used_bytes": int(usage.get("team_used") or 0) or None,
        "allocation_type": usage.get("allocation_type"),
    }
    with api._storage_cache_lock:
        api._storage_cache = snap
        api._storage_cache_at = now

    return {
        **snap,
        "target_bytes": target_bytes,
        "target_tb": target_tb,
        "cached_at": now,
    }


def _disk_snapshot(api: ApiServer) -> dict:
    try:
        staging = Path(api.config.local_staging_dir)
        usage = shutil.disk_usage(staging) if staging.exists() else None
    except OSError:
        usage = None

    return {
        "staging_dir": str(api.config.local_staging_dir),
        "output_dir": str(api.config.local_output_dir),
        "log_dir": str(api.config.log_dir),
        "database_path": str(api.config.database_path),
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
    "legacy_reorganize_delete_h264_after_seconds": {
        "type": "int",
        "min": 0,
        "max": 86400,
        "yaml_key": "legacy_reorganize_delete_h264_after_seconds",
        "label": "Delete /h264 backup folder N seconds after reorganize (0 = keep)",
    },
    "legacy_reorganize_delete_wav_after_seconds": {
        "type": "int",
        "min": 0,
        "max": 86400,
        "yaml_key": "legacy_reorganize_delete_wav_after_seconds",
        "label": "Delete /wav backup folder N seconds after audio reorganize (0 = keep)",
    },
    "audio_enabled": {
        "type": "bool",
        "yaml_key": "audio.enabled",
        "label": "Convert WAV → MP3 192k inside 'Audio Source Files' folders (CPU, parallel to QSV)",
    },
    "audio_workers": {
        "type": "int",
        "min": 1,
        "max": 8,
        "yaml_key": "concurrency.audio_workers",
        "label": "Parallel audio (libmp3lame) workers",
    },
    "preserve_chroma_422": {
        "type": "bool",
        "yaml_key": "preserve_chroma_422",
        "label": "Preserve Chroma 4:2:2 (forces libx265 — ~10x slower)",
    },
    "low_bitrate_skip_mbps_per_megapixel": {
        "type": "float",
        "min": 0.0,
        "max": 50.0,
        "yaml_key": "low_bitrate_skip_mbps_per_megapixel",
        "label": "Skip files with input bitrate below N Mbps per megapixel (0 = disable)",
    },
    "cleanup_dot_underscore": {
        "type": "bool",
        "yaml_key": "cleanup_dot_underscore",
        "label": "Sweep ._ macOS resource forks into 'ponto tracinho/' after each reorganize batch",
    },
    "cleanup_dot_underscore_delete_after_seconds": {
        "type": "int",
        "min": 0,
        "max": 86400,
        "yaml_key": "cleanup_dot_underscore_delete_after_seconds",
        "label": "Delete 'ponto tracinho/' quarantine after N seconds (0 = keep)",
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
    "health_check_interval_minutes": {
        "type": "int",
        "min": 1,
        "max": 1440,
        "yaml_key": "incidents.health_check_interval_minutes",
        "label": "Self-health agent: minutes between checks",
    },
    "api_bind": {
        "type": "bind",
        "yaml_key": "api.bind",
        "label": "Dashboard access (127.0.0.1 = local only, 0.0.0.0 = LAN)",
    },
    "transcode_workers": {
        "type": "int",
        "min": 1,
        "max": 8,
        "yaml_key": "concurrency.transcode_workers",
        "label": "Parallel transcodes (1 = max QSV speed; >1 splits the encoder)",
    },
    "download_workers": {
        "type": "int",
        "min": 1,
        "max": 8,
        "yaml_key": "concurrency.download_workers",
        "label": "Parallel downloads from Dropbox",
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
            "legacy_reorganize_delete_h264_after_seconds": cfg.legacy_reorganize_delete_h264_after_seconds,
            "legacy_reorganize_delete_wav_after_seconds": cfg.legacy_reorganize_delete_wav_after_seconds,
            "audio_enabled": cfg.audio.enabled,
            "audio_workers": cfg.concurrency.audio_workers,
            "preserve_chroma_422": cfg.preserve_chroma_422,
            "low_bitrate_skip_mbps_per_megapixel": cfg.low_bitrate_skip_mbps_per_megapixel,
            "cleanup_dot_underscore": cfg.cleanup_dot_underscore,
            "cleanup_dot_underscore_delete_after_seconds": cfg.cleanup_dot_underscore_delete_after_seconds,
            "dropbox_root": cfg.dropbox_root,
            "health_check_interval_minutes": cfg.incidents.health_check_interval_minutes,
            "api_bind": cfg.api.bind,
            "transcode_workers": cfg.concurrency.transcode_workers,
            "download_workers": cfg.concurrency.download_workers,
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
        elif knob["type"] == "bind":
            value = str(raw or "").strip()
            if value not in ("127.0.0.1", "0.0.0.0"):
                raise ValueError(f"{key} must be '127.0.0.1' or '0.0.0.0'")
        else:
            raise ValueError(f"unsupported type for {key}")

        # Mutate in-memory config. Nested keys (e.g. "incidents.X") walk
        # the dotted path; flat keys are direct attributes on Config.
        knob = _SETTINGS_KNOBS[key]
        yaml_key = knob["yaml_key"]
        if "." in yaml_key:
            parent, leaf = yaml_key.rsplit(".", 1)
            sub = cfg
            for part in parent.split("."):
                sub = getattr(sub, part)
            setattr(sub, leaf, value)
        else:
            setattr(cfg, key, value)
        updated.append(key)

    # Persist to config.yaml using regex line replacement so comments survive.
    raw = Path(cfg_path).read_text(encoding="utf-8")
    for key in updated:
        knob = _SETTINGS_KNOBS[key]
        yaml_key = knob["yaml_key"]
        if "." in yaml_key:
            parent, leaf = yaml_key.rsplit(".", 1)
            sub = cfg
            for part in parent.split("."):
                sub = getattr(sub, part)
            new_value = getattr(sub, leaf)
        else:
            new_value = getattr(cfg, key)
        if isinstance(new_value, bool):
            yaml_val = "true" if new_value else "false"
        else:
            yaml_val = str(new_value)
        if "." in yaml_key:
            # Nested key: rewrite the matching `  leaf: ...` line within
            # the parent block. Indented 2 spaces per the YAML the rest of
            # the codebase emits.
            parent, leaf = yaml_key.rsplit(".", 1)
            line = f"  {leaf}: {yaml_val}"
            # Find the parent block and patch the leaf inside it.
            block_pat = re.compile(
                rf'(^{re.escape(parent)}:\s*\n(?: {{2}}.*\n)*)',
                re.MULTILINE,
            )
            m = block_pat.search(raw)
            if m:
                block = m.group(1)
                leaf_pat = re.compile(rf'^ {{2}}{re.escape(leaf)}\s*:.*$', re.MULTILINE)
                if leaf_pat.search(block):
                    new_block = leaf_pat.sub(line, block, count=1)
                else:
                    new_block = block.rstrip("\n") + "\n" + line + "\n"
                raw = raw.replace(block, new_block, 1)
            else:
                # Parent block missing — append a fresh one.
                if raw and not raw.endswith("\n"):
                    raw += "\n"
                raw += f"{parent}:\n{line}\n"
        else:
            pattern = re.compile(rf'^{re.escape(yaml_key)}\s*:.*$', re.MULTILINE)
            line = f"{yaml_key}: {yaml_val}"
            if pattern.search(raw):
                raw = pattern.sub(line, raw, count=1)
            else:
                if raw and not raw.endswith("\n"):
                    raw += "\n"
                raw += line + "\n"
    Path(cfg_path).write_text(raw, encoding="utf-8")

    # Live-apply: a few settings affect long-lived components (the
    # self-health agent's poll interval, etc.) and would otherwise need a
    # daemon restart to take effect. Push them through here so saving in
    # the dashboard reflects within seconds.
    if "health_check_interval_minutes" in updated:
        agent = getattr(getattr(api, "daemon", None), "self_health", None)
        if agent is not None:
            agent.interval_sec = max(60, int(cfg.incidents.health_check_interval_minutes) * 60)
            logger.info(f"self-health interval updated live to {agent.interval_sec}s")

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
    settled/active classification, considering BOTH layouts (video h264/h265
    AND audio wav/mp3). Mirrors the main pipeline's discovery scope so the
    retroactive sweep has the same coverage as live processing.
    Cheap enough to call inline (no Dropbox moves performed).
    """
    from .dropbox_client import make_client_from_config
    from .reorganize import (
        AUDIO_LAYOUT, VIDEO_LAYOUT,
        find_unreorganized_pairs, is_folder_settled,
    )

    threshold = int(body.get("min_age_days", api.config.legacy_reorganize_min_age_days))
    folder = body.get("folder") or api.config.dropbox_root

    dropbox = make_client_from_config(api.config)
    # Run discovery for each layout; merge by parent so the same folder
    # with both video AND audio pending appears once with combined counts.
    video_cands = find_unreorganized_pairs(dropbox, folder, VIDEO_LAYOUT)
    audio_cands = find_unreorganized_pairs(dropbox, folder, AUDIO_LAYOUT)

    by_parent: dict[str, dict] = {}
    for cand in video_cands:
        by_parent.setdefault(cand.parent, {"video_pairs": [], "audio_pairs": []})["video_pairs"] = cand.pairs
    for cand in audio_cands:
        by_parent.setdefault(cand.parent, {"video_pairs": [], "audio_pairs": []})["audio_pairs"] = cand.pairs

    rows = []
    for parent, slots in by_parent.items():
        activity = is_folder_settled(dropbox, parent, threshold)
        v_pairs = slots["video_pairs"]
        a_pairs = slots["audio_pairs"]
        total = len(v_pairs) + len(a_pairs)
        sample = [p.name for p in (v_pairs + a_pairs)[:5]]
        rows.append({
            "parent": parent,
            "pairs": total,
            "video_pairs": len(v_pairs),
            "audio_pairs": len(a_pairs),
            "names": sample,
            "more": max(0, total - 5),
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

    This mirrors the main pipeline's per-folder reorganize behavior:
      - Discovers BOTH video (h264/h265) AND audio (wav/mp3) pairs
      - For each settled folder, reorganizes every pending pair (both layouts)
      - Updates job DB output_path when a matching job exists
      - After a fully-successful batch in a folder, schedules backup folder
        cleanup (h264/ or wav/) honoring the configured delays
      - Sweeps `._` macOS resource forks in the same folder

    The intentional difference from the main pipeline is the gate:
      - Main: is_folder_complete (DB-driven; every job in folder is terminal)
      - Retroactive: just is_folder_settled (these folders may pre-date the
        DB; checking job-completeness would skip them all)
    """
    from .database import JobState
    from .dropbox_client import make_client_from_config
    from .reorganize import (
        AUDIO_LAYOUT, VIDEO_LAYOUT,
        _audio_successor_name, _video_successor_name,
        cleanup_dot_underscore_files,
        find_unreorganized_pairs,
        is_folder_settled,
        reorganize_pair,
        schedule_h264_delete,
    )

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
            cfg = api.config

            run.push(f"Scanning {folder} for unreorganized pairs (video + audio)...")
            video_cands = find_unreorganized_pairs(dropbox, folder, VIDEO_LAYOUT)
            audio_cands = find_unreorganized_pairs(dropbox, folder, AUDIO_LAYOUT)

            # Group by parent so each folder's batch is processed atomically
            # — same way the main pipeline keeps per-folder reorganize +
            # cleanup tightly coupled.
            grouped: dict[str, dict] = {}
            for cand in video_cands:
                grouped.setdefault(cand.parent, {"video": [], "audio": []})["video"] = cand.pairs
            for cand in audio_cands:
                grouped.setdefault(cand.parent, {"video": [], "audio": []})["audio"] = cand.pairs

            ready_parents: list[tuple[str, dict]] = []
            for parent, slots in grouped.items():
                activity = is_folder_settled(dropbox, parent, threshold)
                if activity.settled:
                    ready_parents.append((parent, slots))
                else:
                    run.skipped_folders += 1

            run.total_pairs = sum(
                len(s["video"]) + len(s["audio"]) for _, s in ready_parents
            )
            run.push(
                f"Found {run.total_pairs} pair(s) in {len(ready_parents)} settled folder(s); "
                f"{run.skipped_folders} active folder(s) deferred."
            )

            for parent, slots in ready_parents:
                v_pairs = slots["video"]
                a_pairs = slots["audio"]
                total_in_folder = len(v_pairs) + len(a_pairs)
                run.push(
                    f"--- {parent} ({len(v_pairs)} video + {len(a_pairs)} audio) ---"
                )

                # Track per-layout success so we only schedule the backup
                # cleanup when the whole layout's batch landed in this folder.
                video_done_in_folder = 0
                audio_done_in_folder = 0

                for layout, pairs, label in (
                    (VIDEO_LAYOUT, v_pairs, "video"),
                    (AUDIO_LAYOUT, a_pairs, "audio"),
                ):
                    for pair in pairs:
                        run.current = f"{parent}/{pair.name}"
                        try:
                            new_path = reorganize_pair(
                                dropbox, parent, pair.name,
                                int(pair.original.size or 0),
                                int(pair.h265.size or 0),
                                layout=layout,
                            )
                            run.push(f"  + {pair.name} ({label})")
                            run.done_pairs += 1
                            if label == "video":
                                video_done_in_folder += 1
                            else:
                                audio_done_in_folder += 1

                            # Best-effort: bring the matching DB job's
                            # output_path in line with the new canonical
                            # location. Ignored when no DB row exists
                            # (these are usually retroactive sweeps over
                            # files predating the daemon).
                            try:
                                original_path = (
                                    parent.rstrip('/') + '/' + pair.name
                                ) if parent else '/' + pair.name
                                related = api.db.get_job_by_path(original_path)
                                if related is not None:
                                    api.db.update_job_state(
                                        related.id, JobState.DONE,
                                        output_path=new_path,
                                    )
                            except Exception:
                                pass
                        except Exception as e:
                            run.push(f"  x {pair.name} ({label}): {e}")
                            run.failed_pairs += 1

                # Schedule backup-folder cleanup only when the WHOLE
                # layout batch in this folder landed (mirror of the main
                # pipeline's safety check).
                if v_pairs and video_done_in_folder == len(v_pairs):
                    delay = cfg.legacy_reorganize_delete_h264_after_seconds
                    if delay > 0:
                        h264_dir = (parent.rstrip('/') + '/h264') if parent else '/h264'
                        run.push(
                            f"  · scheduling h264 cleanup in {delay}s "
                            f"(folder kept; audit log inside; "
                            f"successor-existence check active)"
                        )
                        schedule_h264_delete(
                            dropbox, h264_dir, delay,
                            successor_resolver=_video_successor_name,
                        )

                if a_pairs and audio_done_in_folder == len(a_pairs):
                    delay = cfg.legacy_reorganize_delete_wav_after_seconds
                    if delay > 0:
                        wav_dir = (parent.rstrip('/') + '/wav') if parent else '/wav'
                        run.push(
                            f"  · scheduling wav cleanup in {delay}s "
                            f"(successor-existence check active)"
                        )
                        schedule_h264_delete(
                            dropbox, wav_dir, delay,
                            successor_resolver=_audio_successor_name,
                        )

                # Sweep ._ resource forks in this folder if cleanup is
                # enabled — same housekeeping the main pipeline does after
                # each batch. Best-effort, won't fail the run.
                if (
                    cfg.cleanup_dot_underscore
                    and (video_done_in_folder + audio_done_in_folder) == total_in_folder
                ):
                    try:
                        cleaned = cleanup_dot_underscore_files(
                            dropbox,
                            parent,
                            cfg.cleanup_dot_underscore_delete_after_seconds,
                            target_folder_names=cfg.dot_underscore_target_folder_names,
                            max_size_bytes=cfg.dot_underscore_max_size_bytes,
                        )
                        if cleaned > 0:
                            run.push(f"  · quarantined {cleaned} ._ file(s)")
                    except Exception as e:
                        run.push(f"  · ._ sweep failed: {e}")

            run.current = ""
            run.push(
                f"Done. {run.done_pairs} reorganized, {run.failed_pairs} failed."
            )
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


def _health_payload(api: ApiServer) -> dict:
    """Self-health agent state for the dashboard."""
    agent = getattr(getattr(api, "daemon", None), "self_health", None)
    if agent is None:
        return {"ok": True, "agent": None}
    return {"ok": True, "agent": agent.status()}


def _reorganize_status_payload(api: ApiServer) -> dict:
    run = api._reorganize_run
    return {"ok": True, "run": run.to_dict() if run else None}


def _enumerate_lan_addresses(port: int) -> list[str]:
    """Enumerate the IPv4 LAN addresses for this host so the dashboard can
    show 'http://192.168.x.y:9123/' to copy on another device. Filters out
    loopback. Returns a list, possibly empty if the host has no usable
    LAN-facing NIC."""
    import socket
    addrs: list[str] = []
    try:
        hostname = socket.gethostname()
        # getaddrinfo returns IPv4+IPv6; we filter to AF_INET (v4) since
        # the average user types LAN URLs in v4 form.
        for info in socket.getaddrinfo(hostname, None, socket.AF_INET):
            ip = info[4][0]
            if ip and not ip.startswith("127.") and ip not in addrs:
                addrs.append(ip)
    except OSError:
        pass
    # Build URLs
    return [f"http://{ip}:{port}/" for ip in addrs]


def _kill_all_ffmpeg() -> int:
    """Terminate every ffmpeg.exe process on the host.

    Used by the dashboard's 'Kill all ffmpeg' button and at daemon startup
    to clean up orphans left behind when the previous instance was killed
    abruptly (e.g. by Task Scheduler restart) — orphan ffmpeg.exe keep
    grinding on the GPU/CPU and starve the new daemon's transcodes.

    Cross-platform: uses taskkill on Windows, pkill/killall elsewhere.
    Returns the number of processes targeted (best-effort count).
    """
    import sys as _sys
    import subprocess as _sp
    if _sys.platform == "win32":
        try:
            r = _sp.run(
                ["taskkill", "/F", "/IM", "ffmpeg.exe"],
                capture_output=True, text=True, timeout=10,
            )
            # taskkill prints "SUCCESS: ..." per process killed
            return r.stdout.count("SUCCESS")
        except Exception as e:
            logger.warning(f"taskkill failed: {e}")
            return 0
    else:
        try:
            r = _sp.run(
                ["pkill", "-9", "-x", "ffmpeg"],
                capture_output=True, timeout=5,
            )
            # pkill returncode 0 = killed something, 1 = nothing matched
            return 1 if r.returncode == 0 else 0
        except FileNotFoundError:
            try:
                _sp.run(["killall", "-9", "ffmpeg"], capture_output=True, timeout=5)
                return 1
            except Exception:
                return 0
        except Exception as e:
            logger.warning(f"pkill failed: {e}")
            return 0


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


# =====================================================================
# Reduction-map census + deep-scan payloads
# =====================================================================


def _census_status_payload(api: ApiServer) -> dict:
    """Return the worker's current phase + last-run timestamp/totals."""
    worker = getattr(getattr(api, "daemon", None), "census_worker", None)
    last = api.db.get_last_census_run()
    out: dict = {
        "ok": True,
        "enabled": api.config.census.enabled,
        "daily_run_at": api.config.census.daily_run_at,
        "worker": worker.status() if worker is not None else None,
        "last_run": last,
    }
    return out


def _census_tree_payload(api: ApiServer) -> dict:
    """Build a hierarchical tree from folder_census flat rows.

    Each node carries its own counts/bytes (the folder itself) plus a
    rolled-up subtree count/bytes (sum of self + descendants), so the
    dashboard can color a parent by total backlog without doing the math
    in JS. Children are sorted by `pending_bytes` DESC so the largest
    backlog floats to the top.
    """
    rows = api.db.get_folder_census()
    last = api.db.get_last_census_run()
    if not rows:
        return {
            "ok": True,
            "last_run": last,
            "tree": None,
        }

    # Build a node per folder. Some intermediate folders may have no row
    # (because they contain only subfolders, no video files of their own).
    # We still need them in the tree so the user can navigate. Synthesize
    # empty rows on the fly.
    by_path: dict[str, dict] = {}
    for r in rows:
        node = _new_tree_node(r["path"])
        node.update({
            "self_pending_count": int(r["pending_count"]),
            "self_pending_bytes": int(r["pending_bytes"]),
            "self_done_count": int(r["done_count"]),
            "self_done_bytes": int(r["done_bytes"]),
            "self_ineligible_count": int(r["ineligible_count"]),
            "self_ineligible_bytes": int(r["ineligible_bytes"]),
        })
        by_path[r["path"]] = node

    # Synthesize ancestor folders so the tree is connected.
    root = api.config.dropbox_root.rstrip("/") or "/"
    for path in list(by_path.keys()):
        cur = path
        while cur and cur != root and cur != "/":
            parent = "/".join(cur.rstrip("/").split("/")[:-1]) or "/"
            if parent not in by_path:
                by_path[parent] = _new_tree_node(parent)
            cur = parent
    if root not in by_path:
        by_path[root] = _new_tree_node(root)

    # Wire children. Each node's parent gets the child appended.
    for path, node in by_path.items():
        if path == root or path == "/":
            continue
        parent = "/".join(path.rstrip("/").split("/")[:-1]) or "/"
        # Map any path that's not in by_path to root (shouldn't happen, but
        # belt-and-braces).
        parent_node = by_path.get(parent) or by_path[root]
        parent_node["children"].append(node)

    # Roll up subtree totals depth-first.
    def rollup(node: dict) -> None:
        for c in node["children"]:
            rollup(c)
        pending_c = node["self_pending_count"]
        pending_b = node["self_pending_bytes"]
        done_c = node["self_done_count"]
        done_b = node["self_done_bytes"]
        inel_c = node["self_ineligible_count"]
        inel_b = node["self_ineligible_bytes"]
        for c in node["children"]:
            pending_c += c["pending_count"]
            pending_b += c["pending_bytes"]
            done_c += c["done_count"]
            done_b += c["done_bytes"]
            inel_c += c["ineligible_count"]
            inel_b += c["ineligible_bytes"]
        node["pending_count"] = pending_c
        node["pending_bytes"] = pending_b
        node["done_count"] = done_c
        node["done_bytes"] = done_b
        node["ineligible_count"] = inel_c
        node["ineligible_bytes"] = inel_b
        # Largest backlog first so the user's eye lands on red folders.
        node["children"].sort(key=lambda x: -x["pending_bytes"])

    rollup(by_path[root])

    return {
        "ok": True,
        "last_run": last,
        "tree": by_path[root],
    }


def _new_tree_node(path: str) -> dict:
    return {
        "path": path,
        "name": path.rsplit("/", 1)[-1] or path,
        "self_pending_count": 0,
        "self_pending_bytes": 0,
        "self_done_count": 0,
        "self_done_bytes": 0,
        "self_ineligible_count": 0,
        "self_ineligible_bytes": 0,
        "pending_count": 0,
        "pending_bytes": 0,
        "done_count": 0,
        "done_bytes": 0,
        "ineligible_count": 0,
        "ineligible_bytes": 0,
        "children": [],
    }


def _deep_scan_status_payload(api: ApiServer) -> dict:
    ds = getattr(getattr(api, "daemon", None), "deep_scan", None)
    if ds is None:
        return {"ok": True, "status": None}
    return {"ok": True, "status": ds.status()}


def _projection_payload(api: ApiServer) -> dict:
    """Project the path from current pending → "watch folder fully converted".

    Surfaces:

      - Per-root progress bar fuel: done bytes vs total (under the current
        `dropbox_root` only — switching the watch folder naturally
        restarts the timeline because `path_prefix` filters out jobs from
        other roots).
      - ETA in days + calendar date, computed from a 7-day rolling window
        of completed jobs (recent throughput is more representative than
        all-time average — the daemon may have been idle for weeks before
        the operator turned it back on).
      - Reduction ratio + bucket breakdown (resolution proxy) so the
        operator can sanity-check the freed-bytes estimate.
    """
    from datetime import datetime, timedelta, timezone

    cfg = api.config
    root = (cfg.dropbox_root or "").rstrip("/") or "/"

    # All-time DONE jobs scoped to current watch folder. Keeps history
    # clean when the operator switches projects mid-stream.
    savings_root = api.db.get_savings_stats(path_prefix=root)
    # Last 7 days of activity → recent rate. Wall-clock days, not
    # transcode_seconds, because the operator wants real-time ETA
    # ("when will my Dropbox folder be done?") not "active CPU hours".
    seven_days_ago = datetime.now(timezone.utc) - timedelta(days=7)
    savings_recent = api.db.get_savings_stats(since=seven_days_ago, path_prefix=root)

    # Buckets unchanged — diagnostic only, doesn't filter by root.
    buckets = api.db.get_savings_stats_buckets()
    last_run = api.db.get_last_census_run()
    storage = _dropbox_storage_snapshot(api)

    # Pending bytes scoped to current watch folder. Census wipes
    # folder_census on every run, so when the operator just switched
    # dropbox_root and census hasn't run yet, this returns 0 and the UI
    # surfaces a "census stale for current root" hint instead of showing
    # ghost totals from the previous root.
    census_under_root = api.db.get_folder_census_totals(root)
    pending_bytes = int(census_under_root["pending_bytes"])
    pending_count = int(census_under_root["pending_count"])
    census_done_count = int(census_under_root["done_count"])

    # Detect "last census ran on a different root" — when the global last
    # run shows pending data but the per-root sum is zero. Lets the UI
    # prompt the operator to "Run census now" after a root switch.
    last_run_pending_global = int((last_run or {}).get("pending_total_bytes") or 0)
    census_stale_for_root = (
        last_run_pending_global > 0
        and pending_bytes == 0
        and census_under_root["matching_rows"] == 0
    )

    # All-time ratio for "freed bytes" estimate (uses every completed job
    # under the current root, not just recent ones — more samples = more
    # stable estimate for a one-shot projection).
    ratio_pct = float(savings_root.get("avg_reduction_pct") or 0.0)
    sample_jobs = int(savings_root.get("jobs") or 0)

    safe_ratio = max(0.0, min(99.0, ratio_pct))
    estimated_freed_bytes = int(pending_bytes * (safe_ratio / 100.0))
    estimated_output_bytes = pending_bytes - estimated_freed_bytes

    # ETA from recent throughput. Use INPUT bytes consumed (= bytes the
    # daemon processed off the pending pile) per real day. The denominator
    # is the SHORTER of 7 days or "time since first DONE under this root"
    # — without that bound, a daemon that just started yesterday and
    # processed 1 TB in 24h gets reported as 1/7 TB/day = 143 GB/day, and
    # the ETA inflates by 7×. Min span 12h so a half-day of activity
    # doesn't divide-by-near-zero into infinity.
    started_at = api.db.get_earliest_done_at(path_prefix=root)
    started_at_iso = started_at.isoformat() if started_at else None

    seven_days_sec = 7 * 86400
    if started_at and started_at > seven_days_ago:
        span_sec = (datetime.now(timezone.utc) - started_at).total_seconds()
    else:
        span_sec = seven_days_sec
    span_sec = max(43200.0, span_sec)  # floor at 12h
    bytes_per_day = (savings_recent.get("input_bytes") or 0) * 86400.0 / span_sec
    eta_days = None
    eta_at_iso = None
    if bytes_per_day > 0 and pending_bytes > 0:
        eta_days = pending_bytes / bytes_per_day
        eta_at = datetime.now(timezone.utc) + timedelta(days=eta_days)
        eta_at_iso = eta_at.isoformat()

    # Progress numbers for the bar.
    done_bytes_root = int(savings_root.get("input_bytes") or 0)
    total_bytes = done_bytes_root + pending_bytes
    progress_pct = (100.0 * done_bytes_root / total_bytes) if total_bytes > 0 else 0.0

    projection: dict = {
        "ok": True,
        "have_data": last_run is not None and sample_jobs > 0,
        "watch_folder": root,
        "census_stale_for_root": census_stale_for_root,
        "last_census_at": (last_run or {}).get("finished_at"),
        # Progress bar fuel
        "done_bytes": done_bytes_root,
        "pending_bytes": pending_bytes,
        "total_bytes": total_bytes,
        "progress_pct": round(progress_pct, 2),
        "done_jobs_root": int(savings_root.get("jobs") or 0),
        "pending_count": pending_count,
        "census_done_count": census_done_count,
        # ETA
        "started_at": started_at_iso,
        "eta_days": eta_days,
        "eta_at": eta_at_iso,
        "bytes_per_day_recent": int(bytes_per_day),
        "rate_window_days": round(span_sec / 86400.0, 2),
        "recent_jobs": int(savings_recent.get("jobs") or 0),
        "recent_input_bytes": int(savings_recent.get("input_bytes") or 0),
        # Reduction ratio (per-root)
        "global_ratio_pct": round(ratio_pct, 1),
        "global_sample_jobs": sample_jobs,
        "global_input_bytes": done_bytes_root,
        "global_output_bytes": int(savings_root.get("output_bytes") or 0),
        "estimated_freed_bytes": estimated_freed_bytes,
        "estimated_output_bytes": estimated_output_bytes,
        "buckets": buckets,
    }

    if storage is not None:
        used = int(storage.get("used_bytes") or 0)
        target = int(storage.get("target_bytes") or 0)
        projected_after_used = max(0, used - estimated_freed_bytes)
        projection.update({
            "current_used_bytes": used,
            "target_bytes": target,
            "projected_used_after_bytes": projected_after_used,
            "projected_distance_to_target_bytes": projected_after_used - target,
        })

    return projection
