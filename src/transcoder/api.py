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

from .database import Database, JobState
from .progress import REGISTRY as ACTIVITY
from .updater import read_status as read_update_status

if TYPE_CHECKING:
    from .config import Config
    from .dispatcher import JobDispatcher

logger = logging.getLogger(__name__)


DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>HeavyDrops Transcoder</title>
<style>
  body { font: 14px/1.45 system-ui, sans-serif; margin: 2em; background: #111; color: #eee; }
  h1 { margin: 0 0 .3em 0; }
  h2 { margin: 1.2em 0 .3em 0; color: #aaa; font-weight: 500; font-size: 1em; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 1em; margin-bottom: 1em; }
  .card { background: #1c1c1c; border: 1px solid #2a2a2a; border-radius: 6px; padding: .8em 1em; }
  .k { color: #888; font-size: .85em; text-transform: uppercase; letter-spacing: .03em; }
  .v { font-size: 1.3em; margin-top: .15em; word-break: break-all; }
  .ok { color: #8c8; } .warn { color: #ea8; } .err { color: #e66; }
  table { width: 100%; border-collapse: collapse; margin-top: .4em; font-size: 13px; }
  th, td { text-align: left; padding: 4px 8px; border-bottom: 1px solid #2a2a2a; }
  th { color: #888; font-weight: 500; }
  button { background: #333; color: #eee; border: 1px solid #444; border-radius: 4px;
           padding: .4em .9em; cursor: pointer; font: inherit; margin-right: .4em; }
  button:hover { background: #444; }
  .banner { background: #332a1a; border: 1px solid #8a6a2a; color: #fda; padding: .7em 1em; border-radius: 6px; margin-bottom: 1em; }
  .worker { background: #1c1c1c; border: 1px solid #2a2a2a; border-radius: 6px; padding: .8em 1em; margin-bottom: .6em; }
  .worker-head { display: flex; justify-content: space-between; gap: 1em; margin-bottom: .3em; }
  .worker-head .name { color: #aaa; font-size: .85em; text-transform: uppercase; letter-spacing: .03em; }
  .worker-head .stage { font-size: .85em; padding: 1px 8px; border-radius: 10px; }
  .stage-download { background: #1a3a52; color: #9cf; }
  .stage-transcode { background: #4a2a52; color: #d9f; }
  .stage-upload { background: #1a523a; color: #9fc; }
  .worker .path { word-break: break-all; font-size: .9em; color: #ccc; margin-bottom: .4em; }
  .worker .meta { color: #888; font-size: .82em; }
  .bar { background: #2a2a2a; border-radius: 4px; height: 8px; overflow: hidden; margin: .3em 0; }
  .bar > span { display: block; height: 100%; background: linear-gradient(90deg, #4a8 0%, #7c8 100%); }
  .scan-card { background: #1c1c1c; border: 1px solid #2a2a2a; border-radius: 6px; padding: .8em 1em; margin-bottom: 1em; }
  .scan-card .path { color: #ccc; font-size: .9em; word-break: break-all; }
  .empty { color: #666; padding: .6em 0; font-style: italic; }
  pre.log { background: #0c0c0c; border: 1px solid #2a2a2a; border-radius: 6px;
            padding: .7em 1em; font: 12px/1.4 ui-monospace, Consolas, monospace;
            color: #cfc; max-height: 320px; overflow-y: scroll; margin: 0; white-space: pre-wrap; }
  pre.log .err { color: #f99; } pre.log .warn { color: #fc7; }
</style>
</head>
<body>
<h1>HeavyDrops Transcoder</h1>
<div id="banner"></div>

<div class="grid" id="status-grid"></div>

<div>
  <button onclick="post('/api/scan-now')">Scan now</button>
  <button onclick="post('/api/pause')">Pause</button>
  <button onclick="post('/api/resume')">Resume</button>
  <button onclick="post('/api/retry-failed')">Retry FAILED</button>
</div>

<h2>Scanner</h2>
<div id="scan-card" class="scan-card"></div>

<h2>Workers — live</h2>
<div id="workers"></div>

<h2>Jobs by state</h2>
<table id="state-table"><thead><tr><th>State</th><th>Count</th></tr></thead><tbody></tbody></table>

<h2>Recent jobs</h2>
<table id="jobs-table">
  <thead><tr><th>ID</th><th>State</th><th>Path</th><th>Size</th><th>Retries</th></tr></thead>
  <tbody></tbody>
</table>

<h2>Activity log <span style="color:#666;font-weight:normal;font-size:.85em">(last 200 lines, auto-refresh)</span></h2>
<pre id="log" class="log"></pre>

<script>
function fmtBytes(n) {
  if (!n) return '0 B';
  const u = ['B','KB','MB','GB','TB','PB']; let i = 0;
  while (n >= 1024 && i < u.length-1) { n /= 1024; i++; }
  return n.toFixed(2) + ' ' + u[i];
}
function fmtDuration(sec) {
  if (sec == null || isNaN(sec)) return '—';
  sec = Math.max(0, Math.floor(sec));
  if (sec < 60) return sec + 's';
  const m = Math.floor(sec / 60), s = sec % 60;
  if (m < 60) return m + 'm ' + s + 's';
  const h = Math.floor(m / 60), mm = m % 60;
  if (h < 24) return h + 'h ' + mm + 'm';
  const d = Math.floor(h / 24), hh = h % 24;
  return d + 'd ' + hh + 'h';
}
function basename(p) { if (!p) return ''; const i = p.lastIndexOf('/'); return i >= 0 ? p.slice(i+1) : p; }
function card(k, v, cls='') { return `<div class="card"><div class="k">${k}</div><div class="v ${cls}">${v}</div></div>`; }

function renderScan(scan) {
  const el = document.getElementById('scan-card');
  if (!scan || scan.mode === 'idle' || !scan.mode) {
    el.innerHTML = `<div class="empty">Scanner idle (next pass scheduled).</div>`;
    return;
  }
  const cls = scan.mode === 'bulk' ? 'warn' : 'ok';
  el.innerHTML = `
    <div class="worker-head">
      <span class="name">Scanner</span>
      <span class="stage stage-download ${cls}">${scan.mode.toUpperCase()}</span>
    </div>
    <div class="path">${scan.current_path || '—'}</div>
    <div class="meta">${scan.entries_seen.toLocaleString()} entries seen · running for ${fmtDuration(scan.elapsed_sec)}</div>
  `;
}

function renderWorkers(active) {
  const el = document.getElementById('workers');
  const ws = (active && active.workers) || [];
  if (ws.length === 0) {
    el.innerHTML = `<div class="empty">No active jobs right now.</div>`;
    return;
  }
  el.innerHTML = ws.sort((a,b) => a.worker.localeCompare(b.worker)).map(w => {
    const pct = (w.percent || 0).toFixed(1);
    let meta = '';
    if (w.stage === 'transcode') {
      meta = `${fmtDuration(w.time_sec)} / ${fmtDuration(w.duration_sec)} · ${w.fps.toFixed(1)} fps · ${w.speed.toFixed(2)}x`;
      if (w.bitrate_kbps) meta += ` · ${(w.bitrate_kbps/1024).toFixed(1)} Mb/s`;
    } else {
      meta = `${fmtBytes(w.bytes_done)} / ${fmtBytes(w.bytes_total)}`;
      if (w.elapsed_sec > 0 && w.bytes_done > 0) {
        const rate = w.bytes_done / w.elapsed_sec;
        meta += ` · ${fmtBytes(rate)}/s`;
      }
    }
    if (w.eta_sec != null) meta += ` · ETA ${fmtDuration(w.eta_sec)}`;
    meta += ` · elapsed ${fmtDuration(w.elapsed_sec)}`;
    return `
      <div class="worker">
        <div class="worker-head">
          <span class="name">${w.worker} · job #${w.job_id}</span>
          <span class="stage stage-${w.stage}">${w.stage.toUpperCase()} ${pct}%</span>
        </div>
        <div class="path">${basename(w.path)} <span style="color:#666">${w.path.replace(basename(w.path),'').replace(/\\/$/,'')}</span></div>
        <div class="bar"><span style="width:${pct}%"></span></div>
        <div class="meta">${meta}</div>
      </div>
    `;
  }).join('');
}

function renderLog(payload) {
  const el = document.getElementById('log');
  const wasAtBottom = el.scrollTop + el.clientHeight >= el.scrollHeight - 4;
  const lines = (payload && payload.lines) || [];
  el.innerHTML = lines.map(ln => {
    const safe = ln.replace(/[<&>]/g, c => ({'<':'&lt;','&':'&amp;','>':'&gt;'})[c]);
    if (/\bERROR\b/.test(ln)) return `<span class="err">${safe}</span>`;
    if (/\bWARNING\b/.test(ln)) return `<span class="warn">${safe}</span>`;
    return safe;
  }).join('\n');
  if (wasAtBottom) el.scrollTop = el.scrollHeight;
}

async function tick() {
  try {
    const [s, j, a, lg] = await Promise.all([
      fetch('/api/status').then(r => r.json()),
      fetch('/api/jobs?limit=20').then(r => r.json()),
      fetch('/api/active').then(r => r.json()),
      fetch('/api/log?lines=200').then(r => r.json()),
    ]);

    document.getElementById('banner').innerHTML = s.update.update_available
      ? `<div class="banner">Update available: <b>${s.update.latest_tag}</b> (installed ${s.update.current_version}). Run <code>hd update</code> to apply.</div>`
      : '';

    const g = document.getElementById('status-grid');
    g.innerHTML = [
      card('Version', s.update.current_version),
      card('Uptime', s.uptime_human),
      card('Scan mode', s.scan.mode, s.scan.mode === 'bulk' ? 'warn' : 'ok'),
      card('Bulk complete', s.scan.bulk_pass_complete ? 'yes' : 'no', s.scan.bulk_pass_complete ? 'ok' : 'warn'),
      card('Entries seen', s.scan.entries_seen.toLocaleString()),
      card('Dispatcher', s.dispatcher.paused ? 'PAUSED' : 'running', s.dispatcher.paused ? 'warn' : 'ok'),
      card('Queues (D/T/U)', `${s.dispatcher.download}/${s.dispatcher.transcode}/${s.dispatcher.upload}`),
      card('Active jobs', s.dispatcher.active),
      card('Disk reserved', fmtBytes(s.disk.reserved_bytes)),
      card('Disk free', fmtBytes(s.disk.free_bytes)),
      card('Jobs done', s.jobs.done.toLocaleString(), 'ok'),
      card('Jobs failed', s.jobs.failed.toLocaleString(), s.jobs.failed ? 'err' : ''),
    ].join('');

    const st = document.querySelector('#state-table tbody');
    st.innerHTML = Object.entries(s.jobs.state_counts)
      .sort((a, b) => b[1] - a[1])
      .map(([k, v]) => `<tr><td>${k}</td><td>${v.toLocaleString()}</td></tr>`)
      .join('');

    const jt = document.querySelector('#jobs-table tbody');
    jt.innerHTML = j.jobs.map(r =>
      `<tr><td>${r.id}</td><td>${r.state}</td><td>${r.dropbox_path}</td>
           <td>${fmtBytes(r.dropbox_size)}</td><td>${r.retry_count}</td></tr>`
    ).join('');

    renderScan(a.scanner);
    renderWorkers(a);
    renderLog(lg);
  } catch (e) {
    document.getElementById('banner').innerHTML =
      `<div class="banner">Daemon unreachable: ${e}</div>`;
  }
}
async function post(path) {
  try {
    await fetch(path, { method: 'POST' });
    tick();
  } catch (e) { alert(e); }
}
tick();
setInterval(tick, 3000);
</script>
</body></html>
"""


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
            self.send_error(404, "not found")

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
