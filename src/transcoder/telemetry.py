"""Status publisher: push a redacted health snapshot to GitHub on a timer.

The daemon runs on a machine we can't reach directly, so to keep an eye on it
remotely it periodically publishes a small Markdown snapshot — recent log tail,
job-state counts, disk free, and any new crash.log content — to a dedicated
branch in the GitHub repo. A reader (a human or a scheduled assistant session)
can then pull that one file every few hours and diagnose without anyone copying
logs by hand.

Design notes:
  * Off unless ``telemetry.enabled`` AND a token is available. The token is
    reused from the incidents reporter (or the GITHUB_TOKEN env var), so there
    is usually nothing new to set up.
  * Publishes ONE file per machine: ``<branch>:/telemetry/<pc>-status.md``,
    overwritten in place each interval (the branch keeps the history).
  * Network/IO failures only warn and retry next interval — telemetry must
    never take the daemon down. The token is never logged or published.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import shutil
import socket
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .config import Config
    from .database import Database

logger = logging.getLogger(__name__)

GITHUB_API = "https://api.github.com"


def _tail(path: Path, max_lines: int, max_bytes: int = 96 * 1024) -> str:
    """Return the last ``max_lines`` lines of a (possibly large) text file.

    Only the trailing ``max_bytes`` are read so a multi-MB log stays cheap.
    """
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - max_bytes))
            data = f.read().decode("utf-8", errors="replace")
        # Drop a possibly-partial first line when we seeked into the middle.
        lines = data.splitlines()
        if size > max_bytes and len(lines) > 1:
            lines = lines[1:]
        return "\n".join(lines[-max_lines:])
    except FileNotFoundError:
        return ""
    except Exception as e:  # pragma: no cover — defensive
        return f"(could not read {path.name}: {e})"


def _human_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


class StatusPublisher(threading.Thread):
    """Periodically publishes a health snapshot to GitHub."""

    def __init__(
        self,
        config: "Config",
        db: "Database",
        stop_event: threading.Event,
        log_dir: Path,
        token: str,
    ) -> None:
        super().__init__(name="status-publisher", daemon=True)
        self.config = config
        self.db = db
        self.stop_event = stop_event
        self.log_dir = Path(log_dir)
        self.token = token
        self._started_at = time.time()
        self._last_crash_size = self._crash_size()

    # ----------------------------------------------------------------- helpers

    @property
    def _t(self):
        return self.config.telemetry

    def _pc(self) -> str:
        try:
            return socket.gethostname() or "unknown-pc"
        except Exception:  # pragma: no cover — defensive
            return "unknown-pc"

    def _slug(self, name: str) -> str:
        return "".join(c if c.isalnum() or c in "-_" else "-" for c in name).strip("-") or "pc"

    def _path(self) -> str:
        return f"telemetry/{self._slug(self._pc())}-status.md"

    def _crash_size(self) -> int:
        try:
            return (self.log_dir / "crash.log").stat().st_size
        except OSError:
            return 0

    # ------------------------------------------------------------- report body

    def _build_report(self) -> str:
        from . import __version__

        now = datetime.now(timezone.utc)
        lines: list[str] = []
        lines.append(f"# HeavyDrops status — {self._pc()}")
        lines.append("")
        lines.append(f"- generated: `{now.isoformat()}`  (UTC)")
        lines.append(f"- version: `{__version__}`")
        lines.append(
            f"- publisher uptime: `{(time.time() - self._started_at) / 3600:.1f} h` "
            "(time since this daemon process started)"
        )

        # Disk free on the drive that holds the logs/work dir.
        try:
            du = shutil.disk_usage(str(self.log_dir))
            pct = 100 * du.free / du.total if du.total else 0
            lines.append(
                f"- disk free: `{_human_bytes(du.free)}` of "
                f"`{_human_bytes(du.total)}` ({pct:.0f}% free)"
            )
        except Exception:
            lines.append("- disk free: (unavailable)")

        # Job-state counts + today's savings.
        try:
            stats = self.db.get_stats()
            sc = stats.get("state_counts", {})
            counts = ", ".join(f"{k}={v}" for k, v in sorted(sc.items())) or "(none)"
            lines.append(f"- jobs by state: `{counts}`")
            lines.append(f"- jobs total: `{stats.get('total_jobs', 0)}`")
        except Exception as e:
            lines.append(f"- jobs: (db error: {e})")
        try:
            midnight = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            sav = self.db.get_savings_stats(since=midnight)
            saved = sav.get("bytes_saved", 0) or 0
            done = sav.get("jobs", 0) or 0
            lines.append(
                f"- today: `{done}` transcodes done, `{_human_bytes(saved)}` saved"
            )
        except Exception:
            pass

        # Crash log: flag whether it grew since the last report.
        crash_path = self.log_dir / "crash.log"
        crash_size = self._crash_size()
        crash_tail = _tail(crash_path, 60)
        grew = crash_size > self._last_crash_size
        lines.append("")
        if not crash_tail:
            lines.append("## crash.log — empty ✅ (no silent crashes recorded)")
        else:
            flag = " ⚠️ **NEW since last report**" if grew else ""
            lines.append(f"## crash.log — `{_human_bytes(crash_size)}`{flag}")
            lines.append("")
            lines.append("```")
            lines.append(crash_tail)
            lines.append("```")
        self._last_crash_size = crash_size

        # Recent daemon log tail.
        lines.append("")
        n = max(10, int(self._t.log_tail_lines))
        lines.append(f"## transcoder.log — last {n} lines")
        lines.append("")
        lines.append("```")
        lines.append(_tail(self.log_dir / "transcoder.log", n) or "(no log yet)")
        lines.append("```")

        return "\n".join(lines) + "\n"

    # ----------------------------------------------------------- github plumbing

    def _request(
        self, method: str, path: str, payload: dict | None = None
    ) -> tuple[int, dict | list | None]:
        url = f"{GITHUB_API}{path}"
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        req = urllib.request.Request(
            url,
            data=data,
            method=method,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self.token}",
                "User-Agent": "heavydrops-telemetry",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                raw = resp.read().decode("utf-8")
                return resp.status, (json.loads(raw) if raw else None)
        except urllib.error.HTTPError as e:
            try:
                body = json.loads(e.read().decode("utf-8"))
            except Exception:
                body = None
            return e.code, body

    def _ensure_branch(self) -> None:
        """Create the telemetry branch off the default branch if it's missing."""
        repo, branch = self._t.github_repo, self._t.branch
        status, _ = self._request("GET", f"/repos/{repo}/git/ref/heads/{branch}")
        if status == 200:
            return
        # Find the default branch's tip and branch from it.
        s1, repo_info = self._request("GET", f"/repos/{repo}")
        if s1 != 200 or not isinstance(repo_info, dict):
            raise RuntimeError(f"cannot read repo {repo} (HTTP {s1})")
        default = repo_info.get("default_branch", "main")
        s2, ref = self._request("GET", f"/repos/{repo}/git/ref/heads/{default}")
        if s2 != 200 or not isinstance(ref, dict):
            raise RuntimeError(f"cannot read default branch {default} (HTTP {s2})")
        sha = ref["object"]["sha"]
        s3, _ = self._request(
            "POST", f"/repos/{repo}/git/refs",
            {"ref": f"refs/heads/{branch}", "sha": sha},
        )
        if s3 not in (200, 201):
            raise RuntimeError(f"cannot create branch {branch} (HTTP {s3})")
        logger.info("telemetry: created branch '%s' for status snapshots", branch)

    def _publish(self, content: str) -> None:
        repo, branch, path = self._t.github_repo, self._t.branch, self._path()
        # Need the current blob sha to update an existing file.
        status, data = self._request(
            "GET", f"/repos/{repo}/contents/{path}?ref={branch}"
        )
        sha = data.get("sha") if status == 200 and isinstance(data, dict) else None
        body = {
            "message": f"telemetry: {self._pc()} {datetime.now(timezone.utc):%Y-%m-%d %H:%M}Z",
            "content": base64.b64encode(content.encode("utf-8")).decode("ascii"),
            "branch": branch,
        }
        if sha:
            body["sha"] = sha
        st, resp = self._request("PUT", f"/repos/{repo}/contents/{path}", body)
        if st not in (200, 201):
            raise RuntimeError(f"PUT {path} → HTTP {st}: {resp}")

    # ---------------------------------------------------------------- thread loop

    def run(self) -> None:
        if not self._t.enabled:
            logger.info("telemetry: disabled in config; publisher exiting")
            return
        if not self.token:
            logger.warning(
                "telemetry: enabled but no token (set telemetry.github_token, "
                "reuse incidents.github_token, or GITHUB_TOKEN env); publisher exiting"
            )
            return

        logger.info(
            "telemetry: publishing %s → %s@%s every %d min",
            self._path(), self._t.github_repo, self._t.branch, self._t.interval_minutes,
        )
        # Small initial delay so the first snapshot has some runtime context.
        if self.stop_event.wait(min(60, self._t.interval_minutes * 60)):
            return
        try:
            self._ensure_branch()
        except Exception:
            logger.warning("telemetry: could not ensure branch exists", exc_info=True)

        interval = max(60, int(self._t.interval_minutes) * 60)
        while not self.stop_event.is_set():
            try:
                self._publish(self._build_report())
                logger.debug("telemetry: snapshot published")
            except Exception:
                logger.warning(
                    "telemetry: publish failed (retrying next interval)", exc_info=True
                )
            if self.stop_event.wait(interval):
                break
        logger.info("telemetry: publisher stopped")
