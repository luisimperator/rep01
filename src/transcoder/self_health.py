"""
Daemon-side self-health agent.

Runs in a background thread every `interval_sec` (default 3h). Checks for
known failure patterns and applies the corresponding auto-fix without
operator intervention. Posts a recurring status report to a sticky
GitHub Issue when the IncidentReporter is configured.

The intent: replace "operator copy-pastes log into chat" with "daemon
self-diagnoses, auto-fixes what it can, posts a single Issue summary."
"""

from __future__ import annotations

import json
import logging
import re
import shutil
import subprocess
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .config import Config
    from .database import Database
    from .dispatcher import JobDispatcher
    from .dropbox_client import DropboxClient
    from .incidents import IncidentReporter

logger = logging.getLogger(__name__)


@dataclass
class CheckResult:
    name: str
    ok: bool
    detail: str
    actions_taken: list[str] = field(default_factory=list)
    needs_human: bool = False


class SelfHealthAgent(threading.Thread):
    """Periodic self-diagnosis + auto-fix + GitHub status reporting."""

    HEALTH_FINGERPRINT = "fp:daemon-health"
    INITIAL_DELAY_SEC = 60         # let daemon settle before first check
    DEFAULT_INTERVAL_SEC = 3 * 3600

    def __init__(
        self,
        config: "Config",
        db: "Database",
        dispatcher: "JobDispatcher",
        dropbox: "DropboxClient | None",
        reporter: "IncidentReporter | None",
        stop_event: threading.Event,
        interval_sec: int = DEFAULT_INTERVAL_SEC,
    ) -> None:
        super().__init__(name="self-health", daemon=True)
        self.config = config
        self.db = db
        self.dispatcher = dispatcher
        self.dropbox = dropbox
        self.reporter = reporter
        self.stop_event = stop_event
        self.interval_sec = interval_sec
        self._last_summary: dict | None = None

    # ----------------------------------------------------------------- loop

    def run(self) -> None:
        logger.info(f"self-health agent started (interval={self.interval_sec}s)")
        # Wait briefly so the first check sees real telemetry, not a fresh boot.
        if self.stop_event.wait(self.INITIAL_DELAY_SEC):
            return

        while not self.stop_event.is_set():
            try:
                self.run_once()
            except Exception:
                logger.exception("self-health check crashed")

            if self.stop_event.wait(self.interval_sec):
                return

        logger.info("self-health agent stopped")

    def run_once(self) -> dict:
        """Run one round of checks + fixes; return the summary dict."""
        started = time.time()
        results: list[CheckResult] = [
            self._check_partials(),
            self._check_stuck_jobs(),
            self._check_transcode_health(),
            self._check_disk_pressure(),
            self._check_dropbox_connectivity(),
        ]

        summary = self._summarize(results, elapsed=time.time() - started)
        self._last_summary = summary

        # Always log a one-line summary so the local log makes sense even
        # without GitHub.
        logger.info(
            f"self-health: {summary['headline']} "
            f"(ok={summary['ok_checks']}/{summary['total_checks']}, "
            f"actions={len(summary['actions'])}, escalations={summary['escalations']})"
        )

        if self.reporter and self.reporter.enabled:
            self._post_health_report(summary, results)
            for r in results:
                if r.needs_human:
                    self.reporter.report(
                        kind="needs-human",
                        summary=f"{r.name}: {r.detail}",
                        log_tail="\n".join(r.actions_taken),
                        context={"check": r.name},
                    )

        return summary

    # --------------------------------------------------------------- checks

    def _check_partials(self) -> CheckResult:
        """Stale .partial files older than 7 days are dead weight; remove them."""
        staging = Path(self.config.local_staging_dir)
        if not staging.exists():
            return CheckResult("partials", True, "staging dir missing — skipping")

        cutoff = time.time() - 7 * 86400
        removed = 0
        bytes_freed = 0
        actions = []
        for p in staging.rglob("*.partial"):
            try:
                st = p.stat()
            except OSError:
                continue
            if st.st_mtime < cutoff:
                try:
                    p.unlink()
                    removed += 1
                    bytes_freed += st.st_size
                    actions.append(f"unlinked {p} ({_fmt_bytes(st.st_size)})")
                except OSError as e:
                    actions.append(f"could not unlink {p}: {e}")
        if removed:
            return CheckResult(
                "partials", True,
                f"cleaned {removed} stale .partial file(s) ({_fmt_bytes(bytes_freed)})",
                actions_taken=actions,
            )
        return CheckResult("partials", True, "no stale partials")

    def _check_stuck_jobs(self) -> CheckResult:
        """Jobs in DOWNLOADING/TRANSCODING/UPLOADING for >6h with no recent
        update are presumed dead; reset them to NEW so the dispatcher picks
        them up again. The watchdog handles in-flight timeouts within the
        per-stage SLA; this is the catch-all for anything that escapes."""
        from .database import JobState
        cutoff = datetime.now(timezone.utc) - timedelta(hours=6)
        stuck = []
        for state in (JobState.DOWNLOADING, JobState.TRANSCODING, JobState.UPLOADING):
            try:
                jobs = self.db.get_jobs_by_state(state, limit=100)
            except Exception as e:
                return CheckResult("stuck-jobs", False, f"db query failed: {e}")
            for job in jobs:
                if job.updated_at and job.updated_at < cutoff:
                    stuck.append((job, state.value))

        actions = []
        for job, state in stuck:
            try:
                self.db.update_job_state(
                    job.id, JobState.NEW,
                    error_message=f"reset by self-health: stuck in {state} since {job.updated_at}",
                )
                actions.append(f"reset job #{job.id} ({state} -> NEW): {job.dropbox_path}")
            except Exception as e:
                actions.append(f"could not reset job #{job.id}: {e}")

        if not stuck:
            return CheckResult("stuck-jobs", True, "no stuck jobs")
        return CheckResult(
            "stuck-jobs", True,
            f"reset {len(actions)} stuck job(s)",
            actions_taken=actions,
        )

    def _check_transcode_health(self) -> CheckResult:
        """Catastrophic-failure detector. If every recent transcode is failing
        the daemon is just burning Dropbox bandwidth re-downloading files
        it can't process. Pause the dispatcher and escalate to needs-human."""
        from .database import JobState
        try:
            stats = self.db.get_stats()
        except Exception as e:
            return CheckResult("transcode-health", False, f"db query failed: {e}")
        counts = stats.get("state_counts", {})
        done = counts.get(JobState.DONE.value, 0)
        failed = counts.get(JobState.FAILED.value, 0)
        active = counts.get(JobState.TRANSCODING.value, 0) + counts.get(JobState.NEW.value, 0)

        # Need a meaningful sample.
        if (done + failed) < 5:
            return CheckResult(
                "transcode-health", True,
                f"sample too small (done={done}, failed={failed}, active={active})",
            )

        success_rate = done / (done + failed) if (done + failed) else 0.0
        if success_rate < 0.10 and failed >= 5:
            actions = []
            try:
                self.dispatcher.pause()
                actions.append("paused dispatcher")
            except Exception as e:
                actions.append(f"could not pause dispatcher: {e}")
            return CheckResult(
                "transcode-health", False,
                f"catastrophic failure: {failed} failed / {done} done "
                f"(success rate {success_rate*100:.0f}%). Dispatcher paused.",
                actions_taken=actions,
                needs_human=True,
            )

        return CheckResult(
            "transcode-health", True,
            f"{done} done, {failed} failed ({success_rate*100:.0f}% success), {active} pending",
        )

    def _check_disk_pressure(self) -> CheckResult:
        """If the staging volume is tight, turn on disk_budget so the daemon
        stops downloading until capacity frees up."""
        try:
            staging = Path(self.config.local_staging_dir)
            usage = shutil.disk_usage(staging) if staging.exists() else None
        except OSError:
            usage = None
        if usage is None:
            return CheckResult("disk-pressure", True, "staging dir not present yet")

        free_pct = 100.0 * usage.free / usage.total
        if free_pct < 10.0 and not self.config.disk_budget.enabled:
            actions = []
            try:
                self.config.disk_budget.enabled = True
                _persist_yaml_kv(
                    Path("config.yaml"),
                    "disk_budget",
                    {"enabled": True},
                )
                actions.append(f"enabled disk_budget (free {free_pct:.1f}%)")
            except Exception as e:
                actions.append(f"could not enable disk_budget: {e}")
            return CheckResult(
                "disk-pressure", False,
                f"only {free_pct:.1f}% free on staging volume; enabled disk_budget",
                actions_taken=actions,
            )
        return CheckResult(
            "disk-pressure", True,
            f"{free_pct:.1f}% free on staging volume "
            f"({_fmt_bytes(usage.free)} / {_fmt_bytes(usage.total)})",
        )

    def _check_dropbox_connectivity(self) -> CheckResult:
        if self.dropbox is None:
            return CheckResult("dropbox", True, "not initialized")
        try:
            ok = self.dropbox.check_connection()
        except Exception as e:
            return CheckResult(
                "dropbox", False, f"connection check raised: {e}",
                needs_human=True,
            )
        if not ok:
            return CheckResult(
                "dropbox", False, "check_connection returned False",
                needs_human=True,
            )
        return CheckResult("dropbox", True, "connected")

    # ------------------------------------------------------- summarization

    def _summarize(self, results: list[CheckResult], elapsed: float) -> dict:
        actions: list[str] = []
        escalations = 0
        ok_checks = 0
        for r in results:
            actions.extend(r.actions_taken)
            if r.ok and not r.needs_human:
                ok_checks += 1
            if r.needs_human:
                escalations += 1
        if escalations:
            headline = "FAILURES — operator attention needed"
        elif actions:
            headline = "OK with auto-fixes applied"
        else:
            headline = "OK"
        return {
            "headline": headline,
            "ok_checks": ok_checks,
            "total_checks": len(results),
            "actions": actions,
            "escalations": escalations,
            "results": [
                {
                    "name": r.name,
                    "ok": r.ok,
                    "detail": r.detail,
                    "needs_human": r.needs_human,
                    "actions": r.actions_taken,
                }
                for r in results
            ],
            "elapsed_sec": round(elapsed, 2),
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }

    def _post_health_report(self, summary: dict, results: list[CheckResult]) -> None:
        from . import __version__
        body_parts = [
            f"**Headline:** {summary['headline']}",
            f"**Checks:** {summary['ok_checks']}/{summary['total_checks']} ok, "
            f"{summary['escalations']} escalation(s), "
            f"{len(summary['actions'])} auto-fix(es) applied",
            f"**Daemon:** v{__version__}",
            "",
            "| Check | Status | Detail |",
            "|---|---|---|",
        ]
        for r in results:
            status = "needs human" if r.needs_human else ("ok" if r.ok else "warn")
            detail_safe = r.detail.replace("|", "\\|")
            body_parts.append(f"| {r.name} | {status} | {detail_safe} |")
        if summary["actions"]:
            body_parts.append("")
            body_parts.append("**Auto-fixes applied:**")
            for a in summary["actions"]:
                body_parts.append(f"- {a}")
        body = "\n".join(body_parts)

        # Use a stable fingerprint so every health check reuses the same issue.
        try:
            self.reporter.report(
                kind="daemon-health",
                summary=summary["headline"],
                log_tail=body,
                context={
                    "interval_hours": round(self.interval_sec / 3600, 1),
                    "actions_count": len(summary["actions"]),
                    "escalations": summary["escalations"],
                },
            )
        except Exception:
            logger.exception("could not post health report")


# ---------------------------------------------------------------- helpers


def _fmt_bytes(n: int | float) -> str:
    if not n:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    n = float(n)
    while n >= 1024 and i < len(units) - 1:
        n /= 1024
        i += 1
    return f"{n:.2f} {units[i]}"


def _persist_yaml_kv(cfg_path: Path, top_key: str, sub: dict) -> None:
    """Best-effort update of a `top_key:` block inside config.yaml.

    Simple regex approach matching the rest of the codebase: replace the
    block if it exists, append it otherwise. Doesn't preserve in-block
    comments but keeps the rest of the file intact.
    """
    if not cfg_path.exists():
        return
    raw = cfg_path.read_text(encoding="utf-8")
    raw = re.sub(rf'(?ms)^{re.escape(top_key)}:\s*\n(?: {{2}}.*\n)*', '', raw)
    if raw and not raw.endswith("\n"):
        raw += "\n"
    raw += f"{top_key}:\n"
    for k, v in sub.items():
        if isinstance(v, bool):
            raw += f"  {k}: {'true' if v else 'false'}\n"
        elif isinstance(v, (int, float)):
            raw += f"  {k}: {v}\n"
        else:
            raw += f"  {k}: \"{v}\"\n"
    cfg_path.write_text(raw, encoding="utf-8")
