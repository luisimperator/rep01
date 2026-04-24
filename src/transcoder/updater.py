"""
Notify-only update checker backed by GitHub Releases.

On daemon startup a background thread queries the configured repo's
``/releases/latest`` endpoint, compares the published tag to the installed
package version, and persists the result to the ``settings`` table. The HTTP
API surfaces the flag so the GUI (or a curl check) can tell the user a new
release is available. The daemon never self-updates — running ``hd update``
applies it (``git pull`` + ``pip install -e .`` + daemon restart).
"""

from __future__ import annotations

import json
import logging
import re
import subprocess
import sys
import threading
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .database import Database

logger = logging.getLogger(__name__)


_SETTING_LATEST = "updater.latest_tag"
_SETTING_AVAILABLE = "updater.update_available"
_SETTING_CHECKED_AT = "updater.checked_at"
_SETTING_ERROR = "updater.last_error"


# ---------------------------------------------------------------- version util

def installed_version() -> str:
    """Return the installed package version, falling back to __version__."""
    try:
        from importlib.metadata import PackageNotFoundError, version as _pkg_version
        try:
            return _pkg_version("heavydrops-transcoder")
        except PackageNotFoundError:
            pass
    except Exception:
        pass

    # Fallbacks: __init__.__version__, then pyproject.toml in the source tree
    try:
        from . import __version__  # type: ignore[attr-defined]
        if __version__:
            return str(__version__)
    except Exception:
        pass

    try:
        pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
        text = pyproject.read_text(encoding="utf-8")
        m = re.search(r'^version\s*=\s*"([^"]+)"', text, re.MULTILINE)
        if m:
            return m.group(1)
    except Exception:
        pass

    return "0.0.0"


def _normalize_version(value: str) -> tuple[int, ...]:
    """Parse a tag/version into a comparable tuple: ``v5.8.4`` → (5, 8, 4)."""
    cleaned = value.strip().lstrip("vV")
    parts = re.split(r"[^0-9]+", cleaned)
    nums: list[int] = []
    for p in parts:
        if not p:
            continue
        try:
            nums.append(int(p))
        except ValueError:
            break
    return tuple(nums) if nums else (0,)


# ------------------------------------------------------------------ GitHub API

@dataclass
class UpdateStatus:
    """Snapshot of the update-check state for the HTTP API."""
    current_version: str
    latest_tag: str | None
    update_available: bool
    checked_at: str | None
    error: str | None


def _fetch_latest_tag(github_repo: str, timeout_sec: float) -> str:
    url = f"https://api.github.com/repos/{github_repo}/releases/latest"
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": "heavydrops-transcoder-updater",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    tag = payload.get("tag_name")
    if not tag:
        raise ValueError("release payload missing tag_name")
    return str(tag)


def check_for_update(
    db: Database,
    github_repo: str,
    timeout_sec: float = 5.0,
) -> UpdateStatus:
    """Run one update check synchronously and persist the result."""
    current = installed_version()
    try:
        latest = _fetch_latest_tag(github_repo, timeout_sec)
        available = _normalize_version(latest) > _normalize_version(current)
        _persist(db, latest=latest, available=available, error=None)
        if available:
            logger.info("update available: %s (installed %s)", latest, current)
        else:
            logger.info("no update available (installed %s, latest %s)", current, latest)
        return UpdateStatus(
            current_version=current,
            latest_tag=latest,
            update_available=available,
            checked_at=_now_iso(),
            error=None,
        )
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ValueError, OSError) as e:
        err = f"{type(e).__name__}: {e}"
        logger.warning("update check failed: %s", err)
        _persist(db, latest=None, available=False, error=err)
        return UpdateStatus(
            current_version=current,
            latest_tag=None,
            update_available=False,
            checked_at=_now_iso(),
            error=err,
        )


def check_for_update_async(
    db: Database,
    github_repo: str,
    timeout_sec: float = 5.0,
) -> threading.Thread:
    """Fire-and-forget the update check so daemon startup doesn't block on network."""
    t = threading.Thread(
        target=lambda: check_for_update(db, github_repo, timeout_sec),
        name="updater",
        daemon=True,
    )
    t.start()
    return t


def read_status(db: Database) -> UpdateStatus:
    """Load the last persisted update status (for the HTTP API)."""
    latest = db.get_setting(_SETTING_LATEST)
    available = (db.get_setting(_SETTING_AVAILABLE) or "0") == "1"
    checked_at = db.get_setting(_SETTING_CHECKED_AT)
    error = db.get_setting(_SETTING_ERROR)
    return UpdateStatus(
        current_version=installed_version(),
        latest_tag=latest,
        update_available=available,
        checked_at=checked_at,
        error=error,
    )


# ------------------------------------------------------------------ apply flow

def apply_update(
    install_dir: Path,
    log_fn: Callable[[str], None] = print,
) -> int:
    """
    Run ``git pull --ff-only`` followed by ``pip install -e .`` when the
    dependency manifest changed. Returns the exit code of the last failing
    step, or 0 on success.

    Does not restart the daemon — callers are responsible for that. On
    Windows this means re-running the ``HeavyDropsDaemon`` scheduled task;
    on Linux, sending SIGTERM to the daemon pid and relaunching. The
    Task-Scheduler-based Phase C.2 plumbing handles this automatically.
    """
    install_dir = Path(install_dir)
    if not (install_dir / ".git").exists():
        log_fn(f"ERROR: {install_dir} is not a git checkout; refusing to update.")
        return 2

    # Pre-pull manifest digest so we can tell whether pip install is needed.
    pyproject = install_dir / "pyproject.toml"
    before = _digest(pyproject)

    rc = _run(["git", "-C", str(install_dir), "pull", "--ff-only"], log_fn)
    if rc != 0:
        log_fn("git pull failed; aborting update.")
        return rc

    after = _digest(pyproject)
    if before != after:
        log_fn("pyproject.toml changed — reinstalling package.")
        rc = _run([sys.executable, "-m", "pip", "install", "-e", str(install_dir)], log_fn)
        if rc != 0:
            log_fn("pip install failed; aborting update.")
            return rc
    else:
        log_fn("pyproject.toml unchanged — skipping pip install.")

    log_fn("update applied. Restart the daemon to pick up the new version.")
    return 0


# ---------------------------------------------------------------------- helpers

def _run(argv: list[str], log_fn: Callable[[str], None]) -> int:
    log_fn(f"+ {' '.join(argv)}")
    proc = subprocess.run(argv, capture_output=True, text=True)
    if proc.stdout:
        log_fn(proc.stdout.rstrip())
    if proc.stderr:
        log_fn(proc.stderr.rstrip())
    return proc.returncode


def _digest(path: Path) -> str:
    try:
        return path.read_bytes().hex()
    except (FileNotFoundError, PermissionError):
        return ""


def _persist(
    db: Database,
    *,
    latest: str | None,
    available: bool,
    error: str | None,
) -> None:
    db.set_setting(_SETTING_LATEST, latest or "")
    db.set_setting(_SETTING_AVAILABLE, "1" if available else "0")
    db.set_setting(_SETTING_CHECKED_AT, _now_iso())
    db.set_setting(_SETTING_ERROR, error or "")


def _now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()
