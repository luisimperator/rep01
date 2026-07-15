"""
Update checker (and optional self-updater) backed by GitHub Releases.

The daemon runs an :class:`AutoUpdater` thread that periodically queries the
configured repo's ``/releases/latest`` endpoint, compares the published tag
to the installed package version, and persists the result to the ``settings``
table (the HTTP API surfaces it to the dashboard). With ``updater.auto_apply``
enabled, a newer release is also APPLIED in place — ``git pull --ff-only``
plus ``pip install -e .`` when the manifest changed — and the daemon is asked
to restart itself so the new code actually loads. With ``auto_apply`` off it
degrades to the old notify-only behavior (``hd update`` applies manually).
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


# ------------------------------------------------------------------ auto-update

def detect_install_dir() -> Path | None:
    """Locate the git checkout this package runs from, or None.

    Editable installs (the bootstrap's ``pip install -e .``) import straight
    from the checkout, so walking up from this file finds ``.git`` +
    ``pyproject.toml``. A site-packages install finds neither — auto-apply
    then stays off (there is no checkout to ``git pull``).
    """
    for parent in Path(__file__).resolve().parents:
        if (parent / ".git").exists() and (parent / "pyproject.toml").exists():
            return parent
    return None


def _checkout_version(install_dir: Path) -> str:
    """Read the version currently on disk in the checkout's pyproject.toml."""
    try:
        text = (Path(install_dir) / "pyproject.toml").read_text(encoding="utf-8")
        m = re.search(r'^version\s*=\s*"([^"]+)"', text, re.MULTILINE)
        if m:
            return m.group(1)
    except OSError:
        pass
    return "0.0.0"


class AutoUpdater(threading.Thread):
    """Periodic release check that can apply updates and restart the daemon.

    Every ``interval_sec`` (first check right at startup) the thread refreshes
    the persisted update status. When ``auto_apply`` is on and a newer release
    exists, it applies the update in place and calls ``request_restart`` — the
    running process keeps executing the OLD code (Python has it in memory), so
    only a restart loads the new version.

    A tag whose apply fails (or that doesn't actually move the checkout's
    version past the running one) is remembered and not retried, so a broken
    checkout can't put the daemon in a pull/restart loop; publishing a newer
    release resets the latch.
    """

    def __init__(
        self,
        db: Database,
        github_repo: str,
        stop_event: threading.Event,
        *,
        timeout_sec: float = 5.0,
        interval_sec: float = 1800.0,
        auto_apply: bool = False,
        install_dir: Path | None = None,
        request_restart: Callable[[str], None] | None = None,
        check_fn: Callable[..., UpdateStatus] | None = None,
        apply_fn: Callable[..., int] | None = None,
    ) -> None:
        super().__init__(name="updater", daemon=True)
        self.db = db
        self.github_repo = github_repo
        self.stop_event = stop_event
        self.timeout_sec = timeout_sec
        self.interval_sec = interval_sec
        self.auto_apply = auto_apply
        self.install_dir = Path(install_dir) if install_dir else None
        self.request_restart = request_restart
        self._check_fn = check_fn or check_for_update
        self._apply_fn = apply_fn or apply_update
        # Version this process is actually RUNNING. installed_version() reads
        # dist-info, which pip rewrites during apply — capture it now so the
        # newer-than comparison stays anchored to the code in memory.
        self._running_version = installed_version()
        self._skip_tag: str | None = None

    def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                self._tick()
            except Exception:
                logger.warning("update check crashed", exc_info=True)
            self.stop_event.wait(self.interval_sec)

    def _tick(self) -> None:
        status = self._check_fn(self.db, self.github_repo, self.timeout_sec)
        if not (status.update_available and status.latest_tag):
            return
        tag = status.latest_tag
        if not self.auto_apply or self.request_restart is None:
            return
        if tag == self._skip_tag:
            return
        if self.install_dir is None:
            logger.warning(
                "auto-update: %s available but this install is not a git "
                "checkout; apply it manually with `hd update`", tag,
            )
            self._skip_tag = tag
            return

        logger.info(
            "auto-update: applying %s (running %s)", tag, self._running_version
        )
        rc = self._apply_fn(self.install_dir, log_fn=logger.info)
        if rc != 0:
            self._skip_tag = tag
            logger.error(
                "auto-update: apply of %s failed (rc=%d); leaving the daemon "
                "as-is. Will only retry when a newer release is published.",
                tag, rc,
            )
            return

        on_disk = _checkout_version(self.install_dir)
        if _normalize_version(on_disk) <= _normalize_version(self._running_version):
            self._skip_tag = tag
            logger.warning(
                "auto-update: pulled %s but the checkout still holds %s "
                "(running %s); not restarting.", tag, on_disk,
                self._running_version,
            )
            return

        logger.info(
            "auto-update: %s applied (checkout now %s); restarting daemon "
            "to load it", tag, on_disk,
        )
        self.request_restart(f"auto-update to {tag}")


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
