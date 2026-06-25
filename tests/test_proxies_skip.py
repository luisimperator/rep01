"""Tests for the Proxies/ folder rules.

Operator request (Transcoder 11): "we don't need to spend time on proxies —
delete the Proxies folder once it's older than X days." Three behaviors:

  1. ANY file under a `Proxies/` folder is throwaway (until v7.7.x only
     Sony-named `*_Proxy.*` files were matched, so Premiere/DaVinci proxies
     slipped through and were downloaded + transcoded).
  2. The download worker defensively skips proxy jobs already in the queue
     (queued before the rule / by an older scanner).
  3. When scanner.delete_throwaway_files is on, the WHOLE Proxies/ folder is
     deleted in one call — gated by the same folder-age check reorganize
     uses, so active edits are never touched.
"""
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from transcoder.database import JobState  # noqa: E402
from transcoder.utils import (  # noqa: E402
    path_is_in_proxies_folder,
    proxies_folder_root,
)


# --- path helpers ---------------------------------------------------------------

def test_sony_style_proxy_matches():
    assert path_is_in_proxies_folder("/HD/card1/Proxies/C8547_Proxy.mov")


def test_any_file_under_proxies_matches():
    # Premiere "Create proxies" output doesn't follow the Sony _Proxy naming.
    assert path_is_in_proxies_folder("/HD/2025-11-13 venus/video/sd1/Proxies/C8547.mov")


def test_nested_proxies_subfolder_matches():
    assert path_is_in_proxies_folder("/HD/x/Proxies/1080p/clip.mov")


def test_case_insensitive():
    assert path_is_in_proxies_folder("/HD/x/PROXIES/clip.mov")
    assert path_is_in_proxies_folder("/HD/x/proxies/clip.mov")


def test_non_proxies_paths_do_not_match():
    assert not path_is_in_proxies_folder("/HD/x/MyProxies/clip.mov")
    assert not path_is_in_proxies_folder("/HD/x/video/clip_Proxy.mov")
    assert not path_is_in_proxies_folder("")


def test_proxies_folder_root_resolution():
    assert proxies_folder_root("/HD/a/Proxies/c.mov") == "/HD/a/Proxies"
    assert proxies_folder_root("/HD/a/Proxies/sub/c.mov") == "/HD/a/Proxies"
    # Outermost Proxies wins (delete takes the whole tree anyway)
    assert proxies_folder_root("/HD/Proxies/x/Proxies/c.mov") == "/HD/Proxies"
    assert proxies_folder_root("/HD/a/video/c.mov") is None


# --- download worker defensive skip ----------------------------------------------

class _FakeDB:
    def __init__(self):
        self.updates = []

    def update_job_state(self, job_id, state, **kw):
        self.updates.append((job_id, state, kw))


def test_download_worker_skips_queued_proxy_job():
    import threading

    from transcoder.workers import DownloadWorker

    worker = object.__new__(DownloadWorker)
    # Workers are Thread subclasses; the name property needs Thread.__init__.
    threading.Thread.__init__(worker, name="downloader-test", daemon=True)
    worker.db = _FakeDB()

    job = SimpleNamespace(
        id=1553,
        dropbox_path="/HeavyDrops/2025-11-13 venus day talks/video/sd1/Proxies/C8547_Proxy.mov",
        dropbox_size=576_480_000,
    )
    worker.process_job(job)

    assert worker.db.updates, "job must be parked, not downloaded"
    job_id, state, kw = worker.db.updates[-1]
    assert state == JobState.SKIPPED_EXCLUDED
    assert "Proxies" in kw.get("error_message", "")


# --- scanner: whole-folder delete -------------------------------------------------

def test_delete_throwaway_is_on_by_default():
    """Operator decision (Transcoder 11): always on. The folder-age gate
    (legacy_reorganize_min_age_days, default 60d) is the only brake."""
    from transcoder.config import ScannerSettings

    assert ScannerSettings().delete_throwaway_files is True

class _FakeDropbox:
    def __init__(self):
        self.deleted: list[str] = []

    def delete_file(self, path: str) -> bool:
        self.deleted.append(path)
        return True


def _bare_scanner(delete_on: bool, min_age_days: int = 30):
    from transcoder.scanner import Scanner

    scanner = object.__new__(Scanner)
    scanner.config = SimpleNamespace(
        scanner=SimpleNamespace(delete_throwaway_files=delete_on),
        legacy_reorganize_min_age_days=min_age_days,
    )
    scanner.dropbox = _FakeDropbox()
    scanner._deleted_proxy_dirs = set()
    scanner._settled_prproj_cache = {}
    return scanner


def _entry(path: str):
    return SimpleNamespace(path=path)


def test_settled_proxies_folder_deleted_once_for_all_siblings(monkeypatch):
    import transcoder.scanner as scanner_mod

    monkeypatch.setattr(
        scanner_mod, "_is_folder_settled",
        lambda dbx, parent, min_age, **kw: SimpleNamespace(
            settled=True, days_since_newest=42.0, threshold_days=min_age,
        ),
    )
    scanner = _bare_scanner(delete_on=True)

    r1 = scanner._process_file(
        _entry("/HD/x/video/sd1/Proxies/C8547_Proxy.mov"), False, None,
    )
    r2 = scanner._process_file(
        _entry("/HD/x/video/sd1/Proxies/C8723_Proxy.mov"), False, None,
    )

    assert r1 == r2 == "skipped_excluded"
    # One delete, targeting the FOLDER — not two file deletes.
    assert scanner.dropbox.deleted == ["/HD/x/video/sd1/Proxies"]


def test_hot_proxies_folder_is_deferred(monkeypatch):
    import transcoder.scanner as scanner_mod

    monkeypatch.setattr(
        scanner_mod, "_is_folder_settled",
        lambda dbx, parent, min_age, **kw: SimpleNamespace(
            settled=False, days_since_newest=2.0, threshold_days=min_age,
        ),
    )
    scanner = _bare_scanner(delete_on=True)

    result = scanner._process_file(
        _entry("/HD/x/Proxies/C0001_Proxy.mov"), False, None,
    )

    assert result == "skipped_excluded"
    assert scanner.dropbox.deleted == [], "active edit must never be deleted"


def test_delete_off_only_skips(monkeypatch):
    scanner = _bare_scanner(delete_on=False)

    result = scanner._process_file(_entry("/HD/x/Proxies/c.mov"), False, None)

    assert result == "skipped_excluded"
    assert scanner.dropbox.deleted == []


def test_dry_run_never_deletes():
    scanner = _bare_scanner(delete_on=True)

    result = scanner._process_file(_entry("/HD/x/Proxies/c.mov"), True, None)

    assert result == "skipped_excluded"
    assert scanner.dropbox.deleted == []
