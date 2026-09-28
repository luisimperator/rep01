"""v8.5.0 daily cold-project sweep: acceptance tests from the briefing.

  - Podfactory proxy_only pair whose project is settled → swapped.
  - Proxies/ seen hot by the scanner and settled later → deleted.
  - Adobe Premiere Pro Audio Previews settled → deleted; Auto-Save untouched.
  - Hot folders are deferred, in-flight folders are left alone, a pause
    stops the pass, nothing inside Proxies/ is swapped.
"""
import sys
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from transcoder.cold_sweep import ColdSweepWorker, run_cold_sweep  # noqa: E402
from transcoder.config import Config  # noqa: E402
from transcoder.database import Database, JobState  # noqa: E402
from transcoder.dropbox_client import DropboxFileInfo  # noqa: E402

ROOT = "/HeavyDrops"
NOW = datetime.now(timezone.utc)


class FakeTree:
    """In-memory Dropbox: files only (folders are implied by paths)."""

    def __init__(self):
        self.files: dict[str, DropboxFileInfo] = {}
        self.deleted: list[str] = []

    def add(self, path, days_ago=100, size=1000):
        when = NOW - timedelta(days=days_ago)
        self.files[path] = DropboxFileInfo(
            path=path, name=PurePosixPath(path).name, size=size, rev="r",
            server_modified=when, client_modified=when,
        )

    def age(self, prefix, days_ago):
        for p in list(self.files):
            if p.lower().startswith(prefix.lower()):
                self.add(p, days_ago, self.files[p].size)

    # --- DropboxClient surface used by the sweep ---
    def list_folder(self, path, recursive=False):
        base = path.rstrip("/").lower()
        for p, info in list(self.files.items()):
            parent = str(PurePosixPath(p).parent).lower()
            if recursive and (parent == base or parent.startswith(base + "/")):
                yield info
            elif not recursive and parent == base:
                yield info

    def create_folder(self, path):
        return None

    def move_file(self, src, dst, allow_overwrite=False):
        info = self.files.pop(src)
        self.files[dst] = DropboxFileInfo(
            path=dst, name=PurePosixPath(dst).name, size=info.size, rev="r",
            server_modified=info.server_modified, client_modified=info.client_modified,
        )

    def read_text_file(self, path):
        return None

    def write_text_file(self, path, text):
        self.add(path, 0, len(text))

    def delete_file(self, path):
        low = path.lower().rstrip("/")
        gone = [p for p in self.files if p.lower() == low or p.lower().startswith(low + "/")]
        for p in gone:
            del self.files[p]
        self.deleted.append(path)
        return bool(gone)


def _cfg(tmp_path, **kw):
    data = dict(
        dropbox_root=ROOT,
        local_staging_dir=str(tmp_path / "s"),
        database_path=str(tmp_path / "t.db"),
        legacy_reorganize_min_age_days=60,
        legacy_reorganize_delete_h264_after_seconds=0,
        legacy_reorganize_delete_wav_after_seconds=0,
        cleanup_dot_underscore=False,
    )
    data.update(kw)
    return Config(**data)


def _db(tmp_path):
    db = Database(tmp_path / "t.db")
    db.initialize()
    return db


EP = f"{ROOT}/Podfactory3/Pós HeavyDrops/ep 12"
ISO = f"{EP}/Video ISO Files"


def _podfactory_pair(tree, project_days):
    tree.add(f"{EP}/projeto/ep12.prproj", project_days)
    tree.add(f"{ISO}/CAM 1.mp4", 90, size=40_000)
    tree.add(f"{ISO}/h265/CAM 1.mp4", 80, size=700)


# ------------------------------------------------------------------ swaps

class TestDeferredSwaps:
    def test_settled_fast_lane_pair_is_swapped(self, tmp_path):
        tree = FakeTree()
        _podfactory_pair(tree, project_days=70)
        res = run_cold_sweep(_cfg(tmp_path), _db(tmp_path), tree)
        assert res.status == "done"
        assert res.swapped_pairs == 1
        assert f"{ISO}/h264/CAM 1.mp4" in tree.files          # original backed up
        assert tree.files[f"{ISO}/CAM 1.mp4"].size == 700      # H.265 in its spot
        assert f"{ISO}/h265/CAM 1.mp4" not in tree.files

    def test_hot_project_in_projeto_is_deferred(self, tmp_path):
        tree = FakeTree()
        _podfactory_pair(tree, project_days=5)
        res = run_cold_sweep(_cfg(tmp_path), _db(tmp_path), tree)
        assert res.swapped_pairs == 0
        assert res.deferred_count == 1 and "still active" in res.deferred[0]["reason"]
        assert f"{ISO}/h265/CAM 1.mp4" in tree.files

    def test_folder_with_job_in_flight_is_left_alone(self, tmp_path):
        tree = FakeTree()
        _podfactory_pair(tree, project_days=70)
        tree.add(f"{ISO}/CAM 2.mp4", 90)
        db = _db(tmp_path)
        db.create_job(f"{ISO}/CAM 2.mp4", "r", 1, "o", state=JobState.DOWNLOADING)
        res = run_cold_sweep(_cfg(tmp_path), db, tree)
        assert res.swapped_pairs == 0
        assert "in flight" in res.deferred[0]["reason"]

    def test_fast_lane_never_swaps_with_zero_threshold(self, tmp_path):
        tree = FakeTree()
        _podfactory_pair(tree, project_days=1)
        res = run_cold_sweep(_cfg(tmp_path, legacy_reorganize_min_age_days=0),
                             _db(tmp_path), tree)
        assert res.swapped_pairs == 0

    def test_db_output_path_follows_the_swap(self, tmp_path):
        tree = FakeTree()
        _podfactory_pair(tree, project_days=70)
        db = _db(tmp_path)
        job = db.create_job(f"{ISO}/CAM 1.mp4", "r", 1, f"{ISO}/h265/CAM 1.mp4",
                            state=JobState.DONE)
        run_cold_sweep(_cfg(tmp_path), db, tree)
        assert db.get_job(job.id).output_path == f"{ISO}/CAM 1.mp4"

    def test_audio_pairs_swap_too(self, tmp_path):
        tree = FakeTree()
        audio = f"{EP}/Audio Source Files"
        tree.add(f"{EP}/projeto/ep12.prproj", 70)
        tree.add(f"{audio}/MIC 1.wav", 90)
        tree.add(f"{audio}/mp3/MIC 1.mp3", 80)
        res = run_cold_sweep(_cfg(tmp_path), _db(tmp_path), tree)
        assert res.swapped_pairs == 1
        assert f"{audio}/wav/MIC 1.wav" in tree.files and f"{audio}/MIC 1.mp3" in tree.files

    def test_nothing_inside_proxies_is_swapped(self, tmp_path):
        tree = FakeTree()
        prod = f"{ROOT}/Externa/rec/video/cam1/Proxies/H265 Prod"
        tree.add(f"{prod}/C0001.MP4", 90)
        tree.add(f"{prod}/h265/C0001.MP4", 90)
        res = run_cold_sweep(_cfg(tmp_path), _db(tmp_path), tree)
        assert res.swapped_pairs == 0


# ---------------------------------------------------------------- throwaway

REC = f"{ROOT}/Externa/2026-06-01 gravacao"


class TestThrowawayCleanup:
    def test_proxies_seen_hot_by_scanner_deleted_once_settled(self, tmp_path):
        from unittest.mock import MagicMock

        from transcoder.scanner import Scanner

        tree = FakeTree()
        tree.add(f"{REC}/projeto/edit.prproj", 5)
        tree.add(f"{REC}/video/cam1/C0001.MP4", 5)
        tree.add(f"{REC}/video/cam1/Proxies/C0001_Proxy.mp4", 5)
        cfg = _cfg(tmp_path)
        db = _db(tmp_path)

        # The scan sees the proxy while the project is hot: skip, no delete.
        scanner = Scanner(cfg, db, tree)
        proxy = tree.files[f"{REC}/video/cam1/Proxies/C0001_Proxy.mp4"]
        assert scanner._process_file(proxy, False, cfg.stability_profiles.steady) == "skipped_excluded"
        assert tree.deleted == []
        # Delta mode never delivers it again. Weeks later the project is cold:
        tree.age(REC, 70)
        res = run_cold_sweep(cfg, db, tree)
        assert tree.deleted == [f"{REC}/video/cam1/Proxies"]
        assert res.deleted_count == 1
        assert f"{REC}/video/cam1/C0001.MP4" in tree.files     # media untouched

    def test_hot_proxies_are_kept(self, tmp_path):
        tree = FakeTree()
        tree.add(f"{REC}/projeto/edit.prproj", 5)
        tree.add(f"{REC}/video/cam1/Proxies/C0001_Proxy.mp4", 100)
        res = run_cold_sweep(_cfg(tmp_path), _db(tmp_path), tree)
        assert tree.deleted == [] and res.deferred_count == 1

    def test_audio_previews_deleted_auto_save_untouched(self, tmp_path):
        tree = FakeTree()
        proj = f"{REC}/projeto"
        tree.add(f"{proj}/edit.prproj", 70)
        tree.add(f"{proj}/Adobe Premiere Pro Audio Previews/edit.PRV/a.cfa", 70)
        tree.add(f"{proj}/Adobe Premiere Pro Video Previews/edit.PRV/r.mov", 70)
        tree.add(f"{proj}/Adobe Premiere Pro Auto-Save/edit--3.prproj", 70)
        run_cold_sweep(_cfg(tmp_path), _db(tmp_path), tree)
        assert sorted(tree.deleted) == sorted([
            f"{proj}/Adobe Premiere Pro Audio Previews",
            f"{proj}/Adobe Premiere Pro Video Previews",
        ])
        assert f"{proj}/Adobe Premiere Pro Auto-Save/edit--3.prproj" in tree.files

    def test_nested_only_root_uses_newest_file_below(self, tmp_path):
        """A Premiere Proxies root holding only subfolders must not read as
        'empty → settled' while its files are fresh (no .prproj anywhere)."""
        tree = FakeTree()
        tree.add(f"{REC}/video/cam1/Proxies/1080p/C0001.mov", 3)
        run_cold_sweep(_cfg(tmp_path), _db(tmp_path), tree)
        assert tree.deleted == []

    def test_respects_delete_throwaway_files_off(self, tmp_path):
        tree = FakeTree()
        tree.add(f"{REC}/video/cam1/Proxies/C0001_Proxy.mp4", 100)
        run_cold_sweep(_cfg(tmp_path, scanner={"delete_throwaway_files": False}),
                       _db(tmp_path), tree)
        assert tree.deleted == []


# ------------------------------------------------------------ pause + schedule

class TestPauseAndSchedule:
    def test_pause_stops_the_pass(self, tmp_path):
        tree = FakeTree()
        _podfactory_pair(tree, project_days=70)
        res = run_cold_sweep(_cfg(tmp_path), _db(tmp_path), tree,
                             should_continue=lambda: False)
        assert res.status == "aborted"
        assert res.swapped_pairs == 0 and f"{ISO}/h265/CAM 1.mp4" in tree.files

    def _worker(self, tmp_path, paused=False):
        from unittest.mock import MagicMock
        disp = MagicMock()
        disp.is_paused.return_value = paused
        return ColdSweepWorker(_cfg(tmp_path), _db(tmp_path), FakeTree(),
                               threading.Event(), dispatcher=disp,
                               state_path=tmp_path / "cs.json")

    def test_due_inside_catch_up_window_only(self, tmp_path):
        w = self._worker(tmp_path)
        day = datetime(2026, 9, 28)
        assert not w._due(day.replace(hour=2, minute=59))
        assert w._due(day.replace(hour=3, minute=0))
        assert w._due(day.replace(hour=8, minute=59))
        assert not w._due(day.replace(hour=9, minute=1))      # past 6h catch-up
        w._last_scheduled_date = "2026-09-28"
        assert not w._due(day.replace(hour=3, minute=5))       # already ran today

    def test_run_now_refused_while_paused(self, tmp_path):
        ok, err = self._worker(tmp_path, paused=True).trigger_now()
        assert not ok and "paused" in err

    def test_scheduled_run_records_day_and_persists(self, tmp_path):
        w = self._worker(tmp_path)
        w._run("scheduled")
        st = w.status()
        assert st["last"]["status"] == "done"
        assert st["last_scheduled_date"] is not None
        again = self._worker(tmp_path)                         # after a restart
        assert again.status()["last_scheduled_date"] == st["last_scheduled_date"]

    def test_aborted_scheduled_run_retries(self, tmp_path):
        w = self._worker(tmp_path, paused=True)
        w._run("scheduled")
        assert w.status()["last"]["status"] == "aborted"
        assert w.status()["last_scheduled_date"] is None
