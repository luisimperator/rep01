"""Tests for the Podfactory fast lane (v8.4.0).

Operator request: "os videos da Podfactory sejam feitos o h265 o mais rapido
que eles estiverem la, só não apagar o h264 ainda, nem substituir na pasta
principal" — the editors cut on the H.265 while the jumbo original syncs.

Pinned contract:
  1. Steady-state (delta) scans recheck WAITING files every scan. Before
     v8.4.0 a new file got one stability check and was never re-delivered by
     the delta, so it waited forever.
  2. Priority files need only 2 checks, 5 min apart.
  3. Priority jobs jump to the head of every queue, even a full one, and
     never claim or respect the sticky folder.
  4. When all downloaders are busy with backlog work, exactly one yields
     per cooldown window.
  5. Proxy-only: the post-upload reorganize never runs for priority files.
"""
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from queue import Full
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from transcoder.config import Config, path_matches_any  # noqa: E402
from transcoder.database import Database, JobState  # noqa: E402
from transcoder.dispatcher import (  # noqa: E402
    DOWNLOAD_STATES,
    JobDispatcher,
    JobQueue,
)
from transcoder.dropbox_client import DropboxFileInfo  # noqa: E402
from transcoder.scanner import Scanner  # noqa: E402

ROOT = "/HeavyDrops"
POD = f"{ROOT}/Podfactory3/Pós HeavyDrops/2026-09-02 slot1 g1 - Alpha7 -/video/Video ISO Files"
BACKLOG = f"{ROOT}/Riachuelo/2025-08-21 compliance week/video/Video ISO Files"


def _config(tmp_path, **overrides) -> Config:
    data = dict(
        dropbox_root=ROOT,
        min_size_gb=0.0,
        local_staging_dir=str(tmp_path / "staging"),
        database_path=str(tmp_path / "t.db"),
    )
    data.update(overrides)
    return Config(**data)


def _db(tmp_path) -> Database:
    db = Database(tmp_path / "t.db")
    db.initialize()
    return db


def _info(path: str, size: int = 10_000, rev: str = "r1") -> DropboxFileInfo:
    return DropboxFileInfo(
        path=path,
        name=path.rsplit("/", 1)[-1],
        size=size,
        rev=rev,
        server_modified=datetime(2026, 9, 2, 12, 0, tzinfo=timezone.utc),
    )


def _age_checks(db: Database, minutes: int) -> None:
    """Pretend every recorded stability check happened `minutes` ago."""
    with db.transaction() as conn:
        conn.execute(
            "UPDATE stability_checks SET check_time = datetime('now', ?)",
            (f"-{minutes} minutes",),
        )


class FakeDropbox:
    """Just enough of DropboxClient for a delta scan."""

    def __init__(self):
        self.files: dict[str, DropboxFileInfo] = {}
        self.delta: list = []
        self.metadata_calls: list[str] = []

    def list_folder_delta(self, cursor):
        items, self.delta = self.delta, []
        for info in items:
            yield ("file", info)
        yield "cursor-next"

    def get_metadata(self, path):
        self.metadata_calls.append(path)
        return self.files.get(path)

    def read_text_file(self, path):
        return None

    def file_exists(self, path):
        return False


def _delta_scanner(tmp_path, **cfg):
    config = _config(tmp_path, **cfg)
    db = _db(tmp_path)
    db.get_scan_state(ROOT)
    db.save_scan_cursor("cursor-0", 0)
    db.mark_bulk_complete()
    dbx = FakeDropbox()
    return Scanner(config, db, dbx), db, dbx


def _arrive(dbx: FakeDropbox, info: DropboxFileInfo) -> None:
    """A file lands in Dropbox: the delta delivers it once, then never again."""
    dbx.files[info.path] = info
    dbx.delta.append(info)


# --------------------------------------------------------------- path matching

class TestPathMatching:
    def test_default_pattern_matches_any_podfactory_folder(self, tmp_path):
        c = _config(tmp_path)
        assert c.is_priority_path(f"{POD}/CAM 1.mp4")
        assert c.is_priority_path(f"{ROOT}/Podfactory/ep 12/video/a.MP4")
        assert c.is_priority_path(f"{ROOT}/PODFACTORY2/x.mov")

    def test_other_folders_do_not_match(self, tmp_path):
        c = _config(tmp_path)
        assert not c.is_priority_path(f"{BACKLOG}/CAM 1.mp4")
        # A loose file merely named podfactory is not a Podfactory folder.
        assert not c.is_priority_path(f"{ROOT}/Clientes/podfactory.mp4")

    def test_empty_patterns_disable_the_lane(self, tmp_path):
        c = _config(tmp_path, priority={"paths": []})
        assert not c.is_priority_path(f"{POD}/CAM 1.mp4")
        assert not path_matches_any(f"{POD}/CAM 1.mp4", [])

    def test_sql_glob_agrees_with_python_match(self, tmp_path):
        db = _db(tmp_path)
        for i, p in enumerate([f"{POD}/CAM 1.mp4", f"{BACKLOG}/CAM 1.mp4",
                               f"{ROOT}/PODFACTORY2/x.mov"]):
            db.create_job(p, f"r{i}", 1, f"{p}.out")
        jobs = db.get_dispatchable_jobs(
            DOWNLOAD_STATES, limit=10, path_globs=["*/podfactory*/*"],
        )
        assert sorted(j.dropbox_path for j in jobs) == sorted(
            [f"{POD}/CAM 1.mp4", f"{ROOT}/PODFACTORY2/x.mov"]
        )


# ------------------------------------------------------ scanner: waiting recheck

class TestWaitingRecheck:
    def test_backlog_file_is_no_longer_stuck_waiting(self, tmp_path):
        """The latent bug: delta delivers a file once; it must still become
        a job once it has stayed unchanged long enough."""
        s, db, dbx = _delta_scanner(tmp_path)
        path = f"{BACKLOG}/CAM 9.mp4"
        _arrive(dbx, _info(path))

        stats = s.scan()                      # check 1 (from the delta)
        assert stats["waiting_stable"] == 1 and stats["new"] == 0
        _age_checks(db, 10)
        s.scan()                              # check 2 (recheck)
        assert db.get_job_by_path(path) is None
        _age_checks(db, 20)
        stats = s.scan()                      # check 3 → steady profile met
        assert stats["new"] == 1
        job = db.get_job_by_path(path)
        assert job is not None and job.state == JobState.NEW
        # Queued files leave the recheck list.
        assert db.get_pending_stability_paths(10) == []

    def test_file_stranded_for_weeks_is_recovered(self, tmp_path):
        """Files stuck WAITING by the pre-8.4.0 bug must not be purged —
        they become jobs within two scans after the update."""
        s, db, dbx = _delta_scanner(tmp_path)
        path = f"{BACKLOG}/CAM 9.mp4"
        _arrive(dbx, _info(path))
        s.scan()
        _age_checks(db, 30 * 24 * 60)         # stranded for 30 days
        s.scan()
        stats = s.scan()
        assert stats["new"] == 1
        assert db.get_job_by_path(path) is not None

    def test_priority_file_needs_only_two_checks(self, tmp_path):
        s, db, dbx = _delta_scanner(tmp_path)
        path = f"{POD}/CAM 1.mp4"
        _arrive(dbx, _info(path))

        s.scan()
        assert db.get_job_by_path(path) is None
        _age_checks(db, 6)                    # ≥ 5 min since first sighting
        stats = s.scan()
        assert stats["new"] == 1
        assert db.get_job_by_path(path) is not None

    def test_priority_file_still_needs_min_age(self, tmp_path):
        s, db, dbx = _delta_scanner(tmp_path)
        path = f"{POD}/CAM 1.mp4"
        _arrive(dbx, _info(path))
        s.scan()
        stats = s.scan()                      # second check but only seconds later
        assert stats["new"] == 0
        assert db.get_job_by_path(path) is None

    def test_file_still_growing_restarts_the_clock(self, tmp_path):
        s, db, dbx = _delta_scanner(tmp_path)
        path = f"{POD}/CAM 1.mp4"
        _arrive(dbx, _info(path, size=100, rev="r1"))
        s.scan()
        _age_checks(db, 6)
        dbx.files[path] = _info(path, size=200, rev="r2")   # still uploading
        stats = s.scan()
        assert stats["new"] == 0 and db.get_job_by_path(path) is None

    def test_deleted_file_is_forgotten(self, tmp_path):
        s, db, dbx = _delta_scanner(tmp_path)
        path = f"{POD}/CAM 1.mp4"
        _arrive(dbx, _info(path))
        s.scan()
        del dbx.files[path]
        s.scan()
        assert db.get_pending_stability_paths(10) == []

    def test_priority_rechecked_first(self, tmp_path):
        s, db, dbx = _delta_scanner(tmp_path)
        s._RECHECK_GENERAL_LIMIT = 0          # no general budget at all
        pod = f"{POD}/CAM 1.mp4"
        _arrive(dbx, _info(f"{BACKLOG}/CAM 9.mp4"))
        _arrive(dbx, _info(pod))
        s.scan()
        dbx.metadata_calls.clear()
        s.scan()
        assert dbx.metadata_calls == [pod]

    def test_unreachable_dropbox_defers_rest_of_recheck(self, tmp_path):
        s, db, dbx = _delta_scanner(tmp_path)
        for i in range(6):
            _arrive(dbx, _info(f"{BACKLOG}/CAM {i}.mp4"))
        s.scan()

        calls = []

        def boom(path):
            calls.append(path)
            raise OSError("network down")

        dbx.get_metadata = boom
        s.scan()                              # must not raise
        assert len(calls) == 3
        assert len(db.get_pending_stability_paths(10)) == 6


# ---------------------------------------------------------------- queue order

def _job(job_id: int, path: str, kind: str = "video"):
    return SimpleNamespace(id=job_id, dropbox_path=path, kind=kind,
                           dropbox_size=1, created_at=job_id)


def _is_pod(job) -> bool:
    return "podfactory" in job.dropbox_path.lower()


class TestJobQueue:
    def test_priority_jumps_ahead_fifo_among_themselves(self):
        q = JobQueue(10, _is_pod)
        q.put_nowait(_job(1, f"{BACKLOG}/a.mp4"))
        q.put_nowait(_job(2, f"{BACKLOG}/b.mp4"))
        q.put_nowait(_job(3, f"{POD}/1.mp4"))
        q.put_nowait(_job(4, f"{POD}/2.mp4"))
        assert q.priority_count == 2
        assert [q.get_nowait().id for _ in range(4)] == [3, 4, 1, 2]
        assert q.priority_count == 0

    def test_get_priority_never_returns_backlog(self):
        from queue import Empty

        q = JobQueue(10, _is_pod)
        q.put_nowait(_job(1, f"{BACKLOG}/a.mp4"))
        with pytest.raises(Empty):
            q.get_priority(timeout=0.05)
        # The backlog job is still there for a regular worker.
        q.put_nowait(_job(2, f"{POD}/1.mp4"))
        assert q.get_priority(timeout=0.05).id == 2
        assert q.get_nowait().id == 1

    def test_get_priority_wakes_on_priority_put(self):
        q = JobQueue(10, _is_pod)
        got = []
        t = threading.Thread(target=lambda: got.append(q.get_priority(timeout=2)))
        t.start()
        q.put_nowait(_job(1, f"{BACKLOG}/a.mp4"))
        q.put_overflow(_job(2, f"{POD}/1.mp4"))
        t.join(3)
        assert [j.id for j in got] == [2]
        assert q.get_nowait().id == 1

    def test_overflow_bounded_at_double(self):
        q = JobQueue(2, _is_pod)
        q.put_nowait(_job(1, f"{BACKLOG}/a.mp4"))
        q.put_nowait(_job(2, f"{BACKLOG}/b.mp4"))
        with pytest.raises(Full):
            q.put_nowait(_job(3, f"{BACKLOG}/c.mp4"))
        q.put_overflow(_job(4, f"{POD}/1.mp4"))
        q.put_overflow(_job(5, f"{POD}/2.mp4"))
        with pytest.raises(Full):
            q.put_overflow(_job(6, f"{POD}/3.mp4"))
        assert q.get_nowait().id == 4


# ------------------------------------------------------------ dispatcher refill

class TestDispatcherFastLane:
    def _dispatcher(self, tmp_path, **cfg):
        config = _config(tmp_path, **cfg)
        db = _db(tmp_path)
        d = JobDispatcher(config, db, threading.Event())
        return d, db

    def test_priority_job_bypasses_full_queue_and_sticky(self, tmp_path):
        d, db = self._dispatcher(tmp_path)
        # A big backlog folder is sticky and the download queue is full.
        for i in range(40):
            db.create_job(f"{BACKLOG}/CAM {i}.mp4", "r", 40 * 1024**3,
                          f"{BACKLOG}/h265/CAM {i}.mp4")
        d._refill(d.download_q, DOWNLOAD_STATES, prioritize_folder=True)
        assert d.download_q.full()
        sticky = d.sticky_folder()
        assert sticky == BACKLOG

        pod = db.create_job(f"{POD}/CAM 1.mp4", "r", 20 * 1024**3,
                            f"{POD}/h265/CAM 1.mp4")
        d._refill(d.download_q, DOWNLOAD_STATES, prioritize_folder=True)

        assert d.download_q.get_nowait().id == pod.id
        assert d.sticky_folder() == sticky            # never claims the sticky
        assert pod.id in d._priority_ids

    def test_priority_job_reaches_transcode_head(self, tmp_path):
        d, db = self._dispatcher(tmp_path)
        for i in range(12):
            j = db.create_job(f"{BACKLOG}/CAM {i}.mp4", "r", 1, f"o{i}")
            db.update_job_state(j.id, JobState.DOWNLOADED)
        pod = db.create_job(f"{POD}/CAM 1.mp4", "r", 1, "op")
        db.update_job_state(pod.id, JobState.DOWNLOADED)
        from transcoder.dispatcher import TRANSCODE_STATES

        d._refill(d.transcode_q, TRANSCODE_STATES, kind="video")
        # The dedicated fast-lane transcoder picks it straight away.
        assert d.transcode_q.get_priority(timeout=0.1).id == pod.id

    def test_lane_disabled_keeps_plain_fifo(self, tmp_path):
        d, db = self._dispatcher(tmp_path, priority={"paths": []})
        first = db.create_job(f"{BACKLOG}/CAM 0.mp4", "r", 1, "o0")
        db.create_job(f"{POD}/CAM 1.mp4", "r", 1, "o1")
        d._refill(d.download_q, DOWNLOAD_STATES, prioritize_folder=True)
        assert d.download_q.get_nowait().id == first.id


# --------------------------------------------------------------- preemption

class TestPreemption:
    def _busy(self, tmp_path, **cfg):
        config = _config(tmp_path, **cfg)
        d = JobDispatcher(config, _db(tmp_path), threading.Event())
        # All 4 downloaders busy on backlog jobs 1..4.
        d._download_active = {f"downloader-{i}": i + 1 for i in range(4)}
        return d

    def test_no_yield_without_waiting_priority_job(self, tmp_path):
        d = self._busy(tmp_path)
        assert not d.should_yield_download("downloader-0")

    def test_exactly_one_yield_per_cooldown(self, tmp_path):
        d = self._busy(tmp_path)
        d.download_q.put_overflow(_job(99, f"{POD}/CAM 1.mp4"))
        assert d.should_yield_download("downloader-0")
        assert not d.should_yield_download("downloader-1")
        d._last_preempt_at -= 61
        assert d.should_yield_download("downloader-1")

    def test_priority_download_never_yields(self, tmp_path):
        d = self._busy(tmp_path)
        d._priority_ids.add(1)                # downloader-0 is on a priority job
        d.download_q.put_overflow(_job(99, f"{POD}/CAM 1.mp4"))
        assert not d.should_yield_download("downloader-0")

    def test_idle_downloader_means_no_preemption(self, tmp_path):
        d = self._busy(tmp_path)
        d._download_active.pop("downloader-3")
        d.download_q.put_overflow(_job(99, f"{POD}/CAM 1.mp4"))
        assert not d.should_yield_download("downloader-0")

    def test_backlog_held_while_priority_downloads(self, tmp_path):
        d = self._busy(tmp_path)
        assert not d.should_throttle_for_priority("downloader-1")
        d._priority_ids.add(1)                # downloader-0 pulls a Podfactory file
        assert not d.should_throttle_for_priority("downloader-0")
        assert d.should_throttle_for_priority("downloader-1")
        assert d.should_throttle_for_priority("downloader-3")
        # Priority download finished → backlog runs free again.
        d._download_active.pop("downloader-0")
        assert not d.should_throttle_for_priority("downloader-1")

    def test_backlog_hold_can_be_disabled(self, tmp_path):
        d = self._busy(tmp_path, priority={"throttle_backlog_downloads": False})
        d._priority_ids.add(1)
        assert not d.should_throttle_for_priority("downloader-1")

    def test_preemption_can_be_disabled(self, tmp_path):
        d = self._busy(tmp_path, priority={"preempt_downloads": False})
        d.download_q.put_overflow(_job(99, f"{POD}/CAM 1.mp4"))
        assert not d.should_yield_download("downloader-0")


# ------------------------------------------------- downloader progress callback

class TestDownloaderCallback:
    def _worker(self, tmp_path, dispatcher):
        from transcoder.workers import DownloadWorker

        w = object.__new__(DownloadWorker)
        threading.Thread.__init__(w, name="downloader-0", daemon=True)
        w.config = dispatcher.config
        w.stop_event = threading.Event()
        w.dispatcher = dispatcher
        w._abort_download = threading.Event()
        w._progress_marker = None
        return w

    def test_backlog_download_yields_to_waiting_priority_file(self, tmp_path):
        from transcoder.workers import DownloadYielded

        d = JobDispatcher(_config(tmp_path), _db(tmp_path), threading.Event())
        d._download_active = {f"downloader-{i}": i + 1 for i in range(4)}
        d.download_q.put_overflow(_job(99, f"{POD}/CAM 1.mp4"))
        w = self._worker(tmp_path, d)
        cb = w._make_progress_callback(_job(1, f"{BACKLOG}/CAM 0.mp4"))
        with pytest.raises(DownloadYielded):
            cb(8 * 1024**2, 40 * 1024**3)

    def test_backlog_download_held_then_released(self, tmp_path, monkeypatch):
        import transcoder.workers as wk

        d = JobDispatcher(_config(tmp_path), _db(tmp_path), threading.Event())
        d._download_active = {"downloader-0": 1, "downloader-1": 2}
        d._priority_ids.add(2)                # downloader-1 is on Podfactory
        w = self._worker(tmp_path, d)
        sleeps = []

        def fake_sleep(sec):
            sleeps.append(sec)
            if len(sleeps) == 3:              # priority download finishes
                d._download_active.pop("downloader-1")

        monkeypatch.setattr(wk.time, "sleep", fake_sleep)
        cb = w._make_progress_callback(_job(1, f"{BACKLOG}/CAM 0.mp4"))
        cb(8 * 1024**2, 40 * 1024**3)         # returns once the hold clears
        assert len(sleeps) == 3


# ------------------------------------------------------------- proxy-only upload

class TestProxyOnlyUpload:
    def _upload(self, tmp_path, path, **cfg):
        from transcoder.workers import UploadWorker

        config = _config(tmp_path, delete_staging_after_upload=False, **cfg)
        w = object.__new__(UploadWorker)
        threading.Thread.__init__(w, name="uploader-0", daemon=True)
        w.config = config
        w.db = MagicMock()
        w.dropbox = MagicMock()
        w._make_progress_callback = lambda job, size: None
        w._try_reorganize_folder = MagicMock()

        out = tmp_path / "output.mp4"
        out.write_bytes(b"x" * 10)
        parent, name = path.rsplit("/", 1)
        job = SimpleNamespace(
            id=7, dropbox_path=path, kind="video",
            local_output_path=str(out), output_path=f"{parent}/h265/{name}",
        )
        w._upload_job(job)
        return w

    def test_priority_file_is_never_reorganized(self, tmp_path):
        w = self._upload(tmp_path, f"{POD}/CAM 1.mp4")
        w._try_reorganize_folder.assert_not_called()
        # It still uploads to the h265/ sibling.
        assert w.dropbox.upload_file.call_args[0][1] == f"{POD}/h265/CAM 1.mp4"

    def test_backlog_file_still_reorganizes(self, tmp_path):
        w = self._upload(tmp_path, f"{BACKLOG}/CAM 1.mp4")
        w._try_reorganize_folder.assert_called_once()

    def test_proxy_only_off_restores_reorganize(self, tmp_path):
        w = self._upload(tmp_path, f"{POD}/CAM 1.mp4",
                         priority={"proxy_only": False})
        w._try_reorganize_folder.assert_called_once()
