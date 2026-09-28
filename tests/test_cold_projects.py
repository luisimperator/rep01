"""v8.5.0: cold-project housekeeping, Talks by Leo in the fast lane,
newest-recording-first ordering, and no size/bitrate floor for the fast lane.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from transcoder.utils import (  # noqa: E402
    path_is_premiere_preview,
    premiere_preview_root,
)


# --------------------------------------------------------- Premiere previews

class TestPremierePreviews:
    def test_audio_previews_are_throwaway(self):
        p = "/HD/rec/projeto/Adobe Premiere Pro Audio Previews/edit.PRV/a.cfa"
        assert path_is_premiere_preview(p)
        assert premiere_preview_root(p) == "/HD/rec/projeto/Adobe Premiere Pro Audio Previews"

    def test_video_previews_still_throwaway(self):
        p = "/HD/rec/Adobe Premiere Pro Video Previews/x.PRV/Rendered - 1.mov"
        assert path_is_premiere_preview(p)
        assert premiere_preview_root(p) == "/HD/rec/Adobe Premiere Pro Video Previews"

    def test_auto_save_is_never_throwaway(self):
        p = "/HD/rec/projeto/Adobe Premiere Pro Auto-Save/edit--12.prproj"
        assert not path_is_premiere_preview(p)
        assert premiere_preview_root(p) is None

    def test_case_insensitive(self):
        assert path_is_premiere_preview("/HD/x/adobe premiere pro audio previews/a.cfa")


# ------------------------------------------------------------- Talks by Leo

TALKS = "/HeavyDrops/Leo Kuba/Talks by Leo/Arquivo Talks by Leo/ep 40/Video ISO Files/CAM 1.mp4"


def _cfg(tmp_path=None, **kw):
    from transcoder.config import Config
    return Config(dropbox_root="/HeavyDrops", **kw)


class TestTalksByLeo:
    def test_default_includes_talks_by_leo(self):
        assert _cfg().is_priority_path(TALKS)

    def test_other_leo_folders_are_not_fast_lane(self):
        assert not _cfg().is_priority_path(
            "/HeavyDrops/Leo Kuba/Talks by Leo/Cortes/c1.mp4")

    def test_old_local_config_still_gets_talks(self):
        """A machine whose config.yaml pinned the 8.4.0 list must not miss
        the new default."""
        c = _cfg(priority={"paths": ["*/podfactory*/*"]})
        assert c.is_priority_path(TALKS)
        assert c.priority.paths.count("*/podfactory*/*") == 1

    def test_empty_list_still_disables(self):
        assert not _cfg(priority={"paths": []}).is_priority_path(TALKS)

    def test_opt_out_of_defaults(self):
        c = _cfg(priority={"paths": ["*/x/*"], "include_default_paths": False})
        assert not c.is_priority_path(TALKS)


# ------------------------------------------------ fast lane: newest recording first

import sqlite3  # noqa: E402
import threading  # noqa: E402
from datetime import datetime, timedelta, timezone  # noqa: E402

from transcoder.database import Database, JobState  # noqa: E402
from transcoder.dispatcher import (  # noqa: E402
    DOWNLOAD_STATES,
    TRANSCODE_STATES,
    JobDispatcher,
)

POD = "/HeavyDrops/Podfactory3/Pós HeavyDrops"
NOW = datetime.now(timezone.utc)


def _db(tmp_path) -> Database:
    db = Database(tmp_path / "t.db")
    db.initialize()
    return db


def _dispatcher(tmp_path):
    from transcoder.config import Config
    cfg = Config(dropbox_root="/HeavyDrops", min_size_gb=0.0,
                 local_staging_dir=str(tmp_path / "s"))
    db = _db(tmp_path)
    return JobDispatcher(cfg, db, threading.Event()), db


def _ep(db, episode, cam, days_ago, state=JobState.NEW):
    path = f"{POD}/{episode}/Video ISO Files/{cam}.mp4"
    j = db.create_job(path, "r", 1, f"{path}.h265",
                      source_modified=NOW - timedelta(days=days_ago))
    if state != JobState.NEW:
        db.update_job_state(j.id, state)
    return j


class TestNewestRecordingFirst:
    def test_todays_episode_downloads_before_last_weeks(self, tmp_path):
        d, db = _dispatcher(tmp_path)
        old = [_ep(db, "ep 11", f"CAM {i}", 7) for i in range(3)]   # found first
        new = [_ep(db, "ep 12", f"CAM {i}", 0) for i in range(3)]
        d._refill(d.download_q, DOWNLOAD_STATES, prioritize_folder=True)
        order = [d.download_q.get_nowait().id for _ in range(6)]
        assert order == [j.id for j in new] + [j.id for j in old]

    def test_todays_episode_transcodes_first(self, tmp_path):
        d, db = _dispatcher(tmp_path)
        old = _ep(db, "ep 11", "CAM 1", 7, JobState.DOWNLOADED)
        new = _ep(db, "ep 12", "CAM 1", 0, JobState.DOWNLOADED)
        d._refill(d.transcode_q, TRANSCODE_STATES, kind="video")
        assert d.transcode_q.get_priority(timeout=0.1).id == new.id
        assert d.transcode_q.get_priority(timeout=0.1).id == old.id

    def test_limit_keeps_the_newest(self, tmp_path):
        d, db = _dispatcher(tmp_path)
        for i in range(60):                       # a big old catch-up
            _ep(db, f"old ep {i}", "CAM 1", 30 + i)
        new = _ep(db, "ep 99", "CAM 1", 0)
        jobs = db.get_dispatchable_jobs(
            DOWNLOAD_STATES, limit=50, path_globs=list(d.config.priority.paths),
            newest_first=True,
        )
        assert jobs[0].id == new.id

    def test_old_rows_fall_back_to_discovery_time(self, tmp_path):
        d, db = _dispatcher(tmp_path)
        legacy = db.create_job(f"{POD}/legacy/CAM 1.mp4", "r", 1, "o")  # no date
        dated_old = _ep(db, "ep 1", "CAM 1", 400)
        jobs = db.get_dispatchable_jobs(
            DOWNLOAD_STATES, limit=10, path_globs=list(d.config.priority.paths),
            newest_first=True,
        )
        assert [j.id for j in jobs] == [legacy.id, dated_old.id]
        assert legacy.source_modified is None
        assert legacy.recorded_at == legacy.created_at

    def test_backlog_order_unchanged(self, tmp_path):
        d, db = _dispatcher(tmp_path)
        a = db.create_job("/HeavyDrops/X/a.mp4", "r", 1, "o",
                          source_modified=NOW - timedelta(days=900))
        b = db.create_job("/HeavyDrops/X/b.mp4", "r", 1, "o",
                          source_modified=NOW)
        jobs = db.get_dispatchable_jobs(DOWNLOAD_STATES, limit=10)
        assert [j.id for j in jobs] == [a.id, b.id]      # still discovery FIFO


class TestMigration:
    def test_pre_850_database_gains_column(self, tmp_path):
        path = tmp_path / "old.db"
        conn = sqlite3.connect(path)
        conn.execute(
            "CREATE TABLE jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "dropbox_path TEXT NOT NULL, dropbox_rev TEXT NOT NULL, "
            "dropbox_size INTEGER NOT NULL, output_path TEXT NOT NULL, "
            "state TEXT NOT NULL DEFAULT 'NEW', retry_count INTEGER NOT NULL DEFAULT 0, "
            "error_message TEXT, local_input_path TEXT, local_output_path TEXT, "
            "input_codec TEXT, output_codec TEXT, input_duration_sec REAL, "
            "output_duration_sec REAL, input_bitrate_kbps INTEGER, "
            "output_bitrate_kbps INTEGER, encoder_used TEXT, transcode_start TEXT, "
            "transcode_end TEXT, created_at TEXT NOT NULL DEFAULT (datetime('now')), "
            "updated_at TEXT NOT NULL DEFAULT (datetime('now')), "
            "UNIQUE(dropbox_path, dropbox_rev))"
        )
        conn.execute("INSERT INTO jobs (dropbox_path, dropbox_rev, dropbox_size, "
                     "output_path) VALUES ('/HeavyDrops/Podfactory/a.mp4','r',1,'o')")
        conn.commit()
        conn.close()
        db = Database(path)
        db.initialize()
        job = db.get_job_by_path("/HeavyDrops/Podfactory/a.mp4")
        assert job.source_modified is None and job.recorded_at is not None


class TestPreemptionAmongRecordings:
    def _full(self, tmp_path, keys):
        d, _db_ = _dispatcher(tmp_path)
        d._download_active = {}
        for i, days in enumerate(keys):
            job_id = i + 1
            d._download_active[f"downloader-{i}"] = job_id
            if days is not None:                      # None = backlog download
                d._priority_ids.add(job_id)
                d._priority_keys[job_id] = (-(NOW - timedelta(days=days)).timestamp(), "p")
        return d

    def _wait_new(self, d):
        from types import SimpleNamespace
        d.download_q.put_overflow(SimpleNamespace(
            id=99, dropbox_path=f"{POD}/ep 12/Video ISO Files/CAM 1.mp4",
            source_modified=NOW, created_at=NOW))

    def test_backlog_yields_before_any_fast_lane_download(self, tmp_path):
        d = self._full(tmp_path, [7, 7, None, 30])
        self._wait_new(d)
        assert not d.should_yield_download("downloader-3")   # oldest, but backlog runs
        assert d.should_yield_download("downloader-2")       # the backlog one

    def test_oldest_session_yields_to_todays(self, tmp_path):
        d = self._full(tmp_path, [7, 7, 3, 30])
        self._wait_new(d)
        assert not d.should_yield_download("downloader-0")
        assert not d.should_yield_download("downloader-2")
        assert d.should_yield_download("downloader-3")

    def test_newer_download_never_yields_to_older_file(self, tmp_path):
        from types import SimpleNamespace
        d = self._full(tmp_path, [0, 0, 0, 0])
        d.download_q.put_overflow(SimpleNamespace(
            id=99, dropbox_path=f"{POD}/ep 1/Video ISO Files/CAM 1.mp4",
            source_modified=NOW - timedelta(days=90), created_at=NOW))
        assert not any(d.should_yield_download(f"downloader-{i}") for i in range(4))


# ------------------------------------ fast lane: no size / bitrate floor (item 6)

from types import SimpleNamespace  # noqa: E402
from unittest.mock import MagicMock  # noqa: E402

import pytest  # noqa: E402

TALKS_ISO = "/HeavyDrops/Leo Kuba/Talks by Leo/Arquivo Talks by Leo/ep 40/Video ISO Files/CAM 3.mp4"


class TestNoFloorsInFastLane:
    def _scanner(self, tmp_path):
        from transcoder.config import Config
        from transcoder.dropbox_client import DropboxFileInfo
        from transcoder.scanner import Scanner

        cfg = Config(dropbox_root="/HeavyDrops", min_size_gb=6.0,
                     local_staging_dir=str(tmp_path / "s"))
        cfg.stability_profiles.bulk.checks_required = 1
        cfg.stability_profiles.bulk.min_age_sec = 0
        db = _db(tmp_path)
        dbx = MagicMock()
        dbx.read_text_file.return_value = None
        dbx.file_exists.return_value = False
        s = Scanner(cfg, db, dbx)

        def info(path, gb):
            return DropboxFileInfo(path=path, name=path.rsplit("/", 1)[-1],
                                   size=int(gb * 1024**3), rev="r1",
                                   server_modified=NOW, client_modified=NOW)
        return s, db, info

    @pytest.mark.parametrize("path", [
        f"{POD}/ep 12/Video ISO Files/intro.mp4",
        TALKS_ISO,
    ])
    def test_2gb_iso_in_fast_lane_becomes_a_job(self, tmp_path, path):
        s, db, info = self._scanner(tmp_path)
        result = s._process_file(info(path, 2), False, s.config.stability_profiles.bulk)
        assert result == "new"
        job = db.get_job_by_path(path)
        assert job.state == JobState.NEW
        assert job.output_path.endswith("/Video ISO Files/h265/" + path.rsplit("/", 1)[-1])

    def test_2gb_backlog_file_still_too_small(self, tmp_path):
        s, db, info = self._scanner(tmp_path)
        path = "/HeavyDrops/Riachuelo/x/video/a.mp4"
        result = s._process_file(info(path, 2), False, s.config.stability_profiles.bulk)
        assert result == "skipped_small"
        assert db.get_job_by_path(path).state == JobState.SKIPPED_TOO_SMALL

    def _transcode_until_encoder(self, tmp_path, monkeypatch, path, codec="h264"):
        import transcoder.workers as wk
        from transcoder.config import Config

        class _Reached(Exception):
            pass

        cfg = Config(dropbox_root="/HeavyDrops", low_bitrate_skip_mbps_per_megapixel=3.0,
                     local_staging_dir=str(tmp_path / "s"))
        w = object.__new__(wk.TranscodeWorker)
        threading.Thread.__init__(w, name="transcoder-0", daemon=True)
        w.config, w.db, w.encoder = cfg, MagicMock(), None
        w._cleanup_staging = MagicMock()
        vi = SimpleNamespace(codec_name=codec, width=1920, height=1080, bitrate_kbps=500)
        monkeypatch.setattr(wk, "probe_video", lambda *a, **k: SimpleNamespace(
            video_info=vi, is_hevc=(codec == "hevc")))
        monkeypatch.setattr(wk, "select_best_encoder",
                            lambda *a, **k: (_ for _ in ()).throw(_Reached()))
        src = tmp_path / "input.mp4"
        src.write_bytes(b"x")
        job = SimpleNamespace(id=1, dropbox_path=path, local_input_path=str(src))
        try:
            w._transcode_job(job)
        except _Reached:
            return "encoded", w
        return "skipped", w

    def test_low_bitrate_fast_lane_iso_is_encoded(self, tmp_path, monkeypatch):
        outcome, _ = self._transcode_until_encoder(tmp_path, monkeypatch, TALKS_ISO)
        assert outcome == "encoded"

    def test_low_bitrate_backlog_still_skipped(self, tmp_path, monkeypatch):
        outcome, w = self._transcode_until_encoder(
            tmp_path, monkeypatch, "/HeavyDrops/Riachuelo/x/a.mp4")
        assert outcome == "skipped"
        assert w.db.update_job_state.call_args[0][1] == JobState.SKIPPED_LOW_BITRATE

    def test_hevc_fast_lane_iso_still_skipped(self, tmp_path, monkeypatch):
        outcome, w = self._transcode_until_encoder(
            tmp_path, monkeypatch, TALKS_ISO, codec="hevc")
        assert outcome == "skipped"
        assert w.db.update_job_state.call_args[0][1] == JobState.SKIPPED_HEVC

    def test_previous_skips_are_requeued(self, tmp_path):
        from transcoder.config import Config
        db = _db(tmp_path)
        globs = list(Config(dropbox_root="/HeavyDrops").priority.paths)
        small = db.create_job(f"{POD}/ep 9/intro.mp4", "r", 1, "o",
                              state=JobState.SKIPPED_TOO_SMALL)
        lowbr = db.create_job(TALKS_ISO, "r", 1, "o", state=JobState.SKIPPED_LOW_BITRATE)
        hevc = db.create_job(f"{POD}/ep 9/cam.mp4", "r", 1, "o", state=JobState.SKIPPED_HEVC)
        other = db.create_job("/HeavyDrops/X/a.mp4", "r", 1, "o",
                              state=JobState.SKIPPED_TOO_SMALL)
        # A path that already has a fresh job in flight is left alone.
        busy_old = db.create_job(f"{POD}/ep 9/b.mp4", "r1", 1, "o",
                                 state=JobState.SKIPPED_TOO_SMALL)
        db.create_job(f"{POD}/ep 9/b.mp4", "r2", 1, "o", state=JobState.NEW)

        assert db.requeue_priority_skips(globs) == 2
        assert db.get_job(small.id).state == JobState.NEW
        assert db.get_job(lowbr.id).state == JobState.NEW
        assert db.get_job(hevc.id).state == JobState.SKIPPED_HEVC
        assert db.get_job(other.id).state == JobState.SKIPPED_TOO_SMALL
        assert db.get_job(busy_old.id).state == JobState.SKIPPED_TOO_SMALL
        assert db.requeue_priority_skips(globs) == 0          # idempotent
