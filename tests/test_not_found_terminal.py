"""Regression tests for the not-found retry loop (HEAVY7 field case).

When a folder is moved/renamed/deleted on Dropbox after its files were
queued, every download attempt 404s. Before the fix:
  * download_partial converted the ApiError to DropboxNotFoundError INSIDE
    its operation closure, so _retry_operation's generic handler retried the
    permanent 404 with the full 2+4+8+16s backoff (the preflight retry storm);
  * the worker treated the 404 as transient — RETRY_WAIT → FAILED — and the
    watchdog's failed-revive then resurrected all of them forever.

Now: DropboxNotFoundError short-circuits the retry loop, and the download
worker re-confirms the miss with one metadata lookup and parks the job in
SKIPPED_NOT_FOUND (terminal — the failed-revive only touches FAILED). A
file that reappears gets a fresh job: terminal states are excluded from the
unique active-path index and a Dropbox move produces a new rev.
"""
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from transcoder.database import (  # noqa: E402
    Database,
    JobState,
    TERMINAL_STATES,
)


# --- terminal-state plumbing ---------------------------------------------------

def test_skipped_not_found_is_terminal():
    assert JobState.SKIPPED_NOT_FOUND in TERMINAL_STATES


def _make_db(tmp_path) -> Database:
    db = Database(tmp_path / "jobs.db")
    db.initialize()
    return db


def test_reappeared_file_can_be_requeued_after_not_found(tmp_path):
    """A SKIPPED_NOT_FOUND row must not block a fresh job for the same path
    (unique active-path index excludes terminal states)."""
    db = _make_db(tmp_path)
    job = db.create_job(
        dropbox_path="/HD/folder/clip.mp4",
        dropbox_rev="rev-1",
        dropbox_size=123,
        output_path="/HD/folder/h265/clip.mp4",
        state=JobState.NEW,
    )
    db.update_job_state(job.id, JobState.SKIPPED_NOT_FOUND)

    # File came back (move back = new rev) — scanner creates a new job.
    new_job = db.create_job(
        dropbox_path="/HD/folder/clip.mp4",
        dropbox_rev="rev-2",
        dropbox_size=123,
        output_path="/HD/folder/h265/clip.mp4",
        state=JobState.NEW,
    )
    assert new_job is not None
    assert new_job.id != job.id
    assert new_job.state == JobState.NEW


def test_failed_revive_does_not_touch_skipped_not_found(tmp_path):
    """The watchdog's reset_failed_jobs only revives FAILED — the whole point
    of the terminal state is escaping that loop."""
    db = _make_db(tmp_path)
    nf_job = db.create_job(
        dropbox_path="/HD/a/gone.mp4",
        dropbox_rev="r1",
        dropbox_size=1,
        output_path="/HD/a/h265/gone.mp4",
        state=JobState.NEW,
    )
    db.update_job_state(nf_job.id, JobState.SKIPPED_NOT_FOUND)
    failed_job = db.create_job(
        dropbox_path="/HD/a/flaky.mp4",
        dropbox_rev="r1",
        dropbox_size=1,
        output_path="/HD/a/h265/flaky.mp4",
        state=JobState.NEW,
    )
    db.update_job_state(failed_job.id, JobState.FAILED)

    reset = db.reset_failed_jobs()

    assert reset == 1
    assert db.get_job(nf_job.id).state == JobState.SKIPPED_NOT_FOUND
    assert db.get_job(failed_job.id).state == JobState.RETRY_WAIT


# --- _retry_operation must not retry a permanent 404 ---------------------------

def test_retry_operation_does_not_retry_dropbox_not_found(monkeypatch):
    from transcoder.dropbox_client import DropboxClient, DropboxNotFoundError

    client = object.__new__(DropboxClient)
    client.max_retries = 5
    client.retry_delay = 2.0
    client.rate_limiter = None

    calls = {"n": 0}

    def op():
        calls["n"] += 1
        # The shape download_partial produces: already-converted not-found.
        raise DropboxNotFoundError("/HD/gone/file.mp4")

    monkeypatch.setattr(time, "sleep", lambda *_a, **_k: pytest.fail(
        "retry backoff slept on a permanent not-found"
    ))

    with pytest.raises(DropboxNotFoundError):
        client._retry_operation(op, "download_partial(/HD/gone/file.mp4, 0+16777216)")

    assert calls["n"] == 1, "expected exactly one attempt, no retries"


# --- DownloadWorker._handle_not_found ------------------------------------------

class _FakeDB:
    def __init__(self):
        self.updates: list[tuple[int, JobState, dict]] = []
        self.retry_calls = 0

    def update_job_state(self, job_id, state, **kw):
        self.updates.append((job_id, state, kw))

    def increment_retry(self, job_id, max_retries):
        self.retry_calls += 1
        return (1, False)  # always "retry again"

    @property
    def last_state(self):
        return self.updates[-1][1]


def _bare_download_worker(tmp_path, metadata_result):
    """DownloadWorker with just the attributes _handle_not_found touches."""
    import threading

    from transcoder.workers import DownloadWorker

    worker = object.__new__(DownloadWorker)
    # Workers are Thread subclasses; the name property needs Thread.__init__.
    threading.Thread.__init__(worker, name="downloader-test", daemon=True)
    worker.db = _FakeDB()
    worker.config = SimpleNamespace(
        watchdog=SimpleNamespace(max_retries=10),
        local_staging_dir=tmp_path,
    )

    class _FakeDropbox:
        def get_metadata(self, path):
            if isinstance(metadata_result, Exception):
                raise metadata_result
            return metadata_result

    worker.dropbox = _FakeDropbox()
    return worker


def _job():
    return SimpleNamespace(id=42, dropbox_path="/HD/x/clip.mp4", dropbox_size=100)


def test_confirmed_missing_goes_terminal(tmp_path):
    from transcoder.dropbox_client import DropboxNotFoundError

    worker = _bare_download_worker(tmp_path, metadata_result=None)
    worker._handle_not_found(_job(), DropboxNotFoundError("Path not found: x"))

    assert worker.db.last_state == JobState.SKIPPED_NOT_FOUND
    assert worker.db.retry_calls == 0, "terminal skip must not burn a retry"


def test_metadata_says_alive_falls_back_to_retry(tmp_path):
    """A 404 mid-download but the file still exists (transient/namespace
    glitch) → normal retry machinery, NOT a terminal skip."""
    from transcoder.dropbox_client import DropboxNotFoundError

    worker = _bare_download_worker(
        tmp_path, metadata_result=SimpleNamespace(size=100, rev="r1"),
    )
    worker._handle_not_found(_job(), DropboxNotFoundError("Path not found: x"))

    assert worker.db.last_state == JobState.RETRY_WAIT
    assert worker.db.retry_calls == 1


def test_metadata_probe_error_falls_back_to_retry(tmp_path):
    """Couldn't even confirm (network/auth hiccup) → keep retrying rather
    than risk terminal-skipping a live file."""
    from transcoder.dropbox_client import DropboxNotFoundError

    worker = _bare_download_worker(tmp_path, metadata_result=RuntimeError("boom"))
    worker._handle_not_found(_job(), DropboxNotFoundError("Path not found: x"))

    assert worker.db.last_state == JobState.RETRY_WAIT
    assert worker.db.retry_calls == 1
