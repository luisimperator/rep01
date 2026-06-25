"""Reorganize/delete gate keyed on the Premiere project, not "file touched".

Operator correction: the old `is_folder_settled` looked at whether ANY file in
the media folder was modified in the last N days, using Dropbox's
server_modified (upload time). That falsely froze reorganization whenever a
batch of finished, years-old footage was merely re-synced to Dropbox — the
upload reset the clock. The right signal is the Adobe Premiere project's real
last-save time (.prproj, client_modified), found by walking up to the nearest
delivery folder; with the footage's real capture date as the fallback when no
project exists.
"""
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from transcoder.dropbox_client import DropboxFileInfo  # noqa: E402
from transcoder.reorganize import is_folder_settled  # noqa: E402

ROOT = "/HD"


def _file(folder, name, *, client_days_ago, server_days_ago=0):
    now = datetime.now(timezone.utc)
    return DropboxFileInfo(
        path=f"{folder}/{name}",
        name=name,
        size=1,
        rev="rev",
        server_modified=now - timedelta(days=server_days_ago),
        client_modified=now - timedelta(days=client_days_ago),
    )


class _FakeDbx:
    """Serves a fixed {folder_path: [DropboxFileInfo]} tree, counting lists."""

    def __init__(self, tree):
        self.tree = tree
        self.calls: list[str] = []

    def list_folder(self, path, recursive=False):
        self.calls.append(path)
        for entry in self.tree.get(path, []):
            yield entry


# --- .prproj is the signal -------------------------------------------------------

def test_recent_prproj_in_media_folder_holds():
    dbx = _FakeDbx({
        "/HD/proj": [
            _file("/HD/proj", "edit.prproj", client_days_ago=3),
            _file("/HD/proj", "C0001.MP4", client_days_ago=400),
        ],
    })
    r = is_folder_settled(dbx, "/HD/proj", 60, dropbox_root=ROOT)
    assert r.settled is False
    assert r.source == "prproj"


def test_old_prproj_lets_it_reorganize():
    dbx = _FakeDbx({
        "/HD/proj": [
            _file("/HD/proj", "edit.prproj", client_days_ago=120),
            _file("/HD/proj", "C0001.MP4", client_days_ago=400),
        ],
    })
    r = is_folder_settled(dbx, "/HD/proj", 60, dropbox_root=ROOT)
    assert r.settled is True
    assert r.source == "prproj"


def test_prproj_found_by_walking_up_to_delivery_folder():
    # Media sits deep; the project lives at the delivery root above it.
    dbx = _FakeDbx({
        "/HD/proj": [_file("/HD/proj", "edit.prproj", client_days_ago=2)],
        "/HD/proj/footage": [],
        "/HD/proj/footage/day1": [
            _file("/HD/proj/footage/day1", "C0001.MP4", client_days_ago=400),
        ],
    })
    r = is_folder_settled(dbx, "/HD/proj/footage/day1", 60, dropbox_root=ROOT)
    assert r.settled is False
    assert r.source == "prproj"


# --- the actual ingest bug: upload time must be ignored --------------------------

def test_reuploaded_old_footage_is_settled_despite_recent_upload():
    # No project anywhere. Footage shot 400d ago but re-synced to Dropbox today:
    # server_modified is fresh, client_modified is old. Must read as settled.
    dbx = _FakeDbx({
        "/HD/archive": [
            _file("/HD/archive", "C0001.MP4", client_days_ago=400, server_days_ago=0),
            _file("/HD/archive", "C0002.MP4", client_days_ago=400, server_days_ago=0),
        ],
    })
    r = is_folder_settled(dbx, "/HD/archive", 60, dropbox_root=ROOT)
    assert r.settled is True
    assert r.source == "media"


def test_genuinely_recent_footage_without_project_holds():
    dbx = _FakeDbx({
        "/HD/fresh": [_file("/HD/fresh", "C0001.MP4", client_days_ago=5)],
    })
    r = is_folder_settled(dbx, "/HD/fresh", 60, dropbox_root=ROOT)
    assert r.settled is False
    assert r.source == "media"


# --- edges -----------------------------------------------------------------------

def test_min_age_zero_short_circuits():
    dbx = _FakeDbx({"/HD/x": [_file("/HD/x", "edit.prproj", client_days_ago=0)]})
    r = is_folder_settled(dbx, "/HD/x", 0, dropbox_root=ROOT)
    assert r.settled is True
    assert r.source == "disabled"
    assert dbx.calls == []  # never even lists


def test_empty_folder_is_settled():
    dbx = _FakeDbx({"/HD/empty": []})
    r = is_folder_settled(dbx, "/HD/empty", 60, dropbox_root=ROOT)
    assert r.settled is True
    assert r.source == "empty"


def test_walkup_stops_at_dropbox_root():
    # The only .prproj is ABOVE the configured root — must not be used.
    dbx = _FakeDbx({
        "/HD": [_file("/HD", "stray.prproj", client_days_ago=2)],
        "/HD/proj/day1": [_file("/HD/proj/day1", "C0001.MP4", client_days_ago=400)],
    })
    r = is_folder_settled(dbx, "/HD/proj/day1", 60, dropbox_root="/HD/proj")
    assert r.settled is True          # fell back to the old footage
    assert r.source == "media"
    assert "/HD" not in dbx.calls     # never climbed above the root


def test_cache_avoids_relisting_shared_ancestors():
    dbx = _FakeDbx({
        "/HD/proj": [_file("/HD/proj", "edit.prproj", client_days_ago=2)],
        "/HD/proj/footage": [],
        "/HD/proj/footage/day1": [_file("/HD/proj/footage/day1", "A.MP4", client_days_ago=400)],
        "/HD/proj/footage/day2": [_file("/HD/proj/footage/day2", "B.MP4", client_days_ago=400)],
    })
    cache: dict = {}
    r1 = is_folder_settled(dbx, "/HD/proj/footage/day1", 60, dropbox_root=ROOT, cache=cache)
    r2 = is_folder_settled(dbx, "/HD/proj/footage/day2", 60, dropbox_root=ROOT, cache=cache)
    assert r1.settled is r2.settled is False
    assert r1.source == r2.source == "prproj"
    # The shared delivery folder is listed once, not once per sibling.
    assert dbx.calls.count("/HD/proj") == 1
