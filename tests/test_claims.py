"""Tests for cross-machine claim coordination (offline — fake Dropbox)."""
import sys
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from transcoder.claims import ClaimStore, ClaimReconciler


class FakeDropbox:
    """Emulates the few DropboxClient ops the claim layer needs, including the
    atomic add-mode 'create only if absent' semantics behind claim_create."""

    def __init__(self):
        self.entries = {}  # norm_path -> {"content", "server_modified"}

    def _norm(self, p):
        return "/" + p.strip("/")

    def claim_create(self, path, content, encoding="utf-8"):
        p = self._norm(path)
        if p in self.entries:   # already exists → we lost the race
            return False
        self.entries[p] = {"content": content,
                           "server_modified": datetime.now(timezone.utc)}
        return True

    def write_text_file(self, path, content, encoding="utf-8"):
        self.entries[self._norm(path)] = {
            "content": content, "server_modified": datetime.now(timezone.utc)}
        return True

    def get_metadata(self, path):
        e = self.entries.get(self._norm(path))
        if e is None:
            return None
        return SimpleNamespace(server_modified=e["server_modified"], path=self._norm(path))

    def delete_file(self, path):
        return self.entries.pop(self._norm(path), None) is not None

    # test helper
    def backdate(self, store, dropbox_path, minutes):
        cp = self._norm(store._claim_path(store._key(dropbox_path)))
        self.entries[cp]["server_modified"] = (
            datetime.now(timezone.utc) - timedelta(minutes=minutes))


def _store(fake, pc, ttl=60):
    return ClaimStore(fake, folder="/_h265_claims", pc_name=pc, ttl_minutes=ttl)


PATH = "/Clientes/projeto/big video.mov"


def test_claim_is_exclusive_across_machines():
    fake = FakeDropbox()
    a, b = _store(fake, "Heavy1"), _store(fake, "Heavy2")
    assert a.try_claim(PATH) is True       # A wins
    assert b.try_claim(PATH) is False      # B loses — A holds it
    assert PATH not in [None]  # sanity


def test_release_lets_another_machine_claim():
    fake = FakeDropbox()
    a, b = _store(fake, "Heavy1"), _store(fake, "Heavy2")
    a.try_claim(PATH)
    a.release(PATH)
    assert b.try_claim(PATH) is True       # freed → B can take it


def test_held_fast_path_no_network():
    fake = FakeDropbox()
    a = _store(fake, "Heavy1")
    a.try_claim(PATH)
    fake.entries.clear()                   # even if the file vanished...
    assert a.try_claim(PATH) is True       # ...held set short-circuits


def test_seed_held_readopts_after_restart():
    fake = FakeDropbox()
    a = _store(fake, "Heavy1")
    a.try_claim(PATH)
    # Simulate restart: brand-new store, claim file still in Dropbox.
    a2 = _store(fake, "Heavy1")
    a2.seed_held([PATH])
    assert a2.try_claim(PATH) is True       # re-adopts its own claim


def test_fresh_claim_by_other_is_not_stolen():
    fake = FakeDropbox()
    a, b = _store(fake, "Heavy1"), _store(fake, "Heavy2")
    a.try_claim(PATH)
    fake.backdate(a, PATH, minutes=30)      # 30m < ttl(60m) → still alive
    assert b.try_claim(PATH) is False


def test_abandoned_claim_is_stolen():
    fake = FakeDropbox()
    a, b = _store(fake, "Heavy1"), _store(fake, "Heavy2")
    a.try_claim(PATH)
    fake.backdate(a, PATH, minutes=120)     # 2h > ttl → abandoned
    assert b.try_claim(PATH) is True        # B steals it
    assert b.try_claim(PATH) is True        # and now holds it (held fast-path)


def test_heartbeat_only_touches_held():
    fake = FakeDropbox()
    a, b = _store(fake, "Heavy1"), _store(fake, "Heavy2")
    a.try_claim(PATH)
    cp = fake._norm(a._claim_path(a._key(PATH)))
    fake.entries[cp]["server_modified"] = datetime.now(timezone.utc) - timedelta(minutes=40)
    b.heartbeat(PATH)                       # B doesn't hold it → no-op
    assert fake.entries[cp]["server_modified"] < datetime.now(timezone.utc) - timedelta(minutes=30)
    a.heartbeat(PATH)                       # A holds it → refreshes
    assert fake.entries[cp]["server_modified"] > datetime.now(timezone.utc) - timedelta(minutes=1)


def _fake_db(active_paths):
    jobs = [SimpleNamespace(dropbox_path=p) for p in active_paths]
    return SimpleNamespace(get_jobs_by_states=lambda states, limit=0: jobs)


def test_reconciler_releases_finished_and_keeps_active():
    fake = FakeDropbox()
    a = _store(fake, "Heavy1")
    active_path = "/a/active.mov"
    done_path = "/a/done.mov"
    a.try_claim(active_path)
    a.try_claim(done_path)
    # Only active_path is still in flight.
    rec = ClaimReconciler(a, _fake_db([active_path]), {"NEW"}, threading.Event(), 60)
    rec.reconcile_once()
    assert fake._norm(a._claim_path(a._key(done_path))) not in fake.entries   # released
    assert fake._norm(a._claim_path(a._key(active_path))) in fake.entries     # kept
    assert a._key(done_path) not in a.held_keys()
    assert a._key(active_path) in a.held_keys()
