"""Regression tests for the ._ (dot-underscore) move retry storm.

A files_move whose *source* has already vanished returns a Dropbox
RelocationError tagged `from_lookup -> not_found`. That is permanent: the
source is gone, so re-issuing the move just re-runs the same doomed lookup.
Before the fix, `_retry_operation` did not recognise this shape (it only knew
the `path` union via `_is_path_not_found`), so every dead ._ fork burned the
full 2+4+8+16s exponential backoff and five API calls before giving up — the
`cleanup_dot_underscore` log spam the user flagged.

These tests pin the new behaviour: the relocation/not-found shape is detected,
short-circuits the retry loop immediately (no backoff), and surfaces as
DropboxNotFoundError so callers can treat it as idempotent success.
"""
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

pytest.importorskip("dropbox")  # real SDK error types; skip where absent

from dropbox.exceptions import ApiError  # noqa: E402

from transcoder.dropbox_client import (  # noqa: E402
    DropboxClient,
    DropboxNotFoundError,
    _is_path_not_found,
    _is_relocation_source_missing,
)


# --- Fake Dropbox error unions -------------------------------------------------
# Duck-typed stand-ins matching the tag-probe API the helpers rely on, so the
# tests don't depend on SDK constructor internals.

class _FakeLookup:
    def __init__(self, not_found: bool):
        self._nf = not_found

    def is_not_found(self) -> bool:
        return self._nf


class _FakeRelocationError:
    """Mimics dropbox.files.RelocationError (from_lookup variant)."""

    def __init__(self, from_lookup_not_found: bool, is_from: bool = True):
        self._is_from = is_from
        self._lookup = _FakeLookup(from_lookup_not_found)

    def is_from_lookup(self) -> bool:
        return self._is_from

    def get_from_lookup(self):
        return self._lookup


class _FakePathError:
    """Mimics the `path` union used by list/get_metadata errors."""

    def __init__(self, not_found: bool, is_path: bool = True):
        self._is_path = is_path
        self._lookup = _FakeLookup(not_found)

    def is_path(self) -> bool:
        return self._is_path

    def get_path(self):
        return self._lookup


def _api_error(err) -> ApiError:
    return ApiError(
        request_id="req-test",
        error=err,
        user_message_text=None,
        user_message_locale=None,
    )


# --- _is_relocation_source_missing --------------------------------------------

def test_relocation_from_lookup_not_found_is_detected():
    assert _is_relocation_source_missing(
        _api_error(_FakeRelocationError(from_lookup_not_found=True))
    )


def test_relocation_from_lookup_present_is_not_missing():
    # from_lookup is set but the source *exists* (some other lookup failure)
    assert not _is_relocation_source_missing(
        _api_error(_FakeRelocationError(from_lookup_not_found=False))
    )


def test_relocation_without_from_lookup_is_not_missing():
    assert not _is_relocation_source_missing(
        _api_error(_FakeRelocationError(from_lookup_not_found=True, is_from=False))
    )


def test_relocation_helper_is_defensive_against_garbage():
    assert not _is_relocation_source_missing(object())
    assert not _is_relocation_source_missing(_api_error(object()))


def test_relocation_shape_not_confused_with_path_union():
    # A RelocationError is *not* a path-union error, so the old detector must
    # not flag it (proving the two probes are complementary, not overlapping).
    reloc = _api_error(_FakeRelocationError(from_lookup_not_found=True))
    assert not _is_path_not_found(reloc)
    assert _is_relocation_source_missing(reloc)

    path_nf = _api_error(_FakePathError(not_found=True))
    assert _is_path_not_found(path_nf)
    assert not _is_relocation_source_missing(path_nf)


# --- _retry_operation short-circuit -------------------------------------------

def _bare_client(max_retries: int = 5, retry_delay: float = 2.0) -> DropboxClient:
    """A DropboxClient with the retry knobs set but no real auth/network."""
    client = object.__new__(DropboxClient)
    client.max_retries = max_retries
    client.retry_delay = retry_delay
    client.rate_limiter = None
    return client


def test_missing_source_short_circuits_without_backoff(monkeypatch):
    client = _bare_client(max_retries=5, retry_delay=2.0)

    calls = {"n": 0}

    def op():
        calls["n"] += 1
        raise _api_error(_FakeRelocationError(from_lookup_not_found=True))

    # Any sleep here would mean the retry loop ran — fail loudly instead.
    monkeypatch.setattr(time, "sleep", lambda *_a, **_k: pytest.fail(
        "retry backoff slept on a permanent not-found relocation"
    ))

    with pytest.raises(DropboxNotFoundError):
        client._retry_operation(op, "move(gone -> quarantine)")

    assert calls["n"] == 1, "expected exactly one attempt, no retries"


def test_other_relocation_errors_still_retry(monkeypatch):
    client = _bare_client(max_retries=3, retry_delay=0.0)

    calls = {"n": 0}

    def op():
        calls["n"] += 1
        # from_lookup set but source present -> a *different*, retryable failure
        raise _api_error(_FakeRelocationError(from_lookup_not_found=False))

    slept = []
    monkeypatch.setattr(time, "sleep", lambda d: slept.append(d))

    with pytest.raises(Exception):
        client._retry_operation(op, "move(busy -> dst)")

    assert calls["n"] == 3, "retryable relocation error should exhaust attempts"
    assert len(slept) == 2, "should back off between the 3 attempts"
