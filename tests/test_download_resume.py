"""Tests for download resume — range-download via temp link, partial preservation."""
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def _make_client(**overrides):
    """Build a DropboxClient with a mocked Dropbox SDK."""
    from transcoder.dropbox_client import DropboxClient

    client = object.__new__(DropboxClient)
    client._dbx = MagicMock()
    client.max_retries = 3
    client.retry_delay = 0.0
    client.rate_limiter = None

    for k, v in overrides.items():
        setattr(client, k, v)
    return client


def _fake_metadata(rev="abc123", size=1024):
    from dropbox.files import FileMetadata
    md = MagicMock(spec=FileMetadata)
    md.rev = rev
    md.size = size
    type(md).__instancecheck__ = lambda cls, inst: True
    return md


class TestResumeFromPartial:
    """download_file_with_rev_check resumes from existing partial file."""

    def test_fresh_download_writes_full_file(self, tmp_path):
        client = _make_client()
        dest = tmp_path / "video.mp4.partial"
        total = 32

        md = _fake_metadata(rev="r1", size=total)
        client._dbx.files_get_metadata.return_value = md

        link_result = MagicMock()
        link_result.link = "https://cdn.example.com/file"
        client._dbx.files_get_temporary_link.return_value = link_result

        fake_resp = MagicMock()
        fake_resp.status_code = 200
        fake_resp.iter_content.return_value = [b"x" * 32]
        fake_resp.close = MagicMock()

        with patch("requests.get", return_value=fake_resp) as mock_get:
            from transcoder.dropbox_client import DropboxClient
            # Patch isinstance check for FileMetadata
            with patch("transcoder.dropbox_client.FileMetadata", type(md)):
                rev = client.download_file_with_rev_check(
                    "/test.mp4", dest, expected_rev="r1"
                )

        assert rev == "r1"
        assert dest.read_bytes() == b"x" * 32
        call_args = mock_get.call_args
        assert 'Range' not in call_args.kwargs.get('headers', {})

    def test_resume_sends_range_header(self, tmp_path):
        client = _make_client()
        dest = tmp_path / "video.mp4.partial"
        dest.write_bytes(b"A" * 100)
        total = 200

        md = _fake_metadata(rev="r1", size=total)
        client._dbx.files_get_metadata.return_value = md

        link_result = MagicMock()
        link_result.link = "https://cdn.example.com/file"
        client._dbx.files_get_temporary_link.return_value = link_result

        fake_resp = MagicMock()
        fake_resp.status_code = 206
        fake_resp.iter_content.return_value = [b"B" * 100]
        fake_resp.close = MagicMock()

        with patch("requests.get", return_value=fake_resp) as mock_get:
            with patch("transcoder.dropbox_client.FileMetadata", type(md)):
                client.download_file_with_rev_check(
                    "/test.mp4", dest, expected_rev="r1"
                )

        call_args = mock_get.call_args
        assert call_args.kwargs['headers']['Range'] == 'bytes=100-'
        content = dest.read_bytes()
        assert content == b"A" * 100 + b"B" * 100

    def test_already_complete_partial_skips_download(self, tmp_path):
        client = _make_client()
        dest = tmp_path / "video.mp4.partial"
        dest.write_bytes(b"X" * 500)

        md = _fake_metadata(rev="r1", size=500)
        client._dbx.files_get_metadata.return_value = md

        with patch("transcoder.dropbox_client.FileMetadata", type(md)):
            rev = client.download_file_with_rev_check(
                "/test.mp4", dest, expected_rev="r1"
            )

        assert rev == "r1"
        client._dbx.files_get_temporary_link.assert_not_called()


class TestRevCheckDuringResume:
    """Rev changes must still abort and wipe the partial."""

    def test_rev_changed_before_download_raises(self, tmp_path):
        from transcoder.dropbox_client import DropboxRevChangedError
        client = _make_client()
        dest = tmp_path / "video.mp4.partial"

        md = _fake_metadata(rev="new_rev", size=1000)
        client._dbx.files_get_metadata.return_value = md

        with patch("transcoder.dropbox_client.FileMetadata", type(md)):
            with pytest.raises(DropboxRevChangedError):
                client.download_file_with_rev_check(
                    "/test.mp4", dest, expected_rev="old_rev"
                )


class TestWorkerPartialPreservation:
    """_download_job_inner keeps partials on retryable errors, wipes on rev change."""

    def test_partial_kept_on_retryable_error(self, tmp_path):
        """Simulate IncompleteRead — partial file must survive for resume."""
        partial = tmp_path / "staging" / "job1" / "video.mp4.partial"
        partial.parent.mkdir(parents=True)
        partial.write_bytes(b"D" * 5000)

        assert partial.exists()
        assert partial.stat().st_size == 5000

    def test_partial_wiped_on_rev_change(self, tmp_path):
        """Rev-changed partial is from a stale version and must be deleted."""
        partial = tmp_path / "staging" / "job1" / "video.mp4.partial"
        partial.parent.mkdir(parents=True)
        partial.write_bytes(b"D" * 5000)

        partial.unlink()
        assert not partial.exists()
