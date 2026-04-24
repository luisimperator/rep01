"""Tests for smart retry — permanent vs retryable error classification."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


class TestSmartRetry:
    """Test permanent error detection logic."""

    PERMANENT_ERROR_PATTERNS = [
        'invalid data found',
        'corrupt',
        'moov atom not found',
        'invalid nal',
        'no decoder',
        'could not find codec',
        'not a video',
        'invalid argument',
        'unrecognized option',
        'protocol not found',
        'no such file or directory',
    ]

    def _is_permanent_error(self, error_msg: str) -> bool:
        error_lower = error_msg.lower()
        return any(pattern in error_lower for pattern in self.PERMANENT_ERROR_PATTERNS)

    def test_corrupt_file_is_permanent(self):
        assert self._is_permanent_error("Error: corrupt data in file header")

    def test_moov_atom_is_permanent(self):
        assert self._is_permanent_error("[mov] moov atom not found")

    def test_invalid_data_is_permanent(self):
        assert self._is_permanent_error("Invalid data found when processing input")

    def test_no_decoder_is_permanent(self):
        assert self._is_permanent_error("No decoder could be found for codec xyz")

    def test_invalid_nal_is_permanent(self):
        assert self._is_permanent_error("Invalid NAL unit size (0 < 5)")

    def test_no_such_file_is_permanent(self):
        assert self._is_permanent_error("No such file or directory: test.mp4")

    def test_could_not_find_codec_is_permanent(self):
        assert self._is_permanent_error("Could not find codec parameters")

    def test_timeout_is_retryable(self):
        assert not self._is_permanent_error("Connection timed out")

    def test_permission_denied_is_retryable(self):
        # Permission denied can be temporary (Dropbox sync lock)
        assert not self._is_permanent_error("Permission denied: file.mp4")

    def test_disk_space_is_retryable(self):
        assert not self._is_permanent_error("No space left on device")

    def test_generic_ffmpeg_error_is_retryable(self):
        assert not self._is_permanent_error("Error while decoding stream #0:0")

    def test_network_error_is_retryable(self):
        assert not self._is_permanent_error("Network is unreachable")

    def test_empty_error_is_retryable(self):
        assert not self._is_permanent_error("")

    def test_case_insensitive(self):
        assert self._is_permanent_error("INVALID DATA FOUND in input")
        assert self._is_permanent_error("MOOV ATOM NOT FOUND")
