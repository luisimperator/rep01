"""Tests for path mapping utilities (R3, R4)."""

import pytest

from transcoder.utils import (
    get_output_path,
    is_in_h265_folder,
    is_partial_file,
    is_video_file,
    matches_exclude_pattern,
    normalize_dropbox_path,
)


class TestNormalizeDropboxPath:
    """Tests for path normalization."""

    def test_adds_leading_slash(self) -> None:
        assert normalize_dropbox_path("Videos/clip.mp4") == "/Videos/clip.mp4"

    def test_preserves_leading_slash(self) -> None:
        assert normalize_dropbox_path("/Videos/clip.mp4") == "/Videos/clip.mp4"

    def test_removes_trailing_slash(self) -> None:
        assert normalize_dropbox_path("/Videos/") == "/Videos"

    def test_normalizes_separators(self) -> None:
        assert normalize_dropbox_path("\\Videos\\clip.mp4") == "/Videos/clip.mp4"

    def test_removes_duplicate_slashes(self) -> None:
        assert normalize_dropbox_path("//Videos//clip.mp4") == "/Videos/clip.mp4"

    def test_strips_whitespace(self) -> None:
        assert normalize_dropbox_path("  /Videos/clip.mp4  ") == "/Videos/clip.mp4"

    def test_root_path(self) -> None:
        assert normalize_dropbox_path("/") == "/"


class TestGetOutputPath:
    """Tests for output path calculation (R3)."""

    def test_basic_path(self) -> None:
        """Input: /A/B/clip001.MP4 -> Output: /A/B/h265/clip001.MP4"""
        assert get_output_path("/A/B/clip001.MP4") == "/A/B/h265/clip001.MP4"

    def test_preserves_exact_filename(self) -> None:
        """Filename must be preserved exactly."""
        assert get_output_path("/Videos/MyClip_001.mov") == "/Videos/h265/MyClip_001.mov"

    def test_preserves_extension_case(self) -> None:
        """Extension case must be preserved."""
        assert get_output_path("/A/B/clip.MP4") == "/A/B/h265/clip.MP4"
        assert get_output_path("/A/B/clip.mp4") == "/A/B/h265/clip.mp4"
        assert get_output_path("/A/B/clip.MOV") == "/A/B/h265/clip.MOV"

    def test_deep_path(self) -> None:
        """Works with deeply nested paths."""
        result = get_output_path("/Project/Year/Month/Day/clip.mp4")
        assert result == "/Project/Year/Month/Day/h265/clip.mp4"

    def test_root_level_file(self) -> None:
        """File at root level."""
        assert get_output_path("/clip.mp4") == "/h265/clip.mp4"

    def test_special_characters_in_name(self) -> None:
        """Handles special characters in filename."""
        result = get_output_path("/Videos/My Clip (2023) - Final.mp4")
        assert result == "/Videos/h265/My Clip (2023) - Final.mp4"


class TestIsInH265Folder:
    """Tests for h265 folder detection (R4)."""

    def test_in_h265_folder(self) -> None:
        """Files in h265 folder should be detected."""
        assert is_in_h265_folder("/A/B/h265/clip.mp4") is True

    def test_case_insensitive(self) -> None:
        """Detection should be case-insensitive."""
        assert is_in_h265_folder("/A/B/H265/clip.mp4") is True
        assert is_in_h265_folder("/A/B/H265/CLIP.MP4") is True

    def test_not_in_h265_folder(self) -> None:
        """Files not in h265 folder."""
        assert is_in_h265_folder("/A/B/clip.mp4") is False
        assert is_in_h265_folder("/Videos/original.mp4") is False

    def test_h265_in_filename(self) -> None:
        """h265 in filename shouldn't trigger."""
        # The folder check looks for /h265/ so this should be False
        assert is_in_h265_folder("/Videos/clip_h265.mp4") is False

    def test_nested_h265_folder(self) -> None:
        """Nested h265 folders."""
        assert is_in_h265_folder("/A/h265/B/clip.mp4") is True

    def test_h265_as_folder_name(self) -> None:
        """Folder named h265 at end of path."""
        assert is_in_h265_folder("/A/B/h265") is True


class TestMatchesExcludePattern:
    """Tests for exclude pattern matching."""

    def test_basic_glob_match(self) -> None:
        patterns = ["*/h265/*"]
        assert matches_exclude_pattern("/A/B/h265/clip.mp4", patterns) is True

    def test_case_insensitive_match(self) -> None:
        patterns = ["*/h265/*"]
        assert matches_exclude_pattern("/A/B/H265/clip.mp4", patterns) is True

    def test_no_match(self) -> None:
        patterns = ["*/h265/*"]
        assert matches_exclude_pattern("/A/B/original/clip.mp4", patterns) is False

    def test_multiple_patterns(self) -> None:
        patterns = ["*/h265/*", "*/temp/*", "*.partial"]
        assert matches_exclude_pattern("/A/temp/clip.mp4", patterns) is True
        assert matches_exclude_pattern("/A/clip.partial", patterns) is True


class TestIsVideoFile:
    """Tests for video file detection."""

    def test_mp4_extension(self) -> None:
        extensions = [".mp4", ".mov", ".MP4", ".MOV"]
        assert is_video_file("/path/to/video.mp4", extensions) is True
        assert is_video_file("/path/to/video.MP4", extensions) is True

    def test_mov_extension(self) -> None:
        extensions = [".mp4", ".mov"]
        assert is_video_file("/path/to/video.mov", extensions) is True
        assert is_video_file("/path/to/video.MOV", extensions) is True

    def test_non_video_extension(self) -> None:
        extensions = [".mp4", ".mov"]
        assert is_video_file("/path/to/document.pdf", extensions) is False
        assert is_video_file("/path/to/image.jpg", extensions) is False


class TestIsPartialFile:
    """Tests for partial file detection."""

    def test_partial_suffix(self) -> None:
        assert is_partial_file("/path/to/video.mp4.partial") is True

    def test_tmp_suffix(self) -> None:
        assert is_partial_file("/path/to/video.mp4.tmp") is True

    def test_hidden_file(self) -> None:
        assert is_partial_file("/path/to/.hidden") is True

    def test_tilde_prefix(self) -> None:
        assert is_partial_file("/path/to/~video.mp4") is True

    def test_normal_file(self) -> None:
        assert is_partial_file("/path/to/video.mp4") is False
