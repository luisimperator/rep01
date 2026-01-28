"""Tests for YouTube download detection."""

import pytest

from transcoder.utils import is_youtube_download


class TestYoutubeDownloadDetection:
    """Tests for is_youtube_download function."""

    def test_detects_2160p_av1(self) -> None:
        """Detect 4K AV1 YouTube download."""
        path = "/Videos/Tutorial Python (2160p_24fps_AV1-128kbit_AAC).mp4"
        assert is_youtube_download(path) is True

    def test_detects_1080p_h264(self) -> None:
        """Detect 1080p H264 YouTube download."""
        path = "/Videos/Music Video (1080p_30fps_H264-128kbit_AAC).mp4"
        assert is_youtube_download(path) is True

    def test_detects_720p_vp9(self) -> None:
        """Detect 720p VP9 YouTube download."""
        path = "/Videos/Podcast (720p_60fps_VP9-256kbit_OPUS).webm"
        assert is_youtube_download(path) is True

    def test_detects_480p(self) -> None:
        """Detect lower resolution YouTube download."""
        path = "/Videos/Old Video (480p_30fps_H264-96kbit_AAC).mp4"
        assert is_youtube_download(path) is True

    def test_regular_video_not_detected(self) -> None:
        """Regular video files should not be detected."""
        path = "/Videos/DJI_0042.MP4"
        assert is_youtube_download(path) is False

    def test_phone_video_not_detected(self) -> None:
        """Phone recordings should not be detected."""
        path = "/Videos/VID_20240127_153045.mp4"
        assert is_youtube_download(path) is False

    def test_gopro_not_detected(self) -> None:
        """GoPro files should not be detected."""
        path = "/Videos/GH010042.MP4"
        assert is_youtube_download(path) is False

    def test_video_with_resolution_in_name(self) -> None:
        """Video with resolution in name but not YouTube format."""
        path = "/Videos/My Video 1080p.mp4"
        assert is_youtube_download(path) is False

    def test_nested_path(self) -> None:
        """YouTube download in nested folder."""
        path = "/Videos/2024/January/Tutorial (1080p_30fps_H264-128kbit_AAC).mp4"
        assert is_youtube_download(path) is True

    def test_complex_title(self) -> None:
        """YouTube download with complex title."""
        path = "/Videos/How to Code in Python - Full Course [2024] (1080p_30fps_H264-128kbit_AAC).mp4"
        assert is_youtube_download(path) is True
