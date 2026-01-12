"""Tests for FFmpeg command builder."""

from pathlib import Path

import pytest

from transcoder.config import Config, TranscodeProfile
from transcoder.encoder_detect import EncoderType
from transcoder.ffmpeg_builder import FFmpegCommandBuilder, VideoInfo


@pytest.fixture
def config() -> Config:
    """Create test configuration."""
    return Config(
        profile=TranscodeProfile.QUALITY,
        cq_value=24,
        cpu_crf_equivalent=23,
        gop_size=60,
    )


@pytest.fixture
def video_info() -> VideoInfo:
    """Create test video info."""
    return VideoInfo(
        codec_name="h264",
        width=1920,
        height=1080,
        fps=29.97,
        duration_sec=3600.0,
        bitrate_kbps=40000,
        bit_depth=8,
        pix_fmt="yuv420p",
        has_audio=True,
        has_subtitles=False,
        audio_codec="aac",
    )


class TestFFmpegCommandBuilder:
    """Tests for FFmpeg command building."""

    def test_build_qsv_quality_command(
        self,
        config: Config,
        video_info: VideoInfo,
    ) -> None:
        """Test QSV quality profile command."""
        builder = FFmpegCommandBuilder(config)
        cmd = builder.build_transcode_command(
            Path("/input/video.mp4"),
            Path("/output/video.mp4"),
            video_info,
            EncoderType.QSV,
        )

        args_str = " ".join(cmd.args)

        # Should have QSV encoder
        assert "-c:v hevc_qsv" in args_str

        # Should have CQ setting
        assert "-global_quality:v 24" in args_str

        # Should have metadata preservation
        assert "-map_metadata 0" in args_str
        assert "-map 0" in args_str

        # Should have GOP size
        assert "-g 60" in args_str

    def test_build_nvenc_quality_command(
        self,
        config: Config,
        video_info: VideoInfo,
    ) -> None:
        """Test NVENC quality profile command."""
        builder = FFmpegCommandBuilder(config)
        cmd = builder.build_transcode_command(
            Path("/input/video.mp4"),
            Path("/output/video.mp4"),
            video_info,
            EncoderType.NVENC,
        )

        args_str = " ".join(cmd.args)

        # Should have NVENC encoder
        assert "-c:v hevc_nvenc" in args_str

        # Should have CQ settings (R6)
        assert "-rc:v vbr" in args_str
        assert "-cq:v 24" in args_str

        # Should have metadata preservation
        assert "-map_metadata 0" in args_str

    def test_build_cpu_quality_command(
        self,
        config: Config,
        video_info: VideoInfo,
    ) -> None:
        """Test CPU quality profile command."""
        builder = FFmpegCommandBuilder(config)
        cmd = builder.build_transcode_command(
            Path("/input/video.mp4"),
            Path("/output/video.mp4"),
            video_info,
            EncoderType.CPU,
        )

        args_str = " ".join(cmd.args)

        # Should have libx265 encoder
        assert "-c:v libx265" in args_str

        # Should have CRF setting (R6)
        assert "crf=23" in args_str

        # Should have metadata preservation
        assert "-map_metadata 0" in args_str

    def test_metadata_preservation(
        self,
        config: Config,
        video_info: VideoInfo,
    ) -> None:
        """Test that metadata is preserved (R6)."""
        builder = FFmpegCommandBuilder(config)

        for encoder in [EncoderType.QSV, EncoderType.NVENC, EncoderType.CPU]:
            cmd = builder.build_transcode_command(
                Path("/input/video.mp4"),
                Path("/output/video.mp4"),
                video_info,
                encoder,
            )

            args_str = " ".join(cmd.args)

            # Must have metadata copy
            assert "-map_metadata 0" in args_str, f"Missing -map_metadata 0 for {encoder}"

            # Must map all streams
            assert "-map 0" in args_str, f"Missing -map 0 for {encoder}"

    def test_10bit_video(self, config: Config) -> None:
        """Test 10-bit video handling."""
        video_info = VideoInfo(
            codec_name="h264",
            width=3840,
            height=2160,
            fps=60.0,
            duration_sec=1800.0,
            bitrate_kbps=80000,
            bit_depth=10,  # 10-bit
            pix_fmt="yuv420p10le",
            has_audio=True,
            has_subtitles=False,
            audio_codec="aac",
        )

        builder = FFmpegCommandBuilder(config)
        cmd = builder.build_transcode_command(
            Path("/input/video.mp4"),
            Path("/output/video.mp4"),
            video_info,
            EncoderType.CPU,
        )

        args_str = " ".join(cmd.args)

        # Should use main10 profile
        assert "main10" in args_str or "yuv420p10le" in args_str

    def test_audio_copy(
        self,
        config: Config,
        video_info: VideoInfo,
    ) -> None:
        """Test audio stream copying."""
        builder = FFmpegCommandBuilder(config)
        cmd = builder.build_transcode_command(
            Path("/input/video.mp4"),
            Path("/output/video.mp4"),
            video_info,
            EncoderType.CPU,
        )

        args_str = " ".join(cmd.args)

        # Should copy audio
        assert "-c:a copy" in args_str

    def test_audio_fallback_command(
        self,
        config: Config,
        video_info: VideoInfo,
    ) -> None:
        """Test audio fallback re-encoding."""
        builder = FFmpegCommandBuilder(config)
        cmd = builder.build_audio_fallback_command(
            Path("/input/video.mp4"),
            Path("/output/video.mp4"),
            video_info,
            EncoderType.CPU,
        )

        args_str = " ".join(cmd.args)

        # Should NOT have copy, should have codec and bitrate
        assert "-c:a copy" not in args_str
        assert "-c:a aac" in args_str
        assert "-b:a 320k" in args_str

    def test_temp_output_path(
        self,
        config: Config,
        video_info: VideoInfo,
    ) -> None:
        """Test that output uses temp path first."""
        builder = FFmpegCommandBuilder(config)
        cmd = builder.build_transcode_command(
            Path("/input/video.mp4"),
            Path("/output/video.mp4"),
            video_info,
            EncoderType.CPU,
        )

        # Output should be temp file
        assert cmd.temp_output_path == Path("/output/video.mp4.tmp")

        # Command should write to temp
        assert str(cmd.temp_output_path) in cmd.args


class TestBalancedProfile:
    """Tests for balanced profile (bitrate mode)."""

    def test_balanced_uses_bitrate(self) -> None:
        """Balanced profile should use target bitrate."""
        config = Config(profile=TranscodeProfile.BALANCED)
        video_info = VideoInfo(
            codec_name="h264",
            width=1920,
            height=1080,
            fps=30.0,
            duration_sec=1000.0,
            bitrate_kbps=40000,
            bit_depth=8,
            pix_fmt="yuv420p",
            has_audio=True,
            has_subtitles=False,
            audio_codec="aac",
        )

        builder = FFmpegCommandBuilder(config)
        cmd = builder.build_transcode_command(
            Path("/input/video.mp4"),
            Path("/output/video.mp4"),
            video_info,
            EncoderType.CPU,
        )

        args_str = " ".join(cmd.args)

        # Should have bitrate settings
        assert "-b:v" in args_str
        assert "-maxrate" in args_str
        assert "-bufsize" in args_str
