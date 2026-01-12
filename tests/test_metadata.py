"""Tests for metadata preservation (R6)."""

from pathlib import Path

import pytest

from transcoder.config import Config, TranscodeProfile
from transcoder.encoder_detect import EncoderType
from transcoder.ffmpeg_builder import FFmpegCommandBuilder, VideoInfo


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
        has_subtitles=True,
        audio_codec="aac",
    )


class TestMetadataPreservation:
    """Tests for metadata preservation (R6)."""

    @pytest.mark.parametrize("encoder", [
        EncoderType.QSV,
        EncoderType.NVENC,
        EncoderType.CPU,
    ])
    def test_map_metadata_present(
        self,
        encoder: EncoderType,
        video_info: VideoInfo,
    ) -> None:
        """All encoders must include -map_metadata 0."""
        config = Config(profile=TranscodeProfile.QUALITY)
        builder = FFmpegCommandBuilder(config)

        cmd = builder.build_transcode_command(
            Path("/input/video.mp4"),
            Path("/output/video.mp4"),
            video_info,
            encoder,
        )

        # Find -map_metadata 0 in args
        assert "-map_metadata" in cmd.args
        idx = cmd.args.index("-map_metadata")
        assert cmd.args[idx + 1] == "0"

    @pytest.mark.parametrize("encoder", [
        EncoderType.QSV,
        EncoderType.NVENC,
        EncoderType.CPU,
    ])
    def test_map_all_streams(
        self,
        encoder: EncoderType,
        video_info: VideoInfo,
    ) -> None:
        """All streams should be mapped."""
        config = Config(profile=TranscodeProfile.QUALITY)
        builder = FFmpegCommandBuilder(config)

        cmd = builder.build_transcode_command(
            Path("/input/video.mp4"),
            Path("/output/video.mp4"),
            video_info,
            encoder,
        )

        # Should have -map 0 to map all streams
        assert "-map" in cmd.args
        idx = cmd.args.index("-map")
        assert cmd.args[idx + 1] == "0"

    def test_subtitle_copy(self, video_info: VideoInfo) -> None:
        """Subtitles should be copied when present."""
        config = Config(profile=TranscodeProfile.QUALITY)
        builder = FFmpegCommandBuilder(config)

        cmd = builder.build_transcode_command(
            Path("/input/video.mp4"),
            Path("/output/video.mp4"),
            video_info,
            EncoderType.CPU,
        )

        args_str = " ".join(cmd.args)

        # Subtitles should be copied
        assert "-c:s copy" in args_str

    def test_preserves_input_extension_mp4(self, video_info: VideoInfo) -> None:
        """Output should preserve input extension (.mp4)."""
        config = Config(profile=TranscodeProfile.QUALITY)
        builder = FFmpegCommandBuilder(config)

        cmd = builder.build_transcode_command(
            Path("/input/video.MP4"),  # Uppercase
            Path("/output/video.MP4"),
            video_info,
            EncoderType.CPU,
        )

        # Output should have same extension
        assert cmd.output_path.suffix == ".MP4"

    def test_preserves_input_extension_mov(self, video_info: VideoInfo) -> None:
        """Output should preserve input extension (.mov)."""
        config = Config(profile=TranscodeProfile.QUALITY)
        builder = FFmpegCommandBuilder(config)

        cmd = builder.build_transcode_command(
            Path("/input/video.MOV"),
            Path("/output/video.MOV"),
            video_info,
            EncoderType.CPU,
        )

        assert cmd.output_path.suffix == ".MOV"


class TestNoInventedMetadata:
    """Tests to ensure we don't add metadata not from input."""

    def test_no_extra_metadata_tags(self, video_info: VideoInfo) -> None:
        """Should not add custom metadata tags."""
        config = Config(profile=TranscodeProfile.QUALITY)
        builder = FFmpegCommandBuilder(config)

        cmd = builder.build_transcode_command(
            Path("/input/video.mp4"),
            Path("/output/video.mp4"),
            video_info,
            EncoderType.CPU,
        )

        args_str = " ".join(cmd.args)

        # Should not have metadata write commands
        assert "-metadata " not in args_str
        assert "-metadata:s" not in args_str

    def test_command_description_accurate(self, video_info: VideoInfo) -> None:
        """Command description should be accurate."""
        config = Config(profile=TranscodeProfile.QUALITY)
        builder = FFmpegCommandBuilder(config)

        cmd = builder.build_transcode_command(
            Path("/input/video.mp4"),
            Path("/output/video.mp4"),
            video_info,
            EncoderType.CPU,
        )

        # Description should mention encoder and codec
        assert "libx265" in cmd.description
        assert "h264" in cmd.description
        assert "hevc" in cmd.description
