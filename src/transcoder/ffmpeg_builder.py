"""
FFmpeg command builder for HEVC transcoding.

Builds FFmpeg commands with proper settings for each encoder type and profile.
Ensures metadata preservation and proper output format.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from .encoder_detect import EncoderType

if TYPE_CHECKING:
    from .config import Config, TranscodeProfile

logger = logging.getLogger(__name__)


@dataclass
class VideoInfo:
    """Information about input video from ffprobe."""
    codec_name: str
    width: int
    height: int
    fps: float
    duration_sec: float
    bitrate_kbps: int
    bit_depth: int = 8
    pix_fmt: str = "yuv420p"
    has_audio: bool = True
    has_subtitles: bool = False
    audio_codec: str | None = None


@dataclass
class FFmpegCommand:
    """Represents a complete FFmpeg command."""
    args: list[str]
    input_path: Path
    output_path: Path
    temp_output_path: Path
    encoder: EncoderType
    profile: str
    description: str
    expected_duration_sec: float

    def as_list(self) -> list[str]:
        """Return command as list for subprocess."""
        return self.args

    def as_string(self) -> str:
        """Return command as shell string (for logging)."""
        import shlex
        return shlex.join(self.args)


class FFmpegCommandBuilder:
    """
    Builder for FFmpeg transcode commands.

    Supports QSV, NVENC, and CPU encoders with balanced and quality profiles.
    Ensures metadata preservation per R6.
    """

    def __init__(self, config: Config):
        """
        Initialize builder with config.

        Args:
            config: Application configuration.
        """
        self.config = config

    def build_transcode_command(
        self,
        input_path: Path,
        output_path: Path,
        video_info: VideoInfo,
        encoder: EncoderType,
    ) -> FFmpegCommand:
        """
        Build FFmpeg transcode command.

        Args:
            input_path: Path to input video.
            output_path: Path for output video.
            video_info: Probed video information.
            encoder: Encoder to use.

        Returns:
            FFmpegCommand ready for execution.
        """
        from .config import TranscodeProfile

        profile = self.config.profile
        temp_output = output_path.with_suffix(output_path.suffix + '.tmp')

        # Start building command
        args: list[str] = [self.config.ffmpeg_path, "-hide_banner", "-y"]

        # Add hardware acceleration for input (if applicable)
        args.extend(self._get_input_hwaccel(encoder))

        # Input file
        args.extend(["-i", str(input_path)])

        # Mapping: copy all streams, preserve metadata (R6)
        args.extend([
            "-map", "0",              # Map all streams
            "-map_metadata", "0",     # Copy all metadata from input
        ])

        # Video encoder settings
        args.extend(self._get_video_encoder_args(encoder, profile, video_info))

        # Audio handling
        args.extend(self._get_audio_args(video_info))

        # Subtitle handling (copy if present)
        if video_info.has_subtitles:
            args.extend(["-c:s", "copy"])

        # GOP size
        args.extend(["-g", str(self.config.gop_size)])

        # Extra args from config
        args.extend(self.config.ffmpeg_extra_args)

        # Output file (temp first)
        args.append(str(temp_output))

        description = (
            f"{encoder.value} {profile.value} transcode: "
            f"{video_info.codec_name} -> hevc, "
            f"{video_info.width}x{video_info.height}, "
            f"{video_info.duration_sec:.1f}s"
        )

        return FFmpegCommand(
            args=args,
            input_path=input_path,
            output_path=output_path,
            temp_output_path=temp_output,
            encoder=encoder,
            profile=profile.value,
            description=description,
            expected_duration_sec=video_info.duration_sec,
        )

    def _get_input_hwaccel(self, encoder: EncoderType) -> list[str]:
        """Get input hardware acceleration arguments."""
        if encoder == EncoderType.QSV:
            return ["-hwaccel", "qsv", "-hwaccel_output_format", "qsv"]
        elif encoder == EncoderType.NVENC:
            # NVENC can decode with CUDA but encoding doesn't require hwaccel flag
            # Optionally use CUDA decode for performance
            return ["-hwaccel", "cuda", "-hwaccel_output_format", "cuda"]
        return []

    def _get_video_encoder_args(
        self,
        encoder: EncoderType,
        profile: TranscodeProfile,
        video_info: VideoInfo,
    ) -> list[str]:
        """Get video encoder arguments based on encoder and profile."""
        from .config import TranscodeProfile

        args: list[str] = []

        # Determine if we should use 10-bit
        use_10bit = video_info.bit_depth >= 10 or "10" in video_info.pix_fmt

        if encoder == EncoderType.QSV:
            args.extend(self._get_qsv_args(profile, use_10bit))
        elif encoder == EncoderType.NVENC:
            args.extend(self._get_nvenc_args(profile, use_10bit))
        else:  # CPU
            args.extend(self._get_cpu_args(profile, use_10bit))

        return args

    def _get_qsv_args(self, profile: TranscodeProfile, use_10bit: bool) -> list[str]:
        """Get Intel QuickSync encoder arguments."""
        from .config import TranscodeProfile

        args = ["-c:v", "hevc_qsv"]

        # Profile (main10 for 10-bit, main otherwise)
        if use_10bit:
            args.extend(["-profile:v", "main10"])
        else:
            args.extend(["-profile:v", "main"])

        args.extend(["-preset", "medium"])

        if profile == TranscodeProfile.QUALITY:
            # CQ mode with global_quality (R6)
            args.extend([
                "-global_quality:v", str(self.config.cq_value),
                "-look_ahead", "1",
            ])
        else:  # BALANCED
            # Bitrate mode
            args.extend([
                "-b:v", f"{self.config.bitrate.target_mbps}M",
                "-maxrate", f"{self.config.bitrate.max_mbps}M",
                "-bufsize", f"{self.config.bitrate.bufsize_mbps}M",
            ])

        return args

    def _get_nvenc_args(self, profile: TranscodeProfile, use_10bit: bool) -> list[str]:
        """Get NVIDIA NVENC encoder arguments."""
        from .config import TranscodeProfile

        args = ["-c:v", "hevc_nvenc"]

        # Profile
        if use_10bit:
            args.extend(["-profile:v", "main10"])
        else:
            args.extend(["-profile:v", "main"])

        # Preset (p5 is a good balance; p7 is slower/better)
        args.extend(["-preset", "p5"])

        # Tune for high quality
        args.extend(["-tune", "hq"])

        if profile == TranscodeProfile.QUALITY:
            # VBR with CQ mode (R6)
            args.extend([
                "-rc:v", "vbr",
                "-cq:v", str(self.config.cq_value),
                "-b:v", "0",  # Let CQ control quality
            ])
            # Optional: add maxrate for streaming compatibility
            args.extend([
                "-maxrate", f"{self.config.bitrate.max_mbps}M",
                "-bufsize", f"{self.config.bitrate.bufsize_mbps}M",
            ])
        else:  # BALANCED
            args.extend([
                "-rc:v", "vbr",
                "-b:v", f"{self.config.bitrate.target_mbps}M",
                "-maxrate", f"{self.config.bitrate.max_mbps}M",
                "-bufsize", f"{self.config.bitrate.bufsize_mbps}M",
            ])

        # B-frames for better compression
        args.extend(["-bf", "4"])

        return args

    def _get_cpu_args(self, profile: TranscodeProfile, use_10bit: bool) -> list[str]:
        """Get CPU (libx265) encoder arguments."""
        from .config import TranscodeProfile

        args = ["-c:v", "libx265"]
        args.extend(["-preset", "medium"])

        # x265 params
        x265_params = []

        if profile == TranscodeProfile.QUALITY:
            # CRF mode (R6: equivalent to CQ 24)
            x265_params.append(f"crf={self.config.cpu_crf_equivalent}")
            x265_params.append("aq-mode=3")  # Adaptive quantization
        else:  # BALANCED
            # ABR mode
            args.extend([
                "-b:v", f"{self.config.bitrate.target_mbps}M",
                "-maxrate", f"{self.config.bitrate.max_mbps}M",
                "-bufsize", f"{self.config.bitrate.bufsize_mbps}M",
            ])

        # 10-bit output
        if use_10bit:
            args.extend(["-pix_fmt", "yuv420p10le"])
            x265_params.append("profile=main10")
        else:
            args.extend(["-pix_fmt", "yuv420p"])

        # Add x265 params if any
        if x265_params:
            args.extend(["-x265-params", ":".join(x265_params)])

        return args

    def _get_audio_args(self, video_info: VideoInfo) -> list[str]:
        """Get audio handling arguments."""
        if not video_info.has_audio:
            return ["-an"]

        # Try to copy audio; fallback to re-encode handled in transcoder
        return ["-c:a", "copy"]

    def build_audio_fallback_command(
        self,
        input_path: Path,
        output_path: Path,
        video_info: VideoInfo,
        encoder: EncoderType,
    ) -> FFmpegCommand:
        """
        Build command with audio re-encoding fallback.

        Used when audio copy fails (incompatible codec).
        """
        cmd = self.build_transcode_command(input_path, output_path, video_info, encoder)

        # Replace audio copy with re-encode
        new_args = []
        i = 0
        while i < len(cmd.args):
            if cmd.args[i] == "-c:a" and i + 1 < len(cmd.args) and cmd.args[i + 1] == "copy":
                new_args.extend([
                    "-c:a", self.config.audio_fallback_codec,
                    "-b:a", self.config.audio_fallback_bitrate,
                ])
                i += 2
            else:
                new_args.append(cmd.args[i])
                i += 1

        return FFmpegCommand(
            args=new_args,
            input_path=cmd.input_path,
            output_path=cmd.output_path,
            temp_output_path=cmd.temp_output_path,
            encoder=cmd.encoder,
            profile=cmd.profile,
            description=cmd.description + " (audio re-encode)",
            expected_duration_sec=cmd.expected_duration_sec,
        )


def build_probe_command(
    input_path: Path,
    ffprobe_path: str = "ffprobe",
) -> list[str]:
    """
    Build ffprobe command to analyze video.

    Returns:
        Command list for subprocess.
    """
    return [
        ffprobe_path,
        "-v", "quiet",
        "-print_format", "json",
        "-show_format",
        "-show_streams",
        str(input_path),
    ]


def build_validation_probe_command(
    output_path: Path,
    ffprobe_path: str = "ffprobe",
) -> list[str]:
    """
    Build ffprobe command to validate output.

    Returns:
        Command list for subprocess.
    """
    return [
        ffprobe_path,
        "-v", "error",
        "-print_format", "json",
        "-show_format",
        "-show_streams",
        "-show_error",
        str(output_path),
    ]
