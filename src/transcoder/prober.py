"""
FFprobe video analysis module.

Probes video files to detect codec, duration, and other properties.
"""

from __future__ import annotations

import json
import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .ffmpeg_builder import VideoInfo

logger = logging.getLogger(__name__)


class ProbeError(Exception):
    """Error during video probing."""
    pass


@dataclass
class ProbeResult:
    """Result of video probe."""
    video_info: VideoInfo
    raw_data: dict[str, Any]
    is_hevc: bool


def probe_video(
    input_path: Path,
    ffprobe_path: str = "ffprobe",
    timeout: int = 60,
) -> ProbeResult:
    """
    Probe video file to get codec and format information.

    Args:
        input_path: Path to video file.
        ffprobe_path: Path to ffprobe binary.
        timeout: Command timeout in seconds.

    Returns:
        ProbeResult with video information.

    Raises:
        ProbeError: If probe fails.
    """
    if not input_path.exists():
        raise ProbeError(f"Input file not found: {input_path}")

    cmd = [
        ffprobe_path,
        "-v", "quiet",
        "-print_format", "json",
        "-show_format",
        "-show_streams",
        str(input_path),
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )

        if result.returncode != 0:
            raise ProbeError(f"ffprobe failed: {result.stderr}")

        data = json.loads(result.stdout)

    except subprocess.TimeoutExpired:
        raise ProbeError(f"ffprobe timeout after {timeout}s")
    except json.JSONDecodeError as e:
        raise ProbeError(f"Failed to parse ffprobe output: {e}")
    except FileNotFoundError:
        raise ProbeError(f"ffprobe not found at: {ffprobe_path}")

    # Find video stream
    video_stream = None
    audio_stream = None
    has_subtitles = False

    for stream in data.get('streams', []):
        codec_type = stream.get('codec_type')
        if codec_type == 'video' and video_stream is None:
            video_stream = stream
        elif codec_type == 'audio' and audio_stream is None:
            audio_stream = stream
        elif codec_type == 'subtitle':
            has_subtitles = True

    if not video_stream:
        raise ProbeError("No video stream found in file")

    # Extract video info
    codec_name = video_stream.get('codec_name', 'unknown')
    width = int(video_stream.get('width', 0))
    height = int(video_stream.get('height', 0))
    pix_fmt = video_stream.get('pix_fmt', 'yuv420p')

    # Parse frame rate
    fps = _parse_frame_rate(video_stream.get('r_frame_rate', '30/1'))

    # Parse duration (from format or stream)
    format_info = data.get('format', {})
    duration_str = video_stream.get('duration') or format_info.get('duration', '0')
    try:
        duration_sec = float(duration_str)
    except (ValueError, TypeError):
        duration_sec = 0.0

    # Parse bitrate
    bitrate_str = video_stream.get('bit_rate') or format_info.get('bit_rate', '0')
    try:
        bitrate_kbps = int(int(bitrate_str) / 1000)
    except (ValueError, TypeError):
        bitrate_kbps = 0

    # Detect bit depth
    bit_depth = _detect_bit_depth(video_stream, pix_fmt)

    # Audio codec
    audio_codec = audio_stream.get('codec_name') if audio_stream else None

    # Timecode: try the video stream's tags first (BMD/Apple cameras put it
    # there as e.g. "timecode": "14:39:32;17"), then fall back to a tmcd
    # data stream's tags. Premiere/Resolve read this when the H.265 lands
    # in a project — without it the multi-cam sync is gone.
    timecode = None
    for s in data.get('streams', []):
        tags = s.get('tags') or {}
        tc = tags.get('timecode') or tags.get('TIMECODE')
        if tc:
            timecode = tc
            break

    video_info = VideoInfo(
        codec_name=codec_name,
        width=width,
        height=height,
        fps=fps,
        duration_sec=duration_sec,
        bitrate_kbps=bitrate_kbps,
        bit_depth=bit_depth,
        pix_fmt=pix_fmt,
        has_audio=audio_stream is not None,
        has_subtitles=has_subtitles,
        audio_codec=audio_codec,
        timecode=timecode,
    )

    # Check if already HEVC (R1)
    is_hevc = codec_name.lower() in ('hevc', 'h265', 'hev1', 'hvc1')

    return ProbeResult(
        video_info=video_info,
        raw_data=data,
        is_hevc=is_hevc,
    )


def _parse_frame_rate(rate_str: str) -> float:
    """Parse frame rate string (e.g., '30/1' or '29.97')."""
    try:
        if '/' in rate_str:
            num, den = rate_str.split('/')
            return float(num) / float(den)
        return float(rate_str)
    except (ValueError, ZeroDivisionError):
        return 30.0


def _detect_bit_depth(video_stream: dict[str, Any], pix_fmt: str) -> int:
    """Detect video bit depth from stream info."""
    # Check bits_per_raw_sample
    bits = video_stream.get('bits_per_raw_sample')
    if bits:
        try:
            return int(bits)
        except ValueError:
            pass

    # Check pixel format
    pix_fmt_lower = pix_fmt.lower()
    if '10' in pix_fmt_lower or '10le' in pix_fmt_lower or '10be' in pix_fmt_lower:
        return 10
    elif '12' in pix_fmt_lower:
        return 12

    # Default to 8-bit
    return 8


def validate_output(
    output_path: Path,
    expected_duration_sec: float,
    ffprobe_path: str = "ffprobe",
    duration_tolerance: float = 0.01,  # 1% tolerance
    timeout: int = 60,
) -> tuple[bool, str | None]:
    """
    Validate transcoded output file.

    Checks:
    - File exists and has size > 0
    - Can be probed successfully
    - Duration matches expected within tolerance
    - Has video stream

    Args:
        output_path: Path to output file.
        expected_duration_sec: Expected duration from input.
        ffprobe_path: Path to ffprobe binary.
        duration_tolerance: Allowed duration difference ratio.
        timeout: Probe timeout.

    Returns:
        Tuple of (is_valid, error_message).
    """
    if not output_path.exists():
        return (False, "Output file does not exist")

    if output_path.stat().st_size == 0:
        return (False, "Output file is empty")

    try:
        result = probe_video(output_path, ffprobe_path, timeout)
    except ProbeError as e:
        return (False, f"Failed to probe output: {e}")

    # Check duration
    if expected_duration_sec > 0:
        duration_diff = abs(result.video_info.duration_sec - expected_duration_sec)
        max_diff = expected_duration_sec * duration_tolerance

        if duration_diff > max_diff:
            return (
                False,
                f"Duration mismatch: expected {expected_duration_sec:.1f}s, "
                f"got {result.video_info.duration_sec:.1f}s "
                f"(diff {duration_diff:.1f}s > {max_diff:.1f}s)"
            )

    # Verify it's HEVC
    if not result.is_hevc:
        return (
            False,
            f"Output is not HEVC: codec is {result.video_info.codec_name}"
        )

    return (True, None)


def get_video_info_string(video_info: VideoInfo) -> str:
    """Get human-readable video info string."""
    parts = [
        f"Codec: {video_info.codec_name}",
        f"Resolution: {video_info.width}x{video_info.height}",
        f"FPS: {video_info.fps:.2f}",
        f"Duration: {video_info.duration_sec:.1f}s",
        f"Bitrate: {video_info.bitrate_kbps} kbps",
        f"Bit depth: {video_info.bit_depth}-bit",
    ]
    if video_info.has_audio:
        parts.append(f"Audio: {video_info.audio_codec or 'yes'}")
    if video_info.has_subtitles:
        parts.append("Subtitles: yes")

    return " | ".join(parts)
