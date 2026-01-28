"""
Utility functions for the Dropbox Video Transcoder.

Path handling, formatting, and common operations.
"""

from __future__ import annotations

import fnmatch
import re
from pathlib import Path, PurePosixPath


def normalize_dropbox_path(path: str) -> str:
    """
    Normalize a Dropbox path.

    - Ensures leading slash
    - Removes trailing slash
    - Normalizes separators
    """
    path = path.strip()
    path = path.replace('\\', '/')

    # Remove duplicate slashes
    while '//' in path:
        path = path.replace('//', '/')

    # Ensure leading slash
    if not path.startswith('/'):
        path = '/' + path

    # Remove trailing slash (unless root)
    if path != '/' and path.endswith('/'):
        path = path.rstrip('/')

    return path


def get_output_path(input_path: str) -> str:
    """
    Calculate output path for a given input path (R3).

    Input: /A/B/clip001.MP4
    Output: /A/B/h265/clip001.MP4

    Preserves exact filename and extension.
    """
    input_path = normalize_dropbox_path(input_path)
    p = PurePosixPath(input_path)

    # Parent directory + h265 subdirectory + same filename
    output_dir = p.parent / "h265"
    output_path = output_dir / p.name

    return str(output_path)


def get_h265_log_path(input_path: str) -> str:
    """
    Get path to h265 feito.txt log file for a given input path.

    Input: /A/B/clip001.MP4
    Output: /A/B/h265/h265 feito.txt
    """
    input_path = normalize_dropbox_path(input_path)
    p = PurePosixPath(input_path)
    h265_dir = p.parent / "h265"
    return str(h265_dir / "h265 feito.txt")


def is_in_h265_folder(path: str) -> bool:
    """
    Check if path is inside an h265 output folder (R4).

    Case-insensitive check for "/h265/" anywhere in path.
    """
    path_lower = path.lower()
    return '/h265/' in path_lower or path_lower.endswith('/h265')


def matches_exclude_pattern(path: str, patterns: list[str]) -> bool:
    """
    Check if path matches any exclude pattern.

    Uses glob-style matching.
    """
    for pattern in patterns:
        if fnmatch.fnmatch(path.lower(), pattern.lower()):
            return True
    return False


def is_video_file(path: str, extensions: list[str]) -> bool:
    """Check if path has a video file extension."""
    ext = Path(path).suffix.lower()
    return ext.lower() in [e.lower() for e in extensions]


def is_partial_file(path: str) -> bool:
    """Check if file is a partial/temp download."""
    name = Path(path).name.lower()
    return (
        name.endswith('.partial') or
        name.endswith('.tmp') or
        name.endswith('.part') or
        name.startswith('.') or
        name.startswith('~')
    )


def format_bytes(size: int) -> str:
    """Format bytes to human readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if abs(size) < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"


def format_duration(seconds: float) -> str:
    """Format duration in seconds to human readable string."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        mins = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{mins}m {secs}s"
    else:
        hours = int(seconds // 3600)
        mins = int((seconds % 3600) // 60)
        return f"{hours}h {mins}m"


def format_bitrate(kbps: int) -> str:
    """Format bitrate in kbps to human readable string."""
    if kbps < 1000:
        return f"{kbps} kbps"
    else:
        return f"{kbps / 1000:.1f} Mbps"


def parse_ffmpeg_progress(line: str) -> dict[str, any] | None:
    """
    Parse FFmpeg progress output line.

    Returns dict with parsed values or None if not a progress line.
    """
    progress = {}

    # Match time=HH:MM:SS.MS
    time_match = re.search(r'time=(\d{2}):(\d{2}):(\d{2})\.(\d{2})', line)
    if time_match:
        hours, mins, secs, ms = map(int, time_match.groups())
        progress['time_sec'] = hours * 3600 + mins * 60 + secs + ms / 100

    # Match speed=X.XXx
    speed_match = re.search(r'speed=\s*([\d.]+)x', line)
    if speed_match:
        progress['speed'] = float(speed_match.group(1))

    # Match frame=NNNN
    frame_match = re.search(r'frame=\s*(\d+)', line)
    if frame_match:
        progress['frame'] = int(frame_match.group(1))

    # Match fps=NN.N
    fps_match = re.search(r'fps=\s*([\d.]+)', line)
    if fps_match:
        progress['fps'] = float(fps_match.group(1))

    # Match bitrate=NNNNkbits/s
    bitrate_match = re.search(r'bitrate=\s*([\d.]+)kbits/s', line)
    if bitrate_match:
        progress['bitrate_kbps'] = float(bitrate_match.group(1))

    # Match size=NNNNN
    size_match = re.search(r'size=\s*(\d+)', line)
    if size_match:
        progress['size'] = int(size_match.group(1))

    return progress if progress else None


def safe_filename(name: str) -> str:
    """Make a filename safe for local filesystem."""
    # Remove/replace problematic characters
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    # Limit length
    if len(name) > 200:
        ext = Path(name).suffix
        name = name[:200 - len(ext)] + ext
    return name


def get_staging_paths(
    staging_dir: Path,
    job_id: int,
    original_name: str,
) -> tuple[Path, Path, Path]:
    """
    Get staging paths for a job.

    Returns:
        Tuple of (job_dir, input_path, output_path)
    """
    ext = Path(original_name).suffix
    safe_name = safe_filename(Path(original_name).stem)

    job_dir = staging_dir / f"job_{job_id}"
    input_path = job_dir / f"input{ext}"
    output_path = job_dir / f"output{ext}"

    return job_dir, input_path, output_path
