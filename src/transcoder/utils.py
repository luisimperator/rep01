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


def get_output_path(
    input_path: str,
    layout: str = "sibling",
    dropbox_root: str | None = None,
    mirror_root: str = "_h265_output",
) -> str:
    """
    Calculate output path for a given input path (R3).

    Layouts:
    - sibling (legacy): /A/B/clip001.MP4 -> /A/B/h265/clip001.MP4.
      Collides when sibling folders contain files with the same name.
    - mirror: /A/B/clip001.MP4 with dropbox_root=/ -> /_h265_output/A/B/clip001.MP4.
      Preserves the source folder hierarchy under a single output root.

    Preserves exact filename and extension.
    """
    input_path = normalize_dropbox_path(input_path)
    p = PurePosixPath(input_path)

    if layout == "mirror":
        root = normalize_dropbox_path(dropbox_root or "/")
        root_p = PurePosixPath(root)
        try:
            rel = p.relative_to(root_p)
        except ValueError:
            # input is not under dropbox_root; fall back to sibling layout
            output_dir = p.parent / "h265"
            return str(output_dir / p.name)
        # Strip the filename: rel.parent is the relative folder hierarchy
        output_dir = root_p / mirror_root / rel.parent
        output_path = output_dir / p.name
        return str(output_path)

    # sibling (default / legacy)
    output_dir = p.parent / "h265"
    output_path = output_dir / p.name
    return str(output_path)


def get_h265_log_path(
    input_path: str,
    layout: str = "sibling",
    dropbox_root: str | None = None,
    mirror_root: str = "_h265_output",
) -> str:
    """
    Get path to h265 feito.txt log file for a given input path.

    Tracks the same layout convention as get_output_path so the log lives
    next to the output it describes.
    """
    output_path = get_output_path(input_path, layout, dropbox_root, mirror_root)
    p = PurePosixPath(output_path)
    return str(p.parent / "h265 feito.txt")


def is_in_h265_folder(path: str, mirror_root: str = "_h265_output") -> bool:
    """
    Check if path is inside an h265 output folder (R4).

    Case-insensitive check for "/h265/" or the configured mirror_root anywhere
    in the path.
    """
    path_lower = path.lower()
    if '/h265/' in path_lower or path_lower.endswith('/h265'):
        return True
    mr = mirror_root.lower().strip('/')
    if mr and (f'/{mr}/' in path_lower or path_lower.endswith(f'/{mr}')):
        return True
    return False


def matches_exclude_pattern(path: str, patterns: list[str]) -> bool:
    """
    Check if path matches any exclude pattern.

    Uses glob-style matching.
    """
    for pattern in patterns:
        if fnmatch.fnmatch(path.lower(), pattern.lower()):
            return True
    return False


_ASSETS_SEGMENT_NAMES = {"assets", "Assets", "ASSETS"}


def path_has_assets_segment(path: str) -> bool:
    """True when any path segment matches an 'assets' folder name.

    Independent of config.exclude_patterns so a user override on that
    list can never accidentally re-enable scanning of project resource
    folders. Walks all segments so deeply-nested files like
    `/foo/assets/sub/sub/file.mp4` are caught.
    """
    if not path:
        return False
    from pathlib import PurePosixPath
    for seg in PurePosixPath(path).parts:
        if seg in _ASSETS_SEGMENT_NAMES:
            return True
    return False


# Adobe Premiere Pro writes timeline render previews into a folder named
# "Adobe Premiere Pro Video Previews" alongside the .prproj. Inside it,
# every render produces a UUID-named .mov in a `<sequence>.PRV/` subdir.
# These are EPHEMERAL — Premiere regenerates them whenever the timeline
# changes — so transcoding them is pure waste of bandwidth and CPU.
_PREMIERE_PREVIEW_FOLDER = "Adobe Premiere Pro Video Previews"


def path_is_premiere_preview(path: str) -> bool:
    """True when the path lives under an Adobe Premiere Pro preview cache.

    Matches the literal folder name Premiere uses; case-insensitive. Walks
    all path segments so deep nesting (`*.PRV/Rendered - <uuid>.mov`) is
    still caught.
    """
    if not path:
        return False
    from pathlib import PurePosixPath
    target = _PREMIERE_PREVIEW_FOLDER.lower()
    for seg in PurePosixPath(path).parts:
        if seg.lower() == target:
            return True
    return False


# Sony cameras (FX3, A7S III, etc.) write low-bitrate proxy copies of every
# clip into a `Proxies/` subfolder, named `<original>_Proxy.<ext>`. They're
# only useful for offline edit on slower machines — if the user has the
# originals (we already scan those), transcoding the proxies is pure waste.
_PROXIES_SEGMENT_NAMES = {"Proxies", "proxies", "PROXIES"}


def path_is_camera_proxy(path: str) -> bool:
    """True when the path is a Sony-style camera proxy file.

    Pattern: any path segment named `Proxies/` AND the file basename ends
    with `_Proxy.<ext>` (Sony convention). The basename check is the
    safety net so unrelated files in a folder coincidentally called
    "Proxies/" don't get nuked.
    """
    if not path:
        return False
    from pathlib import PurePosixPath
    p = PurePosixPath(path)
    if not any(seg in _PROXIES_SEGMENT_NAMES for seg in p.parts):
        return False
    stem = p.stem
    return stem.lower().endswith("_proxy")


# Codecs that ffprobe reports for files that are technically images
# wrapped in a movie container (After Effects exports, motion graphics
# templates, etc). They're not real video and trying to transcode them
# usually fails (PNG with rgb24/gbr colorspace is the canonical case).
IMAGE_CODECS = frozenset({
    "png", "mjpeg", "mjpegb", "jpeg2000", "jpegls",
    "gif", "bmp", "tiff", "webp", "ppm", "pgm", "pgmyuv",
    "targa", "exr", "dpx", "psd", "qoi", "smc",
})


def is_image_codec(codec_name: str | None) -> bool:
    """True when the codec is an image format wrapped in a video container.
    These shouldn't be transcoded as video — they're typically After Effects
    exports of logos / overlays / lower thirds."""
    return bool(codec_name) and codec_name.lower() in IMAGE_CODECS


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


def is_youtube_download(path: str) -> bool:
    """
    Check if file appears to be downloaded from YouTube.

    YouTube downloads (e.g., from yt-dlp) already have good compression
    and should not be transcoded again. Detects patterns like:
    - (2160p_24fps_AV1-128kbit_AAC)
    - (1080p_30fps_H264-128kbit_AAC)
    - (720p_60fps_VP9-256kbit_OPUS)
    """
    name = Path(path).name
    # Pattern: (RESp_FPSfps_CODEC-BITRATEkbit_AUDIO)
    # Matches common YouTube download naming from yt-dlp and similar tools
    youtube_pattern = re.compile(
        r'\(\d+p_\d+fps_[A-Za-z0-9]+-\d+kbit_[A-Za-z0-9]+\)'
    )
    return bool(youtube_pattern.search(name))


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
