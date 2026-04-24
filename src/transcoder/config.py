"""
Configuration management for the Dropbox Video Transcoder.

Supports YAML configuration files with environment variable overrides.
"""

from __future__ import annotations

import os
from enum import Enum
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator


class EncoderPreference(str, Enum):
    """Hardware encoder preference."""
    AUTO = "auto"
    QSV = "qsv"
    NVENC = "nvenc"
    CPU = "cpu"


class TranscodeProfile(str, Enum):
    """Transcoding quality profile."""
    BALANCED = "balanced"
    QUALITY = "quality"


class BitrateSettings(BaseModel):
    """Bitrate settings for balanced profile."""
    target_mbps: int = Field(default=40, ge=1, le=200, description="Target bitrate in Mbps")
    max_mbps: int = Field(default=60, ge=1, le=300, description="Max bitrate in Mbps")
    bufsize_mbps: int = Field(default=120, ge=1, le=600, description="Buffer size in Mbps")


class StabilitySettings(BaseModel):
    """File stability detection settings (R2)."""
    poll_interval_sec: int = Field(
        default=300,
        ge=60,
        description="Interval between stability checks (seconds)"
    )
    checks_required: int = Field(
        default=3,
        ge=2,
        le=10,
        description="Number of consecutive stable checks required"
    )
    min_age_sec: int = Field(
        default=900,
        ge=300,
        description="Minimum age since first stable check (seconds)"
    )


class ConcurrencySettings(BaseModel):
    """Worker concurrency settings."""
    scan_interval_sec: int = Field(
        default=600,
        ge=60,
        description="Interval between full scans (seconds)"
    )
    download_workers: int = Field(
        default=2,
        ge=1,
        le=8,
        description="Number of parallel download workers"
    )
    transcode_workers: int = Field(
        default=2,
        ge=1,
        le=8,
        description="Number of parallel transcode workers"
    )
    upload_workers: int = Field(
        default=2,
        ge=1,
        le=8,
        description="Number of parallel upload workers"
    )


class OutputLayout(str, Enum):
    """Output path layout."""
    SIBLING = "sibling"  # {parent}/h265/{name} — legacy, collides across sibling folders
    MIRROR = "mirror"    # {root}/{mirror_root}/{relative_path}/{name} — collision-free for 200TB


class DispatcherSettings(BaseModel):
    """Central job dispatcher settings."""
    poll_interval_sec: float = Field(
        default=2.0,
        ge=0.5,
        le=30.0,
        description="How often the dispatcher refills worker queues from the DB"
    )
    queue_multiplier: int = Field(
        default=4,
        ge=1,
        le=32,
        description="Queue size = workers * this; controls pipelining slack"
    )


class DropboxApiSettings(BaseModel):
    """Dropbox API rate limiting (token bucket)."""
    rate_per_min: int = Field(
        default=600,
        ge=1,
        description="Average API calls per minute permitted"
    )
    burst: int = Field(
        default=50,
        ge=1,
        description="Maximum burst of calls before throttling kicks in"
    )


class WatchdogSettings(BaseModel):
    """Job timeout and watchdog settings."""
    download_timeout_sec: int = Field(
        default=7200,
        ge=300,
        description="Download timeout (seconds)"
    )
    transcode_timeout_sec: int = Field(
        default=86400,
        ge=3600,
        description="Transcode timeout (seconds) - 24h default for large files"
    )
    upload_timeout_sec: int = Field(
        default=7200,
        ge=300,
        description="Upload timeout (seconds)"
    )
    max_retries: int = Field(
        default=10,
        ge=1,
        le=50,
        description="Maximum retry attempts with exponential backoff"
    )


class Config(BaseModel):
    """Main configuration model."""

    # Dropbox settings
    dropbox_token: str = Field(
        default="",
        description="Dropbox API access token (can use env DROPBOX_TOKEN)"
    )
    dropbox_root: str = Field(
        default="/Videos",
        description="Root folder to monitor in Dropbox"
    )

    # Local paths
    local_staging_dir: Path = Field(
        default=Path("/tmp/transcoder/staging"),
        description="Local staging directory for downloads"
    )
    local_output_dir: Path = Field(
        default=Path("/tmp/transcoder/output"),
        description="Local output directory for transcoded files"
    )
    database_path: Path = Field(
        default=Path("/tmp/transcoder/transcoder.db"),
        description="SQLite database path"
    )
    lockfile_path: Path = Field(
        default=Path("/tmp/transcoder/transcoder.lock"),
        description="Lockfile to prevent multiple instances"
    )
    log_dir: Path = Field(
        default=Path("/tmp/transcoder/logs"),
        description="Directory for FFmpeg and job logs"
    )

    # Encoder settings
    encoder_preference: EncoderPreference = Field(
        default=EncoderPreference.AUTO,
        description="Encoder preference: auto/qsv/nvenc/cpu"
    )
    profile: TranscodeProfile = Field(
        default=TranscodeProfile.QUALITY,
        description="Transcoding profile: balanced/quality"
    )

    # Quality settings (R6)
    cq_value: int = Field(
        default=24,
        ge=15,
        le=35,
        description="CQ/CRF value for quality profile"
    )
    cpu_crf_equivalent: int = Field(
        default=23,
        ge=18,
        le=28,
        description="CPU CRF equivalent for CQ 24 (typically 22-24)"
    )

    # Size filter (R5)
    min_size_gb: float = Field(
        default=6.0,
        ge=0.0,
        description="Minimum file size in GB (files smaller are skipped)"
    )

    # Bitrate settings for balanced profile
    bitrate: BitrateSettings = Field(default_factory=BitrateSettings)

    # Stability settings (R2)
    stability: StabilitySettings = Field(default_factory=StabilitySettings)

    # Concurrency
    concurrency: ConcurrencySettings = Field(default_factory=ConcurrencySettings)

    # Watchdog
    watchdog: WatchdogSettings = Field(default_factory=WatchdogSettings)

    # Central dispatcher (bounded worker queues fed by a single DB-reading thread)
    dispatcher: DispatcherSettings = Field(default_factory=DispatcherSettings)

    # Dropbox API token-bucket rate limiter
    dropbox_api: DropboxApiSettings = Field(default_factory=DropboxApiSettings)

    # Output path layout (R3): "sibling" preserves legacy {parent}/h265/{name},
    # "mirror" writes to {dropbox_root}/{output_mirror_root}/{rel}/{name} — required
    # for set-and-forget on large trees where sibling folders share filenames.
    output_layout: OutputLayout = Field(
        default=OutputLayout.SIBLING,
        description="Output path layout: sibling (legacy) or mirror (collision-free)"
    )
    output_mirror_root: str = Field(
        default="_h265_output",
        description="Top-level folder under dropbox_root used when output_layout=mirror"
    )

    # File patterns
    video_extensions: list[str] = Field(
        default=[".mp4", ".mov", ".MP4", ".MOV"],
        description="Video file extensions to process"
    )
    exclude_patterns: list[str] = Field(
        default=["*/h265/*", "*/.h265/*", "*/H265/*"],
        description="Glob patterns to exclude (R4)"
    )

    # Behavior flags
    upload_to_dropbox: bool = Field(
        default=True,
        description="Upload transcoded files back to Dropbox"
    )
    delete_staging_after_upload: bool = Field(
        default=True,
        description="Delete local staging files after successful upload"
    )
    allow_delete_original: bool = Field(
        default=False,
        description="DANGEROUS: Allow deleting original after successful transcode"
    )
    delete_original_delay_hours: int = Field(
        default=168,
        ge=24,
        description="Safety delay before deleting original (hours)"
    )
    allow_mkv_fallback: bool = Field(
        default=False,
        description="Allow fallback to MKV container if MP4/MOV fails"
    )

    # FFmpeg settings
    ffmpeg_path: str = Field(
        default="ffmpeg",
        description="Path to ffmpeg binary"
    )
    ffprobe_path: str = Field(
        default="ffprobe",
        description="Path to ffprobe binary"
    )
    ffmpeg_extra_args: list[str] = Field(
        default=[],
        description="Extra arguments to pass to ffmpeg"
    )

    # GOP size
    gop_size: int = Field(
        default=60,
        ge=1,
        le=300,
        description="GOP size (keyframe interval)"
    )

    # Audio settings
    audio_fallback_codec: str = Field(
        default="aac",
        description="Audio codec for fallback encoding"
    )
    audio_fallback_bitrate: str = Field(
        default="320k",
        description="Audio bitrate for fallback encoding"
    )

    @field_validator('dropbox_token', mode='before')
    @classmethod
    def get_token_from_env(cls, v: str) -> str:
        """Get Dropbox token from environment if not set."""
        if not v:
            return os.environ.get('DROPBOX_TOKEN', '')
        return v

    @field_validator('local_staging_dir', 'local_output_dir', 'database_path',
                     'lockfile_path', 'log_dir', mode='before')
    @classmethod
    def expand_path(cls, v: str | Path) -> Path:
        """Expand user home and environment variables in paths."""
        if isinstance(v, str):
            v = os.path.expandvars(os.path.expanduser(v))
        return Path(v)

    def min_size_bytes(self) -> int:
        """Get minimum size in bytes."""
        return int(self.min_size_gb * 1024 * 1024 * 1024)

    def ensure_directories(self) -> None:
        """Create necessary directories if they don't exist."""
        self.local_staging_dir.mkdir(parents=True, exist_ok=True)
        self.local_output_dir.mkdir(parents=True, exist_ok=True)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.lockfile_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)


def load_config(config_path: Path | str | None = None) -> Config:
    """
    Load configuration from YAML file.

    Args:
        config_path: Path to config file. If None, uses default paths.

    Returns:
        Loaded and validated Config object.
    """
    config_data: dict[str, Any] = {}

    # Try loading from file
    if config_path:
        config_path = Path(config_path)
        if config_path.exists():
            with open(config_path, 'r') as f:
                config_data = yaml.safe_load(f) or {}
    else:
        # Try default paths
        default_paths = [
            Path('config.yaml'),
            Path('config.yml'),
            Path.home() / '.config' / 'transcoder' / 'config.yaml',
            Path('/etc/transcoder/config.yaml'),
        ]
        for path in default_paths:
            if path.exists():
                with open(path, 'r') as f:
                    config_data = yaml.safe_load(f) or {}
                break

    # Create config with defaults and overrides
    config = Config(**config_data)

    return config


def save_example_config(path: Path) -> None:
    """Save an example configuration file."""
    example = {
        'dropbox_token': '${DROPBOX_TOKEN}',
        'dropbox_root': '/Videos',
        'local_staging_dir': '/data/transcoder/staging',
        'local_output_dir': '/data/transcoder/output',
        'database_path': '/data/transcoder/transcoder.db',
        'lockfile_path': '/var/run/transcoder.lock',
        'log_dir': '/var/log/transcoder',
        'encoder_preference': 'auto',
        'profile': 'quality',
        'cq_value': 24,
        'cpu_crf_equivalent': 23,
        'min_size_gb': 6.0,
        'bitrate': {
            'target_mbps': 40,
            'max_mbps': 60,
            'bufsize_mbps': 120,
        },
        'stability': {
            'poll_interval_sec': 300,
            'checks_required': 3,
            'min_age_sec': 900,
        },
        'concurrency': {
            'scan_interval_sec': 600,
            'download_workers': 2,
            'transcode_workers': 2,
            'upload_workers': 2,
        },
        'watchdog': {
            'download_timeout_sec': 7200,
            'transcode_timeout_sec': 86400,
            'upload_timeout_sec': 7200,
            'max_retries': 10,
        },
        'dispatcher': {
            'poll_interval_sec': 2.0,
            'queue_multiplier': 4,
        },
        'dropbox_api': {
            'rate_per_min': 600,
            'burst': 50,
        },
        'output_layout': 'sibling',
        'output_mirror_root': '_h265_output',
        'video_extensions': ['.mp4', '.mov', '.MP4', '.MOV'],
        'exclude_patterns': ['*/h265/*', '*/.h265/*', '*/H265/*'],
        'upload_to_dropbox': True,
        'delete_staging_after_upload': True,
        'allow_delete_original': False,
        'delete_original_delay_hours': 168,
        'allow_mkv_fallback': False,
        'ffmpeg_path': 'ffmpeg',
        'ffprobe_path': 'ffprobe',
        'ffmpeg_extra_args': [],
        'gop_size': 60,
        'audio_fallback_codec': 'aac',
        'audio_fallback_bitrate': '320k',
    }

    with open(path, 'w') as f:
        yaml.dump(example, f, default_flow_style=False, sort_keys=False)
