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
    """File stability detection settings (R2).

    Two profiles share the same shape — one for the bulk first-pass over a
    brand-new archive (aggressive, since static files don't need 45-minute
    guarantees) and one for steady-state watch mode on a settled tree.
    """
    poll_interval_sec: int = Field(
        default=300,
        ge=60,
        description="Interval between stability checks (seconds)"
    )
    checks_required: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Number of consecutive stable checks required"
    )
    min_age_sec: int = Field(
        default=900,
        ge=0,
        description="Minimum age since first stable check (seconds)"
    )


class StabilityProfiles(BaseModel):
    """Bulk-mode (first pass) vs steady-state stability profiles."""
    bulk: StabilitySettings = Field(
        default_factory=lambda: StabilitySettings(
            poll_interval_sec=60,
            checks_required=1,
            min_age_sec=0,
        ),
        description="Aggressive profile used during the initial bulk discovery pass."
    )
    steady: StabilitySettings = Field(
        default_factory=StabilitySettings,
        description="Conservative profile used in delta/steady-state mode."
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


class DiskBudgetSettings(BaseModel):
    """Staging disk budget for incoming downloads."""
    enabled: bool = Field(
        default=False,
        description="When true, DownloadWorker waits for budget before fetching"
    )
    max_staging_bytes: int = Field(
        default=2_000_000_000_000,  # 2 TB
        ge=1_073_741_824,           # 1 GB floor for tests
        description="Soft cap on total bytes reserved by in-flight downloads"
    )
    min_free_bytes: int = Field(
        default=500_000_000_000,    # 500 GB
        ge=1_073_741_824,
        description="Keep at least this many bytes free on the staging filesystem"
    )
    poll_interval_sec: int = Field(
        default=30,
        ge=1,
        description="How often a stalled DownloadWorker rechecks for available budget"
    )


class ScannerSettings(BaseModel):
    """Incremental scanner knobs."""
    cursor_checkpoint_entries: int = Field(
        default=500,
        ge=1,
        description="Persist the Dropbox cursor every N entries seen during a bulk pass"
    )
    feito_cache_ttl_sec: int = Field(
        default=3600,
        ge=60,
        description="Max age of a cached feito.txt read before it is refetched"
    )


class ApiSettings(BaseModel):
    """Local HTTP API settings."""
    enabled: bool = Field(
        default=True,
        description="When true, the daemon serves a JSON/HTML status API on loopback"
    )
    bind: str = Field(
        default="127.0.0.1",
        description="Interface to bind. 127.0.0.1 = local PC only (no auth needed). "
                    "0.0.0.0 = anyone on the LAN can reach it (auth REQUIRED, set "
                    "access_token below)."
    )
    port: int = Field(
        default=9123,
        ge=1,
        le=65535,
        description="TCP port for the status API"
    )
    access_token: str = Field(
        default="",
        description="If non-empty, every request must carry this token via "
                    "?token=X query param, Authorization: Bearer X header, or "
                    "the hd_token cookie set by a prior visit. Auto-generated "
                    "on first bind=0.0.0.0 startup if left blank."
    )


class UpdaterSettings(BaseModel):
    """Notify-only update check against GitHub Releases."""
    enabled: bool = Field(
        default=True,
        description="When true, the daemon checks GitHub Releases on startup"
    )
    github_repo: str = Field(
        default="luisimperator/rep01",
        description="GitHub repo to query for latest release (owner/name)"
    )
    check_timeout_sec: float = Field(
        default=5.0,
        ge=1.0,
        le=60.0,
        description="Network timeout for the release check; failure is non-fatal"
    )


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


class IncidentsSettings(BaseModel):
    """Auto-reporting of daemon errors as GitHub Issues."""
    enabled: bool = Field(
        default=False,
        description="Open/update GitHub Issues automatically on transcode/scan errors."
    )
    github_repo: str = Field(
        default="luisimperator/rep01",
        description="Target repo, in 'owner/name' form."
    )
    github_token: str = Field(
        default="",
        description="GitHub PAT with issues:write. Use env GITHUB_TOKEN to keep out of yaml."
    )
    throttle_sec: int = Field(
        default=600,
        ge=10,
        description="Identical errors within this window are coalesced into comments on the same issue."
    )
    health_check_interval_minutes: int = Field(
        default=10,
        ge=1,
        le=1440,
        description="How often the self-health agent runs its checks. 10min for "
                    "setup/debug, 180min (=3h) is the long-term default."
    )


class Config(BaseModel):
    """Main configuration model."""

    # Dropbox settings
    # Auth supports two modes:
    #   1) Short-lived access token (legacy):  dropbox_token only.
    #      App Console "Generate access token" button. Expires in ~4h, so the
    #      daemon will die when the token expires. Useful for ad-hoc testing.
    #   2) Refresh token (recommended for daemon):
    #      dropbox_app_key + dropbox_refresh_token. The SDK refreshes the
    #      access token automatically; the daemon runs for months unattended.
    #      Run `hd auth` to obtain a refresh token via PKCE OAuth.
    dropbox_token: str = Field(
        default="",
        description="Dropbox short-lived access token (env DROPBOX_TOKEN)"
    )
    dropbox_app_key: str = Field(
        default="",
        description="Dropbox app key (for refresh-token auth)"
    )
    dropbox_app_secret: str = Field(
        default="",
        description="Dropbox app secret (optional; PKCE flow does not need it)"
    )
    dropbox_refresh_token: str = Field(
        default="",
        description="Dropbox long-lived refresh token (env DROPBOX_REFRESH_TOKEN)"
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
        default=25,
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
    stability_profiles: StabilityProfiles = Field(default_factory=StabilityProfiles)

    # Concurrency
    concurrency: ConcurrencySettings = Field(default_factory=ConcurrencySettings)

    # Watchdog
    watchdog: WatchdogSettings = Field(default_factory=WatchdogSettings)

    # Central dispatcher (bounded worker queues fed by a single DB-reading thread)
    dispatcher: DispatcherSettings = Field(default_factory=DispatcherSettings)

    # Incremental scanner (cursor persistence, feito-log cache)
    scanner: ScannerSettings = Field(default_factory=ScannerSettings)

    # Staging disk budget — pauses new downloads when near the disk cap
    disk_budget: DiskBudgetSettings = Field(default_factory=DiskBudgetSettings)

    # Local HTTP status API (GUI thin client + curl inspection)
    api: ApiSettings = Field(default_factory=ApiSettings)

    # Update-notification via GitHub Releases (notify-only; apply via `hd update`)
    updater: UpdaterSettings = Field(default_factory=UpdaterSettings)
    incidents: IncidentsSettings = Field(default_factory=IncidentsSettings)

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
        default=["*/h265/*", "*/.h265/*", "*/H265/*", "*/h264/*", "*/H264/*"],
        description="Glob patterns to exclude (R4). h264/ folders contain originals "
                    "backed up by legacy_reorganize and must be skipped on rescans."
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
    legacy_reorganize: bool = Field(
        default=True,
        description=(
            "Replicate the legacy GUI's post-upload reorganization: move the "
            "original H.264 to <parent>/h264/<name>, then move the H.265 from "
            "<parent>/h265/<name> to <parent>/<name> so it takes the original's "
            "place. Also appends to <parent>/h265/h265 feito.txt."
        ),
    )
    legacy_reorganize_min_age_days: int = Field(
        default=60,
        ge=0,
        description=(
            "Skip reorganization for folders that have any user activity in the "
            "last N days. Set to 0 to always reorganize. Used both at upload "
            "time and by `hd reorganize-existing`."
        ),
    )
    legacy_reorganize_delete_h264_after_seconds: int = Field(
        default=0,
        ge=0,
        description=(
            "After a per-folder reorganize batch succeeds, schedule deletion "
            "of the <parent>/h264/ backup folder this many seconds later. "
            "0 disables deletion (default — keep backups). Dropbox keeps "
            "deleted files in its history for 30 days (Plus) or 180 days "
            "(Business), so this is recoverable."
        ),
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
    preflight_hevc_probe_mb: int = Field(
        default=16,
        ge=0,
        le=512,
        description=(
            "Range-download N MB from the head (and from the tail if the head "
            "probe is inconclusive) before the full download, then run ffprobe "
            "on the chunk to detect natively-encoded HEVC files. When detected, "
            "the full download is short-circuited to SKIPPED_HEVC. Set 0 to "
            "disable. 16 MB covers the typical MP4 moov atom for camera/Premiere "
            "exports without burning bandwidth."
        ),
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

    @field_validator('dropbox_refresh_token', mode='before')
    @classmethod
    def get_refresh_token_from_env(cls, v: str) -> str:
        """Get Dropbox refresh token from environment if not set."""
        if not v:
            return os.environ.get('DROPBOX_REFRESH_TOKEN', '')
        return v

    @field_validator('dropbox_app_key', mode='before')
    @classmethod
    def get_app_key_from_env(cls, v: str) -> str:
        if not v:
            return os.environ.get('DROPBOX_APP_KEY', '')
        return v

    @field_validator('dropbox_app_secret', mode='before')
    @classmethod
    def get_app_secret_from_env(cls, v: str) -> str:
        if not v:
            return os.environ.get('DROPBOX_APP_SECRET', '')
        return v

    def has_dropbox_auth(self) -> bool:
        """True if either auth mode is configured."""
        if self.dropbox_refresh_token and self.dropbox_app_key:
            return True
        if self.dropbox_token:
            return True
        return False

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

    # Try loading from file. Always read as UTF-8: on Windows `open()` defaults
    # to the system code page (cp1252) and will choke on any non-ASCII byte or
    # a UTF-8 BOM the installer/notepad may have written.
    if config_path:
        config_path = Path(config_path)
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8-sig') as f:
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
                with open(path, 'r', encoding='utf-8-sig') as f:
                    config_data = yaml.safe_load(f) or {}
                break

    # Create config with defaults and overrides
    config = Config(**config_data)

    return config


def save_example_config(path: Path) -> None:
    """Save an example configuration file."""
    example = {
        'dropbox_token': '${DROPBOX_TOKEN}',
        'dropbox_app_key': '',
        'dropbox_app_secret': '',
        'dropbox_refresh_token': '${DROPBOX_REFRESH_TOKEN}',
        'dropbox_root': '/Videos',
        'local_staging_dir': '/data/transcoder/staging',
        'local_output_dir': '/data/transcoder/output',
        'database_path': '/data/transcoder/transcoder.db',
        'lockfile_path': '/var/run/transcoder.lock',
        'log_dir': '/var/log/transcoder',
        'encoder_preference': 'auto',
        'profile': 'quality',
        'cq_value': 25,
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
        'stability_profiles': {
            'bulk':   {'poll_interval_sec': 60,  'checks_required': 1, 'min_age_sec': 0},
            'steady': {'poll_interval_sec': 300, 'checks_required': 3, 'min_age_sec': 900},
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
        'scanner': {
            'cursor_checkpoint_entries': 500,
            'feito_cache_ttl_sec': 3600,
        },
        'disk_budget': {
            'enabled': False,
            'max_staging_bytes': 2_000_000_000_000,
            'min_free_bytes':      500_000_000_000,
            'poll_interval_sec': 30,
        },
        'api': {
            'enabled': True,
            'bind': '127.0.0.1',
            'port': 9123,
        },
        'updater': {
            'enabled': True,
            'github_repo': 'luisimperator/rep01',
            'check_timeout_sec': 5.0,
        },
        'dropbox_api': {
            'rate_per_min': 600,
            'burst': 50,
        },
        'output_layout': 'sibling',
        'output_mirror_root': '_h265_output',
        'video_extensions': ['.mp4', '.mov', '.MP4', '.MOV'],
        'exclude_patterns': ['*/h265/*', '*/.h265/*', '*/H265/*', '*/h264/*', '*/H264/*'],
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

    with open(path, 'w', encoding='utf-8') as f:
        yaml.dump(example, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
