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
    audio_workers: int = Field(
        default=2,
        ge=1,
        le=8,
        description=(
            "Number of parallel WAV→MP3 workers. CPU-bound (libmp3lame), "
            "doesn't compete with QSV/NVENC video transcoding."
        ),
    )


class AudioSettings(BaseModel):
    """WAV → MP3 conversion pipeline (CPU only, parallel to video QSV)."""
    enabled: bool = Field(
        default=True,
        description=(
            "Enable WAV → MP3 192kbps conversion for files inside any folder "
            "named exactly 'Audio Source Files' (case-insensitive, the layout "
            "the ATEM hardware writes). Disabled = scanner ignores audio."
        ),
    )
    source_folder_name: str = Field(
        default="Audio Source Files",
        description=(
            "Folder name (case-insensitive) that gates audio discovery. WAVs "
            "outside folders matching this name are never queued."
        ),
    )
    bitrate_kbps: int = Field(
        default=192,
        ge=64,
        le=320,
        description="MP3 bitrate (CBR) in kbps. 192 matches the legacy GUI default.",
    )
    extensions: list[str] = Field(
        default_factory=lambda: [".wav"],
        description="File extensions (lowercase, dot-prefixed) treated as audio sources.",
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
        default=1_200_000_000_000,  # 1.2 TB — conservative for 2 TB staging disks;
                                    # leaves room for transcoded outputs, OS overhead,
                                    # page file, and recycle bin without ENOSPC.
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
    convoy_throttle_sec: float = Field(
        default=5.0,
        ge=0.0,
        le=30.0,
        description=(
            "Convoy mode: sleep N seconds per download chunk on non-leader "
            "workers when the transcoder queue is empty and ≥2 downloaders "
            "are in flight. Frees bandwidth so the leader finishes faster "
            "and feeds the transcoder. 0 disables convoy mode."
        ),
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


class CensusSettings(BaseModel):
    """Reduction-map census worker settings.

    The census walks the entire Dropbox tree under dropbox_root once a day,
    classifies every file (pending / done / ineligible), and stores a
    per-folder rollup in folder_census. The dashboard renders the colored
    tree from that snapshot. Walking 50TB takes minutes, so we run it
    sparingly — once a day is plenty.
    """
    enabled: bool = Field(
        default=True,
        description=(
            "Master switch. When false, the census thread doesn't start; "
            "the reduction-map tree on the dashboard shows the last "
            "snapshot (or empty if none yet)."
        ),
    )
    daily_run_at: str = Field(
        default="08:30",
        description=(
            "Local clock time HH:MM the census runs each day. The daemon "
            "wakes up, walks the tree, refreshes folder_census, then sleeps "
            "until the next 08:30 (or whatever's configured). The /api/"
            "census-now endpoint can also trigger an out-of-band run."
        ),
    )
    initial_run_on_startup: bool = Field(
        default=True,
        description=(
            "Run a census once shortly after the daemon starts so the "
            "dashboard has data on first boot. Subsequent runs follow the "
            "daily_run_at schedule."
        ),
    )
    initial_run_delay_sec: int = Field(
        default=120,
        ge=0,
        description=(
            "Delay before the startup census kicks off. Lets the rest of "
            "the daemon settle (Dropbox auth, dispatcher, workers) before "
            "the long Dropbox walk starts."
        ),
    )
    deep_scan_concurrency: int = Field(
        default=8,
        ge=1,
        le=32,
        description=(
            "Parallel ffprobe workers during a deep scan. The Dropbox API "
            "rate limit (config.dropbox_api.rate_per_min, default 600/min) "
            "caps the practical sweet spot at 8-16 — past that you start "
            "burning backoff. Each parallel probe also pulls ~5MB from the "
            "CDN, so a faster WAN tolerates higher concurrency."
        ),
    )
    deep_scan_pipeline_throttle_mbps: float = Field(
        default=1.0,
        ge=0.0,
        le=1000.0,
        description=(
            "Cap (MB/s) the main pipeline's downloads/uploads while a deep "
            "scan runs, so the deep-scan probes get bandwidth priority. "
            "0 disables the throttle (pipeline runs at full speed alongside "
            "deep scan, no priority claim)."
        ),
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

    # WAV → MP3 audio pipeline (runs in parallel to video transcode)
    audio: AudioSettings = Field(default_factory=AudioSettings)

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

    # Reduction-map census: daily Dropbox tree walk that classifies every
    # file (pending/done/ineligible) and powers the dashboard's colored
    # folder tree.
    census: CensusSettings = Field(default_factory=CensusSettings)

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
        default=["*/h265/*", "*/.h265/*", "*/H265/*", "*/h264/*", "*/H264/*", "*/assets/*", "*/Assets/*"],
        description="Glob patterns to exclude (R4). h264/ folders contain originals "
                    "backed up by legacy_reorganize and must be skipped on rescans. "
                    "/assets/ folders are project resources (LUTs, plates, fonts) "
                    "and must never be scanned for transcode."
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
    low_bitrate_skip_mbps_per_megapixel: float = Field(
        default=3.0,
        ge=0.0,
        description=(
            "Skip files whose video bitrate per megapixel is below this "
            "threshold. Catches YouTube downloads / streaming-grade "
            "files where re-transcoding to H.265 buys little (and often "
            "produces a LARGER file at the configured CQ). The check "
            "scales by resolution: 1080p (~2 MP) skips below ~6 Mbps, "
            "4K (~8 MP) skips below ~25 Mbps. Set to 0 to disable. "
            "Job is marked SKIPPED_LOW_BITRATE and the staging is "
            "cleaned up — no upload, no reorganize."
        ),
    )
    storage_target_tb: float = Field(
        default=50.0,
        ge=0.0,
        description=(
            "Storage usage target in TB displayed on the dashboard. "
            "Drives the progress bar in the 'Dropbox Storage' card "
            "(used / target). Doesn't enforce anything — purely visual."
        ),
    )
    preserve_chroma_422: bool = Field(
        default=False,
        description=(
            "When the source is 4:2:2 (typically ATEM ProRes proxies or "
            "A7siii XAVC-S-I High 4:2:2 in 10-bit), preserve the chroma "
            "subsampling on output instead of downsampling to 4:2:0. "
            "DANGER: forces libx265 (CPU) for those jobs because QSV / "
            "NVENC consumer hardware does not implement HEVC Main 4:2:2. "
            "Encoding is roughly 10x slower (4K 10-bit 4:2:2 at preset "
            "medium runs ~0.1-0.2x real-time on a strong CPU). Leave OFF "
            "for the default workflow; flip ON only for masters that "
            "need the chroma fidelity (graphics, chroma key)."
        ),
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
    legacy_reorganize_delete_wav_after_seconds: int = Field(
        default=300,
        ge=0,
        description=(
            "Same as legacy_reorganize_delete_h264_after_seconds but for the "
            "<parent>/wav/ backup that the audio pipeline produces. Default "
            "300 = 5 minutes; the originals are recoverable from Dropbox "
            "version history."
        ),
    )
    cleanup_dot_underscore: bool = Field(
        default=True,
        description=(
            "After every successful per-folder reorganize batch, sweep "
            "`._*` macOS resource-fork files (4 KB Finder metadata that "
            "ATEM scatters around) into a `<parent>/ponto tracinho/` "
            "subfolder, then schedule deletion of that subfolder. Best-"
            "effort: failures are logged but never abort the pipeline. "
            "Scoped to the ATEM 'Video ISO Files' folder by "
            "dot_underscore_target_folder_name and to small files only "
            "via dot_underscore_max_size_bytes — we don't touch ._ files "
            "elsewhere."
        ),
    )
    cleanup_dot_underscore_delete_after_seconds: int = Field(
        default=300,
        ge=0,
        description=(
            "Delay before the `ponto tracinho` quarantine folder is "
            "cleaned (folder kept, files inside deleted). 0 = keep "
            "forever. Default 300 = 5 minutes."
        ),
    )
    cleanup_dot_underscore_sweep_every_n_scans: int = Field(
        default=1,
        ge=0,
        description=(
            "Run a full recursive sweep across dropbox_root every N scans "
            "to catch ._ files that arrived AFTER a per-folder reorganize "
            "batch already ran. 0 disables the periodic sweep entirely "
            "(only the per-batch hook + manual /api/cleanup-dotunderscore-"
            "now endpoint will clean). Default 1 = sweep every scan."
        ),
    )
    dot_underscore_target_folder_names: list[str] = Field(
        default_factory=lambda: ["Video ISO Files", "Audio Source Files"],
        description=(
            "Folder names (case-insensitive) the ._ cleanup is scoped to. "
            "ATEM hardware writes resource forks specifically here; we "
            "intentionally never touch ._ files in other folders."
        ),
    )
    dot_underscore_max_size_bytes: int = Field(
        default=10240,
        ge=0,
        description=(
            "Upper bound on ._ file size eligible for cleanup. Real "
            "macOS resource forks are typically 4 KB; 10 KB gives "
            "headroom while still skipping anything that looks like "
            "real data accidentally prefixed with ._."
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
        'exclude_patterns': ['*/h265/*', '*/.h265/*', '*/H265/*', '*/h264/*', '*/H264/*', '*/assets/*', '*/Assets/*'],
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
