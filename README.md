# HeavyDrops Transcoder v6.0.0

A robust, idempotent daemon that monitors a Dropbox folder, downloads videos, transcodes H.264 to H.265 (HEVC) using FFmpeg with hardware acceleration support, and uploads the results back to Dropbox.

Designed for processing massive video archives (hundreds of TB) with minimal intervention, running 24/7 for months.

**v6 highlights**: incremental scanner with Dropbox cursor (BULK then DELTA), central dispatcher with bounded queues (no more double-pickup), opt-in disk budget, optional mirror output layout, token-bucket rate limiter, loopback HTTP dashboard at <http://127.0.0.1:9123/>, no-admin one-command installer, GitHub release notify, `hd update`. See `NOTAS_DA_VERSAO.txt` for the full changelog.

## Quick install on Windows (one command, no admin)

Open PowerShell and paste:

```powershell
iwr https://raw.githubusercontent.com/luisimperator/rep01/main/bootstrap.ps1 -UseBasicParsing | iex
```

This installs under `%USERPROFILE%\HeavyDrops`, pulls Python 3.12 and Git via `winget` if missing, downloads FFmpeg, writes a `config.yaml` (you'll be prompted once for your Dropbox token), registers a scheduled task so the daemon auto-starts at logon and restarts on failure, and drops a **HeavyDrops** shortcut on the Desktop. The shortcut opens the dashboard at <http://127.0.0.1:9123/> in your default browser — queue status, bulk-scan progress, disk usage, and pause / scan-now / retry-failed buttons, refreshing every 3 seconds. Closing the browser does **not** stop the daemon.

To apply a new release later:

```powershell
cd $env:USERPROFILE\HeavyDrops
.\.venv\Scripts\hd.exe update
schtasks /End /TN HeavyDropsDaemon
schtasks /Run /TN HeavyDropsDaemon
```

The daemon already checks GitHub on startup and flags an update in the dashboard when one is available; applying it is still a conscious operator action.

## Architecture at a glance

- **Daemon**: a long-running process orchestrating scan → download → probe → transcode → upload. Auto-starts via Task Scheduler, survives logoff when configured to. Single instance guarded by a lockfile.
- **Scanner**: two-mode. On first run (BULK) it walks the whole Dropbox root once, checkpointing the cursor in SQLite so crashes resume. After completion (DELTA) it only processes changes.
- **Dispatcher**: one thread owns the job queues. Workers consume from bounded per-stage queues instead of polling the database — fewer duplicate pickups, better SQLite behaviour at scale.
- **Disk budget** (opt-in): caps staging bytes so a 2TB local disk pointed at a 200TB Dropbox doesn't ENOSPC. Downloaders stall gracefully and log every 5 minutes while waiting.
- **HTTP status API**: loopback-only (127.0.0.1:9123 by default). Serves the browser dashboard plus JSON endpoints (`/api/status`, `/api/jobs`, `/api/metrics`, `POST /api/pause`, `/api/resume`, `/api/scan-now`, `/api/retry-failed`).

## Features

- **Hardware Acceleration**: Automatic detection and use of Intel QuickSync (QSV), NVIDIA NVENC, or CPU fallback
- **File Stability Detection**: Ensures files are fully synced before processing (multiple checks over time)
- **Size Filtering**: Skip small files (< 6GB by default) to focus on large video files
- **HEVC Detection**: Automatically skips files already encoded as HEVC
- **Metadata Preservation**: Copies all metadata from input files (`-map_metadata 0`)
- **Idempotent Processing**: Safe to restart, won't reprocess completed files
- **Robust Error Handling**: Exponential backoff retries, job timeouts, automatic recovery
- **Quality Profiles**: Choose between quality (CQ/CRF) or balanced (bitrate) modes

## Requirements

- Python 3.11+
- FFmpeg with HEVC encoder support (libx265, and optionally hevc_qsv or hevc_nvenc)
- Dropbox account with API access token
- Sufficient local storage for staging (NVMe recommended)

## Installation

```bash
# Clone the repository
git clone https://github.com/example/dropbox-video-transcoder.git
cd dropbox-video-transcoder

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or: venv\Scripts\activate  # Windows

# Install dependencies
pip install -e .

# For development
pip install -e ".[dev]"
```

## Configuration

### 1. Create Dropbox App

1. Go to https://www.dropbox.com/developers/apps
2. Create a new app with "Full Dropbox" access
3. Generate an access token
4. Set the environment variable:

```bash
export DROPBOX_TOKEN="your_token_here"
```

### 2. Create Configuration File

```bash
# Generate example config
transcoder init-config config.yaml

# Edit the configuration
nano config.yaml
```

Key configuration options:

```yaml
# Root folder to monitor
dropbox_root: "/Videos"

# Minimum file size (skip smaller files)
min_size_gb: 6.0

# Encoder preference: auto, qsv, nvenc, cpu
encoder_preference: auto

# Quality profile: quality (CQ/CRF) or balanced (bitrate)
profile: quality

# CQ value for quality profile (lower = higher quality)
cq_value: 24
```

### 3. Verify Setup

```bash
# Run health checks
transcoder doctor
```

## Usage

### Start Daemon (Foreground)

```bash
transcoder start
```

### Run as Systemd Service

Create `/etc/systemd/system/transcoder.service`:

```ini
[Unit]
Description=Dropbox Video Transcoder
After=network.target

[Service]
Type=simple
User=transcoder
Environment=DROPBOX_TOKEN=your_token_here
WorkingDirectory=/opt/transcoder
ExecStart=/opt/transcoder/venv/bin/transcoder start -c /etc/transcoder/config.yaml
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable transcoder
sudo systemctl start transcoder
sudo journalctl -u transcoder -f
```

### CLI Commands

```bash
# Run a single scan iteration
transcoder run-once

# Scan immediately without waiting
transcoder scan-now

# Dry run scan (don't create jobs)
transcoder scan-now --dry-run

# View queue status
transcoder status

# List queued jobs
transcoder list-queue
transcoder list-queue --state FAILED

# Retry failed jobs
transcoder retry-failed

# Preview processing for a file
transcoder dry-run "/Videos/project/clip.mp4"

# Show available encoders
transcoder show-encoders

# Health checks
transcoder doctor
```

## Pipeline

1. **SCAN**: List files in Dropbox, apply filters (size, extensions, h265 folder)
2. **STABILITY**: Wait for file to be fully synced (3 consecutive checks over 15+ minutes)
3. **DOWNLOAD**: Download to local staging with revision checking
4. **PROBE**: Analyze video with ffprobe, skip if already HEVC
5. **TRANSCODE**: Convert H.264 → H.265 with metadata preservation
6. **VALIDATE**: Verify output duration matches input
7. **UPLOAD**: Upload to Dropbox `/h265/` subfolder

## Output Structure

Files are organized in h265 subfolders:

```
Input:  /Videos/Project/clip001.MP4
Output: /Videos/Project/h265/clip001.MP4
```

The original filename and extension are preserved exactly.

## Hardware Encoder Support

### Intel QuickSync (QSV)

Requires Intel CPU with integrated graphics or Intel Arc GPU.

```bash
# Check if QSV is available
vainfo
ffmpeg -hide_banner -encoders | grep hevc_qsv
```

### NVIDIA NVENC

Requires NVIDIA GPU with NVENC support.

```bash
# Check if NVENC is available
nvidia-smi
ffmpeg -hide_banner -encoders | grep hevc_nvenc
```

### CPU Fallback (libx265)

Always available if FFmpeg is compiled with libx265 support.

```bash
ffmpeg -hide_banner -encoders | grep libx265
```

## Quality Settings

### Quality Profile (Recommended)

Uses constant quality mode for best quality per bit:

- **QSV**: `global_quality=24`
- **NVENC**: `rc=vbr, cq=24`
- **CPU**: `crf=23`

### Balanced Profile

Uses target bitrate for predictable file sizes:

- Target: 40 Mbps
- Max: 60 Mbps
- Buffer: 120 Mbps

## Job States

| State | Description |
|-------|-------------|
| NEW | Job queued, waiting for download |
| STABLE_WAIT | Waiting for file to finish syncing |
| DOWNLOADING | Downloading from Dropbox |
| DOWNLOADED | Download complete, waiting for transcode |
| PROBING | Analyzing video with ffprobe |
| TRANSCODING | FFmpeg transcode in progress |
| UPLOADING | Uploading to Dropbox |
| DONE | Successfully completed |
| SKIPPED_HEVC | Skipped (already HEVC) |
| SKIPPED_ALREADY_EXISTS | Skipped (output exists) |
| SKIPPED_TOO_SMALL | Skipped (below min_size_gb) |
| FAILED | Failed after max retries |
| RETRY_WAIT | Waiting for retry (exponential backoff) |

## Monitoring

### Database

Job state is stored in SQLite:

```bash
sqlite3 /data/transcoder/transcoder.db
sqlite> SELECT state, COUNT(*) FROM jobs GROUP BY state;
sqlite> SELECT * FROM jobs WHERE state = 'FAILED';
```

### Logs

- Application logs: `/var/log/transcoder/transcoder.log`
- FFmpeg logs: `/var/log/transcoder/job_<id>/ffmpeg.log`

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run tests with coverage
pytest --cov=transcoder

# Type checking
mypy src/

# Linting
ruff check src/ tests/
```

## Testing

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_path_mapping.py

# Run with verbose output
pytest -v

# Run specific test
pytest tests/test_ffmpeg_builder.py::TestFFmpegCommandBuilder::test_metadata_preservation
```

## Troubleshooting

### "Another instance is already running"

Remove the lock file if no instance is running:

```bash
rm /var/run/transcoder.lock
```

### QSV encoder not working

1. Check Intel graphics driver is loaded: `lsmod | grep i915`
2. Check VA-API: `vainfo`
3. Verify user has access to render device: `ls -la /dev/dri/`

### NVENC encoder not working

1. Check NVIDIA driver: `nvidia-smi`
2. Verify FFmpeg NVENC support: `ffmpeg -hide_banner -encoders | grep nvenc`

### Files stuck in STABLE_WAIT

Files need 3 consecutive stability checks over 15 minutes. Check if:
- File is still being uploaded to Dropbox
- Dropbox client is running on source machine
- Network connectivity is stable

### High disk usage

- Reduce `download_workers` and `transcode_workers`
- Increase `delete_staging_after_upload: true`
- Use faster storage for staging

## License

MIT License - see LICENSE file for details.
