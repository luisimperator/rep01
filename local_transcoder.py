#!/usr/bin/env python3
"""
Local Dropbox Folder Transcoder - Simple version

Monitors a local Dropbox folder, transcodes H.264 to H.265,
and saves to h265/ subfolder. Dropbox syncs automatically.
"""

import subprocess
import sys
import time
import json
import sqlite3
from pathlib import Path
from datetime import datetime

# ============ CONFIGURAÇÃO ============
WATCH_FOLDER = Path(r"D:\HeavyDrops Dropbox\HeavyDrops\App h265 Converter")
MIN_SIZE_GB = 0  # 0 = processar qualquer tamanho
VIDEO_EXTENSIONS = {'.mp4', '.mov', '.MP4', '.MOV'}
SCAN_INTERVAL_SEC = 30
DB_PATH = Path(r"C:\transcoder\local_transcoder.db")
LOG_PATH = Path(r"C:\transcoder\logs")

# Encoder: 'nvenc', 'qsv', 'cpu'
ENCODER = 'cpu'  # cpu funciona sempre
CQ_VALUE = 24
# ======================================


def setup_database():
    """Create database if not exists."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOG_PATH.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS processed (
            id INTEGER PRIMARY KEY,
            input_path TEXT UNIQUE,
            output_path TEXT,
            status TEXT,
            input_size INTEGER,
            output_size INTEGER,
            duration_sec REAL,
            processed_at TEXT,
            error TEXT
        )
    """)
    conn.commit()
    return conn


def is_processed(conn, path: Path) -> bool:
    """Check if file was already processed."""
    cursor = conn.execute(
        "SELECT status FROM processed WHERE input_path = ?",
        (str(path),)
    )
    row = cursor.fetchone()
    return row is not None and row[0] in ('done', 'skipped_hevc')


def mark_processed(conn, input_path: Path, output_path: Path, status: str,
                   input_size: int = 0, output_size: int = 0,
                   duration: float = 0, error: str = None):
    """Mark file as processed."""
    conn.execute("""
        INSERT OR REPLACE INTO processed
        (input_path, output_path, status, input_size, output_size, duration_sec, processed_at, error)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (str(input_path), str(output_path), status, input_size, output_size,
          duration, datetime.now().isoformat(), error))
    conn.commit()


def probe_video(path: Path) -> dict:
    """Get video info using ffprobe."""
    cmd = [
        'ffprobe', '-v', 'quiet', '-print_format', 'json',
        '-show_format', '-show_streams', str(path)
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return None
    return json.loads(result.stdout)


def is_hevc(probe_data: dict) -> bool:
    """Check if video is already HEVC."""
    for stream in probe_data.get('streams', []):
        if stream.get('codec_type') == 'video':
            codec = stream.get('codec_name', '').lower()
            return codec in ('hevc', 'h265')
    return False


def get_duration(probe_data: dict) -> float:
    """Get video duration in seconds."""
    try:
        return float(probe_data.get('format', {}).get('duration', 0))
    except:
        return 0


def build_ffmpeg_command(input_path: Path, output_path: Path) -> list:
    """Build FFmpeg transcode command."""

    if ENCODER == 'nvenc':
        return [
            'ffmpeg', '-hide_banner', '-y',
            '-i', str(input_path),
            '-map', '0',
            '-map_metadata', '0',
            '-c:v', 'hevc_nvenc',
            '-preset', 'p5',
            '-rc:v', 'vbr',
            '-cq:v', str(CQ_VALUE),
            '-c:a', 'copy',
            '-c:s', 'copy',
            str(output_path)
        ]
    elif ENCODER == 'qsv':
        return [
            'ffmpeg', '-hide_banner', '-y',
            '-hwaccel', 'qsv',
            '-i', str(input_path),
            '-map', '0',
            '-map_metadata', '0',
            '-c:v', 'hevc_qsv',
            '-preset', 'medium',
            '-global_quality:v', str(CQ_VALUE),
            '-c:a', 'copy',
            '-c:s', 'copy',
            str(output_path)
        ]
    else:  # cpu
        return [
            'ffmpeg', '-hide_banner', '-y',
            '-i', str(input_path),
            '-map', '0',
            '-map_metadata', '0',
            '-c:v', 'libx265',
            '-preset', 'medium',
            '-crf', str(CQ_VALUE),
            '-c:a', 'copy',
            '-c:s', 'copy',
            str(output_path)
        ]


def transcode_file(input_path: Path, output_path: Path) -> tuple[bool, str]:
    """Transcode a single file. Returns (success, error_message)."""

    # Create output directory
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Temp output
    temp_path = output_path.with_suffix(output_path.suffix + '.tmp')

    cmd = build_ffmpeg_command(input_path, temp_path)

    print(f"    Command: {' '.join(cmd[:6])}...")

    # Run FFmpeg
    log_file = LOG_PATH / f"{input_path.stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    with open(log_file, 'w') as log:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )

        for line in process.stdout:
            log.write(line)
            # Print progress
            if 'time=' in line and 'speed=' in line:
                # Extract time and speed
                parts = line.strip()
                print(f"\r    Progress: {parts[-60:]}", end='', flush=True)

        process.wait()
        print()  # New line after progress

        if process.returncode != 0:
            return False, f"FFmpeg failed with code {process.returncode}"

    # Rename temp to final
    if temp_path.exists():
        temp_path.rename(output_path)
        return True, None

    return False, "Output file not created"


def file_is_stable(path: Path, wait_sec: int = 5) -> bool:
    """Check if file is not being written to."""
    try:
        size1 = path.stat().st_size
        time.sleep(wait_sec)
        size2 = path.stat().st_size
        return size1 == size2 and size1 > 0
    except:
        return False


def scan_and_process(conn):
    """Scan folder and process new files."""

    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Scanning {WATCH_FOLDER}...")

    if not WATCH_FOLDER.exists():
        print(f"  ERROR: Folder not found: {WATCH_FOLDER}")
        return

    # Find all video files (excluding h265 folder)
    video_files = []
    for ext in VIDEO_EXTENSIONS:
        for f in WATCH_FOLDER.rglob(f'*{ext}'):
            # Skip h265 folder
            if 'h265' in str(f).lower():
                continue
            video_files.append(f)

    print(f"  Found {len(video_files)} video files")

    for video_path in video_files:
        # Skip if already processed
        if is_processed(conn, video_path):
            continue

        # Check minimum size
        size_gb = video_path.stat().st_size / (1024**3)
        if size_gb < MIN_SIZE_GB:
            print(f"  SKIP (too small: {size_gb:.2f} GB): {video_path.name}")
            mark_processed(conn, video_path, "", "skipped_small",
                          input_size=video_path.stat().st_size)
            continue

        print(f"\n  Processing: {video_path.name} ({size_gb:.2f} GB)")

        # Check if file is stable (not being synced)
        print(f"    Checking stability...")
        if not file_is_stable(video_path):
            print(f"    File still syncing, will retry later")
            continue

        # Probe video
        print(f"    Probing video...")
        probe_data = probe_video(video_path)
        if not probe_data:
            print(f"    ERROR: Could not probe video")
            mark_processed(conn, video_path, "", "error", error="Probe failed")
            continue

        # Check if already HEVC
        if is_hevc(probe_data):
            print(f"    SKIP: Already HEVC")
            mark_processed(conn, video_path, "", "skipped_hevc",
                          input_size=video_path.stat().st_size)
            continue

        # Build output path: same folder + h265/ + same filename
        output_folder = video_path.parent / 'h265'
        output_path = output_folder / video_path.name

        # Check if output already exists
        if output_path.exists():
            print(f"    SKIP: Output already exists")
            mark_processed(conn, video_path, output_path, "skipped_exists",
                          input_size=video_path.stat().st_size,
                          output_size=output_path.stat().st_size)
            continue

        # Transcode!
        print(f"    Transcoding with {ENCODER}...")
        start_time = time.time()

        success, error = transcode_file(video_path, output_path)

        duration = time.time() - start_time

        if success:
            input_size = video_path.stat().st_size
            output_size = output_path.stat().st_size
            reduction = (1 - output_size/input_size) * 100

            print(f"    DONE! {input_size/(1024**3):.2f} GB → {output_size/(1024**3):.2f} GB ({reduction:.1f}% smaller)")
            print(f"    Time: {duration/60:.1f} minutes")

            mark_processed(conn, video_path, output_path, "done",
                          input_size=input_size, output_size=output_size,
                          duration=duration)
        else:
            print(f"    FAILED: {error}")
            mark_processed(conn, video_path, "", "error", error=error)


def main():
    print("=" * 60)
    print("  Local Dropbox Transcoder (H.264 → H.265)")
    print("=" * 60)
    print(f"  Watch folder: {WATCH_FOLDER}")
    print(f"  Encoder: {ENCODER}")
    print(f"  Min size: {MIN_SIZE_GB} GB")
    print(f"  Scan interval: {SCAN_INTERVAL_SEC} sec")
    print("=" * 60)
    print("  Press Ctrl+C to stop")
    print("=" * 60)

    conn = setup_database()

    try:
        while True:
            scan_and_process(conn)
            print(f"\n  Waiting {SCAN_INTERVAL_SEC} seconds before next scan...")
            time.sleep(SCAN_INTERVAL_SEC)
    except KeyboardInterrupt:
        print("\n\nStopping...")
    finally:
        conn.close()


if __name__ == '__main__':
    main()
