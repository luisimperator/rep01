#!/usr/bin/env python3
"""
HeavyDrops Transcoder v3 — minimal, ruthless, reliable.

One file. One loop. H.264 in, H.265 out, original deleted.
Designed to chew through 230TB on Dropbox unattended for months.
"""

import hashlib
import json
import logging
import os
import re
import shutil
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path, PurePosixPath

# Auto-install dependencies if missing
def _ensure_deps():
    missing = []
    for pkg in ["dropbox", "yaml"]:
        try:
            __import__(pkg)
        except ImportError:
            missing.append("pyyaml" if pkg == "yaml" else pkg)
    if missing:
        print(f"Installing missing dependencies: {', '.join(missing)}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install"] + missing)
        print("Done. Continuing...\n")

_ensure_deps()

import dropbox
from dropbox.exceptions import ApiError, AuthError
from dropbox.files import FileMetadata, WriteMode

# ---------------------------------------------------------------------------
# Config — edit these or use config.yaml
# ---------------------------------------------------------------------------
DEFAULT_CONFIG = {
    "dropbox_token": "",              # or set DROPBOX_TOKEN env var
    "dropbox_root": "/Videos",        # root folder to scan
    "staging_dir": "/tmp/transcode",  # local temp dir (on your SSD)
    "min_size_gb": 0,                 # minimum file size to process (0 = all)
    "encoder": "auto",                # auto / hevc_qsv / hevc_nvenc / libx265
    "cq": 24,                         # quality (lower = better, 20-28 typical)
    "queue_size": 100,                # files to queue per scan
    "scan_interval": 300,             # seconds between rescans
    "extensions": [".mp4", ".mov"],   # video extensions to process
    "done_file": "done.json",         # tracks completed files
    "delete_original": True,          # delete h264 original after success
}

log = logging.getLogger("transcode")


# ---------------------------------------------------------------------------
# Done tracker — simple JSON set of processed paths
# ---------------------------------------------------------------------------
class DoneTracker:
    def __init__(self, path: str):
        self.path = Path(path)
        self.done: set[str] = set()
        self._load()

    def _load(self):
        if self.path.exists():
            try:
                self.done = set(json.loads(self.path.read_text()))
                log.info(f"Loaded {len(self.done)} done files")
            except Exception:
                self.done = set()

    def save(self):
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(sorted(self.done)))
        tmp.replace(self.path)

    def is_done(self, path: str) -> bool:
        return path.lower() in self.done

    def mark_done(self, path: str):
        self.done.add(path.lower())
        self.save()


# ---------------------------------------------------------------------------
# Encoder detection
# ---------------------------------------------------------------------------
def detect_encoder(preference: str, ffmpeg: str = "ffmpeg") -> str:
    """Pick the best available HEVC encoder."""
    if preference != "auto":
        return preference

    try:
        r = subprocess.run(
            [ffmpeg, "-hide_banner", "-encoders"],
            capture_output=True, text=True, timeout=30
        )
        out = r.stdout + r.stderr
    except Exception:
        return "libx265"

    # Priority: QSV > NVENC > CPU
    for enc in ["hevc_qsv", "hevc_nvenc", "libx265"]:
        if enc in out:
            # Verify it actually works
            if enc == "libx265" or _test_encoder(enc, ffmpeg):
                return enc

    return "libx265"


def _test_encoder(enc: str, ffmpeg: str) -> bool:
    try:
        cmd = [ffmpeg, "-hide_banner", "-f", "lavfi",
               "-i", "testsrc=duration=1:size=64x64:rate=1",
               "-c:v", enc, "-frames:v", "1", "-f", "null", "-"]
        r = subprocess.run(cmd, capture_output=True, timeout=30)
        return r.returncode == 0
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Probe — is it h264? how long is it?
# ---------------------------------------------------------------------------
def probe(filepath: Path) -> dict | None:
    """Return {codec, duration, bit_depth, has_audio, has_subs} or None on error."""
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json",
             "-show_format", "-show_streams", str(filepath)],
            capture_output=True, text=True, timeout=120
        )
        if r.returncode != 0:
            return None
        data = json.loads(r.stdout)
    except Exception:
        return None

    vstream = None
    has_audio = False
    has_subs = False
    for s in data.get("streams", []):
        if s.get("codec_type") == "video" and not vstream:
            vstream = s
        elif s.get("codec_type") == "audio":
            has_audio = True
        elif s.get("codec_type") == "subtitle":
            has_subs = True

    if not vstream:
        return None

    codec = vstream.get("codec_name", "unknown").lower()
    dur_str = vstream.get("duration") or data.get("format", {}).get("duration", "0")
    try:
        duration = float(dur_str)
    except (ValueError, TypeError):
        duration = 0.0

    pix_fmt = vstream.get("pix_fmt", "")
    bits = vstream.get("bits_per_raw_sample", "")
    bit_depth = 10 if ("10" in pix_fmt or bits == "10") else 8

    return {
        "codec": codec,
        "duration": duration,
        "bit_depth": bit_depth,
        "has_audio": has_audio,
        "has_subs": has_subs,
    }


# ---------------------------------------------------------------------------
# Transcode — one ffmpeg call, metadata preserved
# ---------------------------------------------------------------------------
def build_ffmpeg_cmd(
    input_path: Path, output_path: Path,
    encoder: str, cq: int, info: dict
) -> list[str]:
    """Build the ffmpeg command."""
    cmd = ["ffmpeg", "-hide_banner", "-y"]

    # HW accel input
    if encoder == "hevc_qsv":
        cmd += ["-hwaccel", "qsv", "-hwaccel_output_format", "qsv"]
    elif encoder == "hevc_nvenc":
        cmd += ["-hwaccel", "cuda", "-hwaccel_output_format", "cuda"]

    cmd += ["-i", str(input_path)]
    cmd += ["-map", "0", "-map_metadata", "0"]

    # Video encoder
    use_10bit = info["bit_depth"] >= 10

    if encoder == "hevc_qsv":
        cmd += ["-c:v", "hevc_qsv",
                "-profile:v", "main10" if use_10bit else "main",
                "-preset", "medium",
                "-global_quality:v", str(cq), "-look_ahead", "1"]
    elif encoder == "hevc_nvenc":
        cmd += ["-c:v", "hevc_nvenc",
                "-profile:v", "main10" if use_10bit else "main",
                "-preset", "p5", "-tune", "hq",
                "-rc:v", "vbr", "-cq:v", str(cq), "-b:v", "0",
                "-bf", "4"]
    else:  # libx265
        crf = max(18, cq - 1)  # CRF ~= CQ - 1 for x265
        x265p = f"crf={crf}:aq-mode=3"
        if use_10bit:
            x265p += ":profile=main10"
            cmd += ["-pix_fmt", "yuv420p10le"]
        else:
            cmd += ["-pix_fmt", "yuv420p"]
        cmd += ["-c:v", "libx265", "-preset", "medium", "-x265-params", x265p]

    # Audio: copy
    if info["has_audio"]:
        cmd += ["-c:a", "copy"]
    else:
        cmd += ["-an"]

    # Subtitles: copy
    if info["has_subs"]:
        cmd += ["-c:s", "copy"]

    cmd.append(str(output_path))
    return cmd


def transcode(input_path: Path, output_path: Path,
              encoder: str, cq: int, info: dict) -> bool:
    """Run ffmpeg. Returns True on success. Retries with audio re-encode on failure."""
    cmd = build_ffmpeg_cmd(input_path, output_path, encoder, cq, info)
    log.info(f"  ffmpeg: {encoder}, cq={cq}")

    ok = _run_ffmpeg(cmd, info["duration"])
    if not ok and info["has_audio"]:
        log.warning("  Retrying with audio re-encode (aac 320k)...")
        output_path.unlink(missing_ok=True)
        # Replace -c:a copy with -c:a aac -b:a 320k
        cmd2 = []
        i = 0
        while i < len(cmd):
            if cmd[i] == "-c:a" and i + 1 < len(cmd) and cmd[i + 1] == "copy":
                cmd2 += ["-c:a", "aac", "-b:a", "320k"]
                i += 2
            else:
                cmd2.append(cmd[i])
                i += 1
        ok = _run_ffmpeg(cmd2, info["duration"])

    return ok


def _run_ffmpeg(cmd: list[str], expected_dur: float) -> bool:
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        last_log = time.time()
        for line in proc.stderr:
            m = re.search(r"time=(\d{2}):(\d{2}):(\d{2})\.(\d{2})", line)
            if m and time.time() - last_log > 60:
                h, mi, s, _ = map(int, m.groups())
                cur = h * 3600 + mi * 60 + s
                pct = (cur / expected_dur * 100) if expected_dur > 0 else 0
                sp = re.search(r"speed=\s*([\d.]+)x", line)
                speed = sp.group(1) if sp else "?"
                log.info(f"  progress: {pct:.0f}% speed={speed}x")
                last_log = time.time()
        proc.wait()
        return proc.returncode == 0
    except Exception as e:
        log.error(f"  ffmpeg error: {e}")
        return False


def validate_output(output_path: Path, expected_dur: float) -> bool:
    """Check output exists, is non-empty, is HEVC, and duration matches."""
    if not output_path.exists() or output_path.stat().st_size == 0:
        return False
    info = probe(output_path)
    if not info:
        return False
    if info["codec"] not in ("hevc", "h265", "hev1", "hvc1"):
        log.error(f"  Output codec is {info['codec']}, expected hevc")
        return False
    if expected_dur > 0:
        diff = abs(info["duration"] - expected_dur)
        if diff > expected_dur * 0.02:  # 2% tolerance
            log.error(f"  Duration mismatch: {info['duration']:.1f}s vs {expected_dur:.1f}s")
            return False
    return True


# ---------------------------------------------------------------------------
# Dropbox helpers
# ---------------------------------------------------------------------------
class DBX:
    """Thin Dropbox wrapper with retry."""
    CHUNK = 8 * 1024 * 1024

    def __init__(self, token: str):
        self.dbx = dropbox.Dropbox(token, timeout=300)

    def list_videos(self, root: str, extensions: list[str]):
        """Yield (path, size) for all video files under root."""
        norm = root if root != "/" else ""
        result = self.dbx.files_list_folder(norm, recursive=True)
        while True:
            for e in result.entries:
                if isinstance(e, FileMetadata):
                    ext = PurePosixPath(e.path_display or "").suffix.lower()
                    if ext in extensions:
                        yield (e.path_display, e.size)
            if not result.has_more:
                break
            result = self.dbx.files_list_folder_continue(result.cursor)

    def download(self, dbx_path: str, local_path: Path):
        """Download file from Dropbox."""
        local_path.parent.mkdir(parents=True, exist_ok=True)
        _, resp = self._retry(lambda: self.dbx.files_download(dbx_path))
        with open(local_path, "wb") as f:
            for chunk in resp.iter_content(self.CHUNK):
                if chunk:
                    f.write(chunk)

    def upload(self, local_path: Path, dbx_path: str):
        """Upload file to Dropbox (chunked for large files)."""
        size = local_path.stat().st_size
        with open(local_path, "rb") as f:
            if size <= self.CHUNK:
                self._retry(lambda: self.dbx.files_upload(
                    f.read(), dbx_path, mode=WriteMode.add))
            else:
                chunk = f.read(self.CHUNK)
                session = self._retry(
                    lambda: self.dbx.files_upload_session_start(chunk))
                offset = len(chunk)
                while offset < size:
                    chunk = f.read(self.CHUNK)
                    if offset + len(chunk) < size:
                        cursor = dropbox.files.UploadSessionCursor(
                            session.session_id, offset)
                        self._retry(lambda: self.dbx.files_upload_session_append_v2(
                            chunk, cursor))
                        offset += len(chunk)
                    else:
                        cursor = dropbox.files.UploadSessionCursor(
                            session.session_id, offset)
                        commit = dropbox.files.CommitInfo(
                            dbx_path, mode=WriteMode.add)
                        self._retry(lambda: self.dbx.files_upload_session_finish(
                            chunk, cursor, commit))
                        offset += len(chunk)

    def delete(self, dbx_path: str):
        """Delete file from Dropbox."""
        self._retry(lambda: self.dbx.files_delete_v2(dbx_path))

    def mkdir(self, dbx_path: str):
        """Create folder, ignore if exists."""
        try:
            self.dbx.files_create_folder_v2(dbx_path)
        except ApiError:
            pass

    def exists(self, dbx_path: str) -> bool:
        try:
            self.dbx.files_get_metadata(dbx_path)
            return True
        except ApiError:
            return False

    def _retry(self, fn, attempts=5):
        for i in range(attempts):
            try:
                return fn()
            except AuthError:
                raise
            except Exception as e:
                if i == attempts - 1:
                    raise
                wait = 2 ** (i + 1)
                log.warning(f"  Dropbox error (retry in {wait}s): {e}")
                time.sleep(wait)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def load_config() -> dict:
    cfg = dict(DEFAULT_CONFIG)
    # Try loading yaml config
    for p in ["config.yaml", "config.yml"]:
        if Path(p).exists():
            try:
                import yaml
                with open(p) as f:
                    user_cfg = yaml.safe_load(f) or {}
                cfg.update(user_cfg)
                log.info(f"Loaded config from {p}")
            except ImportError:
                # No yaml, try json-style
                pass
            break

    # Env var override for token
    if not cfg["dropbox_token"]:
        cfg["dropbox_token"] = os.environ.get("DROPBOX_TOKEN", "")

    if not cfg["dropbox_token"]:
        print("ERROR: Set DROPBOX_TOKEN env var or dropbox_token in config.yaml")
        sys.exit(1)

    return cfg


def is_eligible(path: str, size: int, cfg: dict) -> bool:
    """Quick checks before downloading."""
    pl = path.lower()
    # Skip h265 output folders
    if "/h265/" in pl:
        return False
    # Skip partial/temp files
    name = PurePosixPath(path).name.lower()
    if name.startswith(".") or name.startswith("~") or name.endswith((".partial", ".tmp", ".part")):
        return False
    # Skip YouTube downloads (already compressed)
    if re.search(r"\(\d+p_\d+fps_[A-Za-z0-9]+-\d+kbit_[A-Za-z0-9]+\)", name):
        return False
    # Min size
    min_bytes = int(cfg["min_size_gb"] * 1024 ** 3)
    if min_bytes > 0 and size < min_bytes:
        return False
    return True


def h265_output_path(original: str) -> str:
    """Original: /A/B/clip.MP4 -> /A/B/h265/clip.MP4"""
    p = PurePosixPath(original)
    return str(p.parent / "h265" / p.name)


def fmt_size(b: int) -> str:
    for u in ["B", "KB", "MB", "GB", "TB"]:
        if abs(b) < 1024:
            return f"{b:.1f}{u}"
        b /= 1024
    return f"{b:.1f}PB"


def process_file(dbx: DBX, path: str, size: int, encoder: str,
                 cfg: dict, done: DoneTracker) -> bool:
    """Process one file: download -> probe -> transcode -> upload -> delete original.
    Returns True on success."""
    staging = Path(cfg["staging_dir"])
    name = PurePosixPath(path).name
    work_dir = staging / hashlib.md5(path.encode()).hexdigest()[:12]
    input_file = work_dir / f"in_{name}"
    output_file = work_dir / f"out_{name}"
    out_dbx = h265_output_path(path)

    try:
        work_dir.mkdir(parents=True, exist_ok=True)

        # 1. Check if output already exists on Dropbox
        if dbx.exists(out_dbx):
            log.info(f"  Output already exists, skipping: {out_dbx}")
            done.mark_done(path)
            return True

        # 2. Download
        log.info(f"  Downloading ({fmt_size(size)})...")
        t0 = time.time()
        dbx.download(path, input_file)
        dl_time = time.time() - t0
        log.info(f"  Downloaded in {dl_time:.0f}s")

        # 3. Probe
        info = probe(input_file)
        if not info:
            log.error(f"  Probe failed, skipping")
            done.mark_done(path)  # don't retry broken files
            return False

        if info["codec"] in ("hevc", "h265", "hev1", "hvc1"):
            log.info(f"  Already HEVC, skipping")
            done.mark_done(path)
            return True

        if info["codec"] not in ("h264", "avc", "avc1"):
            log.info(f"  Codec is {info['codec']}, not h264 — skipping")
            done.mark_done(path)
            return True

        # 4. Transcode
        log.info(f"  Transcoding {info['codec']} -> hevc ({info['duration']:.0f}s video)...")
        t0 = time.time()
        ok = transcode(input_file, output_file, encoder, cfg["cq"], info)
        enc_time = time.time() - t0

        if not ok:
            log.error(f"  Transcode FAILED")
            return False

        # 5. Validate
        if not validate_output(output_file, info["duration"]):
            log.error(f"  Validation FAILED")
            return False

        in_size = input_file.stat().st_size
        out_size = output_file.stat().st_size
        ratio = (1 - out_size / in_size) * 100 if in_size > 0 else 0
        log.info(f"  Done: {fmt_size(in_size)} -> {fmt_size(out_size)} "
                 f"({ratio:.0f}% smaller) in {enc_time:.0f}s")

        # 6. Upload
        log.info(f"  Uploading to {out_dbx}...")
        out_dir = str(PurePosixPath(out_dbx).parent)
        dbx.mkdir(out_dir)
        t0 = time.time()
        dbx.upload(output_file, out_dbx)
        log.info(f"  Uploaded in {time.time() - t0:.0f}s")

        # 7. Delete original
        if cfg["delete_original"]:
            log.info(f"  Deleting original: {path}")
            dbx.delete(path)

        done.mark_done(path)
        return True

    except KeyboardInterrupt:
        raise
    except Exception as e:
        log.error(f"  ERROR: {e}")
        return False
    finally:
        # Always clean up local files
        if work_dir.exists():
            shutil.rmtree(work_dir, ignore_errors=True)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Quiet noisy libs
    logging.getLogger("dropbox").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    cfg = load_config()
    staging = Path(cfg["staging_dir"])
    staging.mkdir(parents=True, exist_ok=True)

    done = DoneTracker(cfg["done_file"])
    dbx = DBX(cfg["dropbox_token"])

    # Detect encoder once
    encoder = detect_encoder(cfg["encoder"])
    log.info(f"Encoder: {encoder}")
    log.info(f"Root: {cfg['dropbox_root']}")
    log.info(f"Staging: {cfg['staging_dir']}")
    log.info(f"Done files: {len(done.done)}")
    log.info(f"Delete originals: {cfg['delete_original']}")

    # Graceful shutdown
    stop = False
    def on_signal(sig, frame):
        nonlocal stop
        log.info("Shutting down after current file...")
        stop = True
    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    total_processed = 0
    total_saved = 0
    start_time = time.time()

    while not stop:
        # Scan
        log.info("=" * 60)
        log.info("Scanning Dropbox...")
        queue = []
        try:
            for path, size in dbx.list_videos(cfg["dropbox_root"], cfg["extensions"]):
                if done.is_done(path):
                    continue
                if not is_eligible(path, size, cfg):
                    continue
                queue.append((path, size))
                if len(queue) >= cfg["queue_size"]:
                    break
        except Exception as e:
            log.error(f"Scan error: {e}")
            time.sleep(60)
            continue

        if not queue:
            log.info(f"No files to process. Waiting {cfg['scan_interval']}s...")
            for _ in range(cfg["scan_interval"]):
                if stop:
                    break
                time.sleep(1)
            continue

        log.info(f"Queue: {len(queue)} files")

        # Process queue
        for i, (path, size) in enumerate(queue):
            if stop:
                break
            log.info(f"[{i+1}/{len(queue)}] {path}")
            ok = process_file(dbx, path, size, encoder, cfg, done)
            if ok:
                total_processed += 1

            # Quick disk space check
            usage = shutil.disk_usage(staging)
            free_gb = usage.free / (1024 ** 3)
            if free_gb < 20:
                log.warning(f"Low disk space: {free_gb:.1f}GB free. Pausing 5min...")
                time.sleep(300)

        # Stats
        elapsed_h = (time.time() - start_time) / 3600
        log.info(f"Session: {total_processed} files in {elapsed_h:.1f}h")

    log.info("Stopped.")


if __name__ == "__main__":
    main()
