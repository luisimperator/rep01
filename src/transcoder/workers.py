"""
Worker implementations for the transcoding pipeline.

Provides Downloader, Transcoder, and Uploader workers that process jobs
from the queue and move them through states.
"""

from __future__ import annotations

import logging
import os
import shutil
import signal
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from queue import Empty, Queue
from typing import TYPE_CHECKING, Callable

from .database import Database, Job, JobState
from .dropbox_client import DropboxClient, DropboxRevChangedError
from .encoder_detect import EncoderType, select_best_encoder
from .ffmpeg_builder import FFmpegCommand, FFmpegCommandBuilder
from .prober import ProbeError, ProbeResult, probe_video, validate_output
from .scanner import Scanner
from .utils import format_bytes, format_duration, get_staging_paths, parse_ffmpeg_progress

if TYPE_CHECKING:
    from .config import Config

logger = logging.getLogger(__name__)


class WorkerStop(Exception):
    """Signal to stop worker."""
    pass


class BaseWorker(threading.Thread):
    """Base class for pipeline workers."""

    def __init__(
        self,
        name: str,
        config: Config,
        db: Database,
        stop_event: threading.Event,
    ):
        super().__init__(name=name, daemon=True)
        self.config = config
        self.db = db
        self.stop_event = stop_event
        self._current_job: Job | None = None

    def run(self) -> None:
        """Main worker loop."""
        logger.info(f"Worker {self.name} started")

        while not self.stop_event.is_set():
            try:
                self.process_next()
            except WorkerStop:
                break
            except Exception as e:
                logger.exception(f"Worker {self.name} error: {e}")
                time.sleep(5)  # Brief pause on error

        logger.info(f"Worker {self.name} stopped")

    def process_next(self) -> None:
        """Process next job. Override in subclasses."""
        raise NotImplementedError

    def should_stop(self) -> bool:
        """Check if worker should stop."""
        return self.stop_event.is_set()


class DownloadWorker(BaseWorker):
    """Worker that downloads files from Dropbox."""

    def __init__(
        self,
        worker_id: int,
        config: Config,
        db: Database,
        dropbox: DropboxClient,
        scanner: Scanner,
        stop_event: threading.Event,
    ):
        super().__init__(f"downloader-{worker_id}", config, db, stop_event)
        self.dropbox = dropbox
        self.scanner = scanner

    def process_next(self) -> None:
        """Get next NEW job and download it."""
        # Get job
        jobs = self.db.get_jobs_by_state(JobState.NEW, limit=1)
        if not jobs:
            # Also check RETRY_WAIT jobs
            jobs = self.db.get_jobs_by_state(JobState.RETRY_WAIT, limit=1)

        if not jobs:
            time.sleep(5)
            return

        job = jobs[0]
        self._current_job = job

        logger.info(f"[{self.name}] Downloading: {job.dropbox_path}")

        try:
            self._download_job(job)
        except DropboxRevChangedError as e:
            logger.warning(f"[{self.name}] Rev changed during download: {e}")
            self.db.update_job_state(
                job.id,
                JobState.STABLE_WAIT,
                error_message=str(e),
            )
        except Exception as e:
            logger.error(f"[{self.name}] Download failed: {e}")
            self._handle_failure(job, str(e))
        finally:
            self._current_job = None

    def _download_job(self, job: Job) -> None:
        """Download file for job."""
        # Update state
        self.db.update_job_state(job.id, JobState.DOWNLOADING)

        # Setup staging paths
        original_name = Path(job.dropbox_path).name
        job_dir, input_path, _ = get_staging_paths(
            self.config.local_staging_dir,
            job.id,
            original_name,
        )

        # Create job directory
        job_dir.mkdir(parents=True, exist_ok=True)

        # Partial file path during download
        partial_path = input_path.with_suffix(input_path.suffix + '.partial')

        try:
            # Download with rev check
            self.dropbox.download_file_with_rev_check(
                job.dropbox_path,
                partial_path,
                expected_rev=job.dropbox_rev,
                progress_callback=self._make_progress_callback(job),
            )

            # Final rev check before committing
            if not self.scanner.verify_job_rev(job.id, job.dropbox_path, job.dropbox_rev):
                # Job was reset to STABLE_WAIT
                if partial_path.exists():
                    partial_path.unlink()
                return

            # Rename to final path
            partial_path.rename(input_path)

            # Update state
            self.db.update_job_state(
                job.id,
                JobState.DOWNLOADED,
                local_input_path=str(input_path),
            )

            logger.info(f"[{self.name}] Download complete: {job.dropbox_path}")

        except Exception:
            # Clean up on failure
            if partial_path.exists():
                partial_path.unlink()
            raise

    def _make_progress_callback(
        self,
        job: Job,
    ) -> Callable[[int, int], None]:
        """Create progress callback for download."""
        last_log = [0.0]

        def callback(downloaded: int, total: int) -> None:
            if self.should_stop():
                raise WorkerStop("Worker stopping")

            now = time.time()
            if now - last_log[0] > 30:  # Log every 30s
                pct = (downloaded / total * 100) if total else 0
                logger.info(
                    f"[{self.name}] Download progress: {pct:.1f}% "
                    f"({format_bytes(downloaded)}/{format_bytes(total)})"
                )
                last_log[0] = now

        return callback

    def _handle_failure(self, job: Job, error: str) -> None:
        """Handle job failure with retry logic."""
        retry_count, should_fail = self.db.increment_retry(
            job.id,
            self.config.watchdog.max_retries,
        )

        if should_fail:
            self.db.update_job_state(
                job.id,
                JobState.FAILED,
                error_message=f"Max retries exceeded: {error}",
            )
        else:
            # Exponential backoff delay
            delay = min(300, 5 * (2 ** retry_count))
            self.db.update_job_state(
                job.id,
                JobState.RETRY_WAIT,
                error_message=f"Retry {retry_count}: {error}",
            )
            logger.info(f"[{self.name}] Will retry job {job.id} in {delay}s")


class TranscodeWorker(BaseWorker):
    """Worker that transcodes downloaded videos."""

    def __init__(
        self,
        worker_id: int,
        config: Config,
        db: Database,
        stop_event: threading.Event,
        encoder: EncoderType | None = None,
    ):
        super().__init__(f"transcoder-{worker_id}", config, db, stop_event)
        self.encoder = encoder
        self.command_builder = FFmpegCommandBuilder(config)
        self._ffmpeg_process: subprocess.Popen | None = None

    def process_next(self) -> None:
        """Get next DOWNLOADED job and transcode it."""
        jobs = self.db.get_jobs_by_state(JobState.DOWNLOADED, limit=1)
        if not jobs:
            time.sleep(5)
            return

        job = jobs[0]
        self._current_job = job

        logger.info(f"[{self.name}] Transcoding: {job.dropbox_path}")

        try:
            self._transcode_job(job)
        except Exception as e:
            logger.error(f"[{self.name}] Transcode failed: {e}")
            self._handle_failure(job, str(e))
        finally:
            self._current_job = None
            self._ffmpeg_process = None

    def _transcode_job(self, job: Job) -> None:
        """Transcode video file."""
        if not job.local_input_path:
            raise ValueError("Job has no local input path")

        input_path = Path(job.local_input_path)
        if not input_path.exists():
            raise ValueError(f"Input file not found: {input_path}")

        # Probe input
        self.db.update_job_state(job.id, JobState.PROBING)
        try:
            probe_result = probe_video(input_path, self.config.ffprobe_path)
        except ProbeError as e:
            raise ValueError(f"Probe failed: {e}")

        # R1: Skip if already HEVC
        if probe_result.is_hevc:
            logger.info(f"[{self.name}] Skipping (already HEVC): {job.dropbox_path}")
            self.db.update_job_state(
                job.id,
                JobState.SKIPPED_HEVC,
                input_codec=probe_result.video_info.codec_name,
            )
            # Clean up staging
            self._cleanup_staging(job)
            return

        # Select encoder
        encoder = self.encoder or select_best_encoder(self.config, verify=False)

        # Setup output path
        job_dir = input_path.parent
        output_path = job_dir / f"output{input_path.suffix}"

        # Build command
        cmd = self.command_builder.build_transcode_command(
            input_path,
            output_path,
            probe_result.video_info,
            encoder,
        )

        logger.info(f"[{self.name}] {cmd.description}")
        logger.debug(f"[{self.name}] Command: {cmd.as_string()}")

        # Update state
        self.db.update_job_state(
            job.id,
            JobState.TRANSCODING,
            input_codec=probe_result.video_info.codec_name,
            input_duration_sec=probe_result.video_info.duration_sec,
            input_bitrate_kbps=probe_result.video_info.bitrate_kbps,
            transcode_start=datetime.now(timezone.utc),
            encoder_used=encoder.value,
        )

        # Run FFmpeg
        success = self._run_ffmpeg(cmd, job)

        if not success:
            # Try with audio re-encode fallback
            logger.warning(f"[{self.name}] Retrying with audio re-encode")
            cmd = self.command_builder.build_audio_fallback_command(
                input_path,
                output_path,
                probe_result.video_info,
                encoder,
            )
            success = self._run_ffmpeg(cmd, job)

        if not success:
            raise ValueError("FFmpeg transcode failed")

        # Rename temp to final
        if cmd.temp_output_path.exists():
            cmd.temp_output_path.rename(output_path)

        # Validate output
        is_valid, error = validate_output(
            output_path,
            probe_result.video_info.duration_sec,
            self.config.ffprobe_path,
        )

        if not is_valid:
            if output_path.exists():
                output_path.unlink()
            raise ValueError(f"Output validation failed: {error}")

        # Probe output for stats
        try:
            output_probe = probe_video(output_path, self.config.ffprobe_path)
            output_bitrate = output_probe.video_info.bitrate_kbps
            output_duration = output_probe.video_info.duration_sec
        except ProbeError:
            output_bitrate = 0
            output_duration = 0

        # Update job
        self.db.update_job_state(
            job.id,
            JobState.UPLOADING,  # Ready for upload
            local_output_path=str(output_path),
            output_codec="hevc",
            output_duration_sec=output_duration,
            output_bitrate_kbps=output_bitrate,
            transcode_end=datetime.now(timezone.utc),
        )

        logger.info(
            f"[{self.name}] Transcode complete: {job.dropbox_path} "
            f"({format_bytes(input_path.stat().st_size)} -> "
            f"{format_bytes(output_path.stat().st_size)})"
        )

    def _run_ffmpeg(self, cmd: FFmpegCommand, job: Job) -> bool:
        """Run FFmpeg command with progress tracking."""
        log_dir = self.config.log_dir / f"job_{job.id}"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "ffmpeg.log"

        try:
            with open(log_file, 'w') as log_f:
                self._ffmpeg_process = subprocess.Popen(
                    cmd.args,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )

                # Monitor stderr for progress
                last_progress = time.time()
                while True:
                    if self.should_stop():
                        self._kill_ffmpeg()
                        raise WorkerStop("Worker stopping")

                    line = self._ffmpeg_process.stderr.readline()
                    if not line:
                        break

                    log_f.write(line)

                    # Parse progress
                    progress = parse_ffmpeg_progress(line)
                    if progress and 'time_sec' in progress:
                        now = time.time()
                        if now - last_progress > 30:  # Log every 30s
                            pct = (progress['time_sec'] / cmd.expected_duration_sec * 100
                                   if cmd.expected_duration_sec else 0)
                            speed = progress.get('speed', 0)
                            logger.info(
                                f"[{self.name}] Progress: {pct:.1f}% "
                                f"({format_duration(progress['time_sec'])}/"
                                f"{format_duration(cmd.expected_duration_sec)}) "
                                f"speed={speed:.2f}x"
                            )
                            last_progress = now

                self._ffmpeg_process.wait()
                return_code = self._ffmpeg_process.returncode

                if return_code != 0:
                    logger.error(f"[{self.name}] FFmpeg failed with code {return_code}")
                    return False

                return True

        except Exception as e:
            logger.error(f"[{self.name}] FFmpeg error: {e}")
            self._kill_ffmpeg()
            return False

    def _kill_ffmpeg(self) -> None:
        """Kill running FFmpeg process."""
        if self._ffmpeg_process:
            try:
                self._ffmpeg_process.terminate()
                try:
                    self._ffmpeg_process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self._ffmpeg_process.kill()
            except Exception as e:
                logger.warning(f"Error killing FFmpeg: {e}")

    def _cleanup_staging(self, job: Job) -> None:
        """Clean up staging directory for job."""
        if job.local_input_path:
            job_dir = Path(job.local_input_path).parent
            if job_dir.exists() and job_dir.name.startswith('job_'):
                try:
                    shutil.rmtree(job_dir)
                except Exception as e:
                    logger.warning(f"Failed to clean staging: {e}")

    def _handle_failure(self, job: Job, error: str) -> None:
        """Handle job failure with retry logic."""
        retry_count, should_fail = self.db.increment_retry(
            job.id,
            self.config.watchdog.max_retries,
        )

        if should_fail:
            self.db.update_job_state(
                job.id,
                JobState.FAILED,
                error_message=f"Max retries exceeded: {error}",
            )
        else:
            self.db.update_job_state(
                job.id,
                JobState.RETRY_WAIT,
                error_message=f"Retry {retry_count}: {error}",
            )


class UploadWorker(BaseWorker):
    """Worker that uploads transcoded files to Dropbox."""

    def __init__(
        self,
        worker_id: int,
        config: Config,
        db: Database,
        dropbox: DropboxClient,
        stop_event: threading.Event,
    ):
        super().__init__(f"uploader-{worker_id}", config, db, stop_event)
        self.dropbox = dropbox

    def process_next(self) -> None:
        """Get next job ready for upload."""
        # Jobs with UPLOADING state have completed transcode
        jobs = self.db.get_jobs_by_state(JobState.UPLOADING, limit=1)
        if not jobs:
            time.sleep(5)
            return

        job = jobs[0]
        self._current_job = job

        logger.info(f"[{self.name}] Uploading: {job.output_path}")

        try:
            self._upload_job(job)
        except Exception as e:
            logger.error(f"[{self.name}] Upload failed: {e}")
            self._handle_failure(job, str(e))
        finally:
            self._current_job = None

    def _upload_job(self, job: Job) -> None:
        """Upload transcoded file to Dropbox."""
        if not job.local_output_path:
            raise ValueError("Job has no local output path")

        output_path = Path(job.local_output_path)
        if not output_path.exists():
            raise ValueError(f"Output file not found: {output_path}")

        if not self.config.upload_to_dropbox:
            logger.info(f"[{self.name}] Upload disabled, marking done: {job.output_path}")
            self.db.update_job_state(job.id, JobState.DONE)
            return

        # Create h265 folder if needed
        output_dir = str(Path(job.output_path).parent)
        self.dropbox.create_folder(output_dir)

        # Upload
        self.dropbox.upload_file(
            output_path,
            job.output_path,
            overwrite=True,
            progress_callback=self._make_progress_callback(job, output_path.stat().st_size),
        )

        # Mark done
        self.db.update_job_state(job.id, JobState.DONE)

        logger.info(f"[{self.name}] Upload complete: {job.output_path}")

        # Clean up staging if configured
        if self.config.delete_staging_after_upload:
            self._cleanup_staging(job)

    def _make_progress_callback(
        self,
        job: Job,
        total_size: int,
    ) -> Callable[[int, int], None]:
        """Create progress callback for upload."""
        last_log = [0.0]

        def callback(uploaded: int, total: int) -> None:
            if self.should_stop():
                raise WorkerStop("Worker stopping")

            now = time.time()
            if now - last_log[0] > 30:
                pct = (uploaded / total * 100) if total else 0
                logger.info(
                    f"[{self.name}] Upload progress: {pct:.1f}% "
                    f"({format_bytes(uploaded)}/{format_bytes(total)})"
                )
                last_log[0] = now

        return callback

    def _cleanup_staging(self, job: Job) -> None:
        """Clean up staging directory."""
        if job.local_input_path:
            job_dir = Path(job.local_input_path).parent
            if job_dir.exists() and job_dir.name.startswith('job_'):
                try:
                    shutil.rmtree(job_dir)
                    logger.debug(f"[{self.name}] Cleaned staging: {job_dir}")
                except Exception as e:
                    logger.warning(f"Failed to clean staging: {e}")

    def _handle_failure(self, job: Job, error: str) -> None:
        """Handle upload failure."""
        retry_count, should_fail = self.db.increment_retry(
            job.id,
            self.config.watchdog.max_retries,
        )

        if should_fail:
            self.db.update_job_state(
                job.id,
                JobState.FAILED,
                error_message=f"Upload failed after retries: {error}",
            )
        else:
            # Stay in UPLOADING state for retry
            self.db.update_job_state(
                job.id,
                JobState.UPLOADING,  # Keep state, will be retried
                error_message=f"Upload retry {retry_count}: {error}",
            )
