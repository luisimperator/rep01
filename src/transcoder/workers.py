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
from .disk_budget import DiskBudget
from .dispatcher import JobDispatcher
from .dropbox_client import DropboxClient, DropboxRevChangedError
from .encoder_detect import EncoderType, select_best_encoder
from .ffmpeg_builder import FFmpegCommand, FFmpegCommandBuilder
from .prober import ProbeError, ProbeResult, probe_video, validate_output
from .scanner import Scanner
from .progress import REGISTRY
from .utils import format_bytes, format_duration, get_staging_paths, parse_ffmpeg_progress

if TYPE_CHECKING:
    from .config import Config

logger = logging.getLogger(__name__)

# How long worker.queue.get() blocks before re-checking stop_event.
_QUEUE_GET_TIMEOUT_SEC = 5.0


class WorkerStop(Exception):
    """Signal to stop worker."""
    pass


class BaseWorker(threading.Thread):
    """Base class for pipeline workers."""

    # Subclasses set this to identify which dispatcher queue to consume from.
    stage: str = ""

    def __init__(
        self,
        name: str,
        config: Config,
        db: Database,
        stop_event: threading.Event,
        dispatcher: JobDispatcher,
    ):
        super().__init__(name=name, daemon=True)
        self.config = config
        self.db = db
        self.stop_event = stop_event
        self.dispatcher = dispatcher
        self.queue: Queue = dispatcher.queue_for_stage(self.stage)
        self._current_job: Job | None = None

    def run(self) -> None:
        """Main worker loop: pull jobs from dispatcher, process, release slot."""
        logger.info(f"Worker {self.name} started (stage={self.stage})")

        while not self.stop_event.is_set():
            try:
                job = self.queue.get(timeout=_QUEUE_GET_TIMEOUT_SEC)
            except Empty:
                continue

            self._current_job = job
            try:
                self.process_job(job)
            except WorkerStop:
                self.dispatcher.mark_done(job.id)
                break
            except Exception as e:
                logger.exception(f"Worker {self.name} error on job {job.id}: {e}")
                time.sleep(1)
            finally:
                self._current_job = None
                REGISTRY.end(self.name)
                self.dispatcher.mark_done(job.id)

        logger.info(f"Worker {self.name} stopped")

    def process_job(self, job: Job) -> None:
        """Process a single job. Override in subclasses."""
        raise NotImplementedError

    def should_stop(self) -> bool:
        """Check if worker should stop."""
        return self.stop_event.is_set()


class DownloadWorker(BaseWorker):
    """Worker that downloads files from Dropbox."""

    stage = "download"

    def __init__(
        self,
        worker_id: int,
        config: Config,
        db: Database,
        dropbox: DropboxClient,
        scanner: Scanner,
        stop_event: threading.Event,
        dispatcher: JobDispatcher,
        disk_budget: DiskBudget | None = None,
    ):
        super().__init__(f"downloader-{worker_id}", config, db, stop_event, dispatcher)
        self.dropbox = dropbox
        self.scanner = scanner
        self.disk_budget = disk_budget

    def process_job(self, job: Job) -> None:
        """Download the file for the given job."""
        logger.info(f"[{self.name}] Downloading: {job.dropbox_path}")
        REGISTRY.begin(
            self.name, "download", job.id, job.dropbox_path,
            bytes_total=int(job.dropbox_size or 0),
        )

        # Block until staging has room. On stop_event, bail cleanly and let the
        # dispatcher re-enqueue the job on next refill (state is still NEW).
        if self.disk_budget is not None and self.disk_budget.enabled:
            granted = self.disk_budget.wait_for_slot(
                job.id, job.dropbox_size, self.stop_event
            )
            if not granted:
                logger.info(f"[{self.name}] Aborting job {job.id}: shutting down")
                return

        try:
            self._download_job(job)
        except DropboxRevChangedError as e:
            logger.warning(f"[{self.name}] Rev changed during download: {e}")
            self.db.update_job_state(
                job.id,
                JobState.STABLE_WAIT,
                error_message=str(e),
            )
            if self.disk_budget is not None:
                self.disk_budget.release(job.id)
        except Exception as e:
            logger.error(f"[{self.name}] Download failed: {e}")
            self._handle_failure(job, str(e))
            if self.disk_budget is not None:
                self.disk_budget.release(job.id)

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

            REGISTRY.update(self.name, bytes_done=downloaded, bytes_total=total)

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

    stage = "transcode"

    def __init__(
        self,
        worker_id: int,
        config: Config,
        db: Database,
        stop_event: threading.Event,
        dispatcher: JobDispatcher,
        encoder: EncoderType | None = None,
        disk_budget: DiskBudget | None = None,
        incident_reporter=None,
    ):
        super().__init__(f"transcoder-{worker_id}", config, db, stop_event, dispatcher)
        self.encoder = encoder
        self.command_builder = FFmpegCommandBuilder(config)
        self._ffmpeg_process: subprocess.Popen | None = None
        self.disk_budget = disk_budget
        self.incident_reporter = incident_reporter
        self._last_ffmpeg_log: Path | None = None

    def process_job(self, job: Job) -> None:
        """Transcode the file for the given job."""
        logger.info(f"[{self.name}] Transcoding: {job.dropbox_path}")
        REGISTRY.begin(self.name, "transcode", job.id, job.dropbox_path)

        try:
            self._transcode_job(job)
        except Exception as e:
            logger.error(f"[{self.name}] Transcode failed: {e}")
            self._handle_failure(job, str(e))
        finally:
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
        # Promote the full command to INFO so empty-stderr failures are
        # actionable: if the command is malformed in some Windows-specific
        # way the args themselves are the diagnostic.
        logger.info(f"[{self.name}] ffmpeg cmd: {cmd.as_string()}")

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

        if not success and encoder != EncoderType.CPU:
            # Hardware encoder couldn't handle this file (driver missing,
            # unsupported pixel format, decoder mismatch, etc). Fall back to
            # libx265 so the job actually completes — slower but reliable.
            logger.warning(
                f"[{self.name}] {encoder.value} failed twice; "
                f"falling back to libx265 (CPU)."
            )
            encoder = EncoderType.CPU
            self.db.update_job_state(job.id, JobState.TRANSCODING, encoder_used=encoder.value)
            cmd = self.command_builder.build_transcode_command(
                input_path,
                output_path,
                probe_result.video_info,
                encoder,
            )
            logger.info(f"[{self.name}] {cmd.description}")
            success = self._run_ffmpeg(cmd, job)
            if not success:
                logger.warning(f"[{self.name}] CPU retry with audio re-encode")
                cmd = self.command_builder.build_audio_fallback_command(
                    input_path,
                    output_path,
                    probe_result.video_info,
                    encoder,
                )
                success = self._run_ffmpeg(cmd, job)

        if not success:
            # File an auto-incident before raising so the operator (and any AI
            # tailing the repo) sees the actual ffmpeg stderr without having
            # to copy logs by hand.
            if self.incident_reporter is not None:
                tail = ""
                if self._last_ffmpeg_log:
                    tail = self._tail_ffmpeg_log(self._last_ffmpeg_log, lines=30)
                self.incident_reporter.report(
                    kind="transcode-fail",
                    summary=f"{encoder.value} failed on {Path(job.dropbox_path).name} "
                            f"({probe_result.video_info.codec_name} "
                            f"{probe_result.video_info.width}x{probe_result.video_info.height})",
                    log_tail=tail,
                    context={
                        "job_id": job.id,
                        "dropbox_path": job.dropbox_path,
                        "input_size": f"{(job.dropbox_size or 0)/(1024**3):.2f} GB",
                        "input_codec": probe_result.video_info.codec_name,
                        "input_resolution": f"{probe_result.video_info.width}x{probe_result.video_info.height}",
                        "input_duration_sec": probe_result.video_info.duration_sec,
                        "encoder_attempted": encoder.value,
                        "worker": self.name,
                    },
                )
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
        self._last_ffmpeg_log = log_file

        try:
            # On Windows the console code page (cp1252) will choke on any
            # non-ASCII char in a filename; force UTF-8 end-to-end and
            # tolerate the rare invalid byte so the transcode still proceeds.
            #
            # We merge stdout into stderr (stderr=PIPE, stdout=STDOUT) so the
            # per-job log file captures whichever stream ffmpeg uses for its
            # error messages. Some early-exit failures (bad arg, missing
            # codec, plugin load failure) print only to stdout and were
            # silently dropped before.
            with open(log_file, 'w', encoding='utf-8', errors='replace') as log_f:
                # Pre-pend the actual command line so post-mortem readers can
                # see exactly what was launched.
                log_f.write("# command: " + " ".join(repr(a) for a in cmd.args) + "\n\n")
                log_f.flush()
                self._ffmpeg_process = subprocess.Popen(
                    cmd.args,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding='utf-8',
                    errors='replace',
                )

                # Monitor merged output for progress (and capture everything
                # to disk so failures can be diagnosed after the fact).
                last_progress = time.time()
                while True:
                    if self.should_stop():
                        self._kill_ffmpeg()
                        raise WorkerStop("Worker stopping")

                    line = self._ffmpeg_process.stdout.readline()
                    if not line:
                        break

                    log_f.write(line)

                    # Parse progress
                    progress = parse_ffmpeg_progress(line)
                    if progress and 'time_sec' in progress:
                        REGISTRY.update(
                            self.name,
                            time_sec=progress.get('time_sec', 0.0),
                            duration_sec=cmd.expected_duration_sec or 0.0,
                            fps=progress.get('fps', 0.0),
                            speed=progress.get('speed', 0.0),
                            bitrate_kbps=progress.get('bitrate_kbps', 0.0),
                        )
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
                    # Surface the actual ffmpeg stderr so the operator can see
                    # why it bailed (codec init error, missing nvcuda.dll, bad
                    # input dimension for the HW encoder, etc). Code on its
                    # own is opaque — 4294967274 is just unsigned -22.
                    tail = self._tail_ffmpeg_log(log_file, lines=15)
                    logger.error(
                        f"[{self.name}] FFmpeg failed with code {return_code} "
                        f"(unsigned form of {(return_code - 0x100000000) if return_code > 0x7fffffff else return_code}). "
                        f"Last lines of ffmpeg stderr:\n{tail}"
                    )
                    return False

                return True

        except Exception as e:
            logger.error(f"[{self.name}] FFmpeg error: {e}")
            self._kill_ffmpeg()
            return False

    def _tail_ffmpeg_log(self, log_file: Path, lines: int = 15) -> str:
        """Read the last `lines` lines of the per-job ffmpeg log (or as many
        as exist). Returns a printable indented string, or '<empty>' if the
        file is empty / missing."""
        try:
            with open(log_file, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()
            if not content.strip():
                return "  <empty>"
            tail = content.splitlines()[-lines:]
            return "\n".join("  | " + ln for ln in tail)
        except OSError as e:
            return f"  <could not read {log_file}: {e}>"

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
        if self.disk_budget is not None:
            self.disk_budget.release(job.id)

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
            if self.disk_budget is not None:
                self.disk_budget.release(job.id)
        else:
            self.db.update_job_state(
                job.id,
                JobState.RETRY_WAIT,
                error_message=f"Retry {retry_count}: {error}",
            )


class UploadWorker(BaseWorker):
    """Worker that uploads transcoded files to Dropbox."""

    stage = "upload"

    def __init__(
        self,
        worker_id: int,
        config: Config,
        db: Database,
        dropbox: DropboxClient,
        stop_event: threading.Event,
        dispatcher: JobDispatcher,
        disk_budget: DiskBudget | None = None,
    ):
        super().__init__(f"uploader-{worker_id}", config, db, stop_event, dispatcher)
        self.dropbox = dropbox
        self.disk_budget = disk_budget

    def process_job(self, job: Job) -> None:
        """Upload the transcoded file for the given job."""
        logger.info(f"[{self.name}] Uploading: {job.output_path}")
        upload_size = 0
        if job.local_output_path:
            try:
                upload_size = Path(job.local_output_path).stat().st_size
            except OSError:
                upload_size = 0
        REGISTRY.begin(
            self.name, "upload", job.id, job.output_path or job.dropbox_path,
            bytes_total=upload_size,
        )

        try:
            self._upload_job(job)
        except Exception as e:
            logger.error(f"[{self.name}] Upload failed: {e}")
            self._handle_failure(job, str(e))

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

        # Upload to /<parent>/h265/<name>.MP4 — the temporary location
        # before the optional reorganization swap.
        local_size = output_path.stat().st_size
        self.dropbox.upload_file(
            output_path,
            job.output_path,
            overwrite=True,
            progress_callback=self._make_progress_callback(job, local_size),
        )

        logger.info(f"[{self.name}] Upload complete: {job.output_path}")

        final_path = job.output_path
        if self.config.legacy_reorganize:
            try:
                final_path = self._legacy_reorganize(job, local_size)
            except Exception as reorg_err:
                # Surface the failure but keep the upload state — the H.265
                # file is sitting at /<parent>/h265/<name> and reachable.
                logger.error(
                    f"[{self.name}] Reorganization failed for job {job.id} "
                    f"({job.dropbox_path}): {reorg_err}. "
                    f"H.265 left at {job.output_path}; original untouched."
                )

        # Mark done. Persist the *final* output path (post-reorg if it ran)
        # plus the actual H.265 byte count so /api/stats can report savings.
        self.db.update_job_state(
            job.id,
            JobState.DONE,
            output_path=final_path,
            output_size=int(local_size),
        )

        # Clean up staging if configured
        if self.config.delete_staging_after_upload:
            self._cleanup_staging(job)

    def _legacy_reorganize(self, job: Job, h265_size: int) -> str:
        """
        Honor the legacy_reorganize_min_age_days threshold, then delegate the
        actual swap to reorganize.reorganize_pair so the same code path is
        shared with `hd reorganize-existing`.
        """
        from pathlib import PurePosixPath

        from .reorganize import is_folder_settled, reorganize_pair

        original_p = PurePosixPath(job.dropbox_path)
        parent = str(original_p.parent)
        name = original_p.name

        activity = is_folder_settled(
            self.dropbox,
            parent,
            self.config.legacy_reorganize_min_age_days,
        )
        if not activity.settled:
            days = (f"{activity.days_since_newest:.1f}" if activity.days_since_newest is not None else "?")
            logger.info(
                f"[{self.name}] Reorganize deferred: {parent} has activity "
                f"{days}d old (< threshold {activity.threshold_days}d). "
                f"H.265 stays at {job.output_path}; future "
                f"`hd reorganize-existing` will swap when the folder goes quiet."
            )
            return job.output_path

        return reorganize_pair(
            self.dropbox,
            parent,
            name,
            int(job.dropbox_size or 0),
            h265_size,
        )

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

            REGISTRY.update(self.name, bytes_done=uploaded, bytes_total=total)

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
        if self.disk_budget is not None:
            self.disk_budget.release(job.id)

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
            if self.disk_budget is not None:
                self.disk_budget.release(job.id)
        else:
            # Stay in UPLOADING state for retry
            self.db.update_job_state(
                job.id,
                JobState.UPLOADING,  # Keep state, will be retried
                error_message=f"Upload retry {retry_count}: {error}",
            )
