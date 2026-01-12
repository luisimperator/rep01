#!/usr/bin/env python3
"""
Dropbox Video Transcoder - CLI and Daemon

Main entry point for the transcoder application.
"""

from __future__ import annotations

import fcntl
import logging
import os
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Optional

import click
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

from .config import Config, EncoderPreference, TranscodeProfile, load_config, save_example_config
from .database import ACTIVE_STATES, Database, Job, JobState, TERMINAL_STATES
from .dropbox_client import DropboxClient
from .encoder_detect import (
    EncoderType,
    detect_available_encoders,
    get_encoder_info_string,
    select_best_encoder,
)
from .scanner import Scanner
from .watchdog import HealthChecker, Watchdog
from .workers import DownloadWorker, TranscodeWorker, UploadWorker

console = Console()
logger = logging.getLogger(__name__)


def setup_logging(verbose: bool = False, log_file: Path | None = None) -> None:
    """Configure logging with Rich handler."""
    level = logging.DEBUG if verbose else logging.INFO

    handlers: list[logging.Handler] = [
        RichHandler(
            console=console,
            show_time=True,
            show_path=False,
            rich_tracebacks=True,
        )
    ]

    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        )
        handlers.append(file_handler)

    logging.basicConfig(
        level=level,
        handlers=handlers,
        format="%(message)s",
    )

    # Reduce noise from libraries
    logging.getLogger("dropbox").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


class Daemon:
    """Main daemon that orchestrates the transcoding pipeline."""

    def __init__(self, config: Config):
        """Initialize daemon with configuration."""
        self.config = config
        self.db: Database | None = None
        self.dropbox: DropboxClient | None = None
        self.scanner: Scanner | None = None
        self.stop_event = threading.Event()
        self.workers: list[threading.Thread] = []
        self._lock_fd: int | None = None

    def acquire_lock(self) -> bool:
        """Acquire exclusive lock to prevent multiple instances."""
        try:
            self.config.lockfile_path.parent.mkdir(parents=True, exist_ok=True)
            self._lock_fd = os.open(
                str(self.config.lockfile_path),
                os.O_RDWR | os.O_CREAT,
            )
            fcntl.flock(self._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            os.write(self._lock_fd, str(os.getpid()).encode())
            return True
        except (OSError, IOError):
            return False

    def release_lock(self) -> None:
        """Release exclusive lock."""
        if self._lock_fd is not None:
            try:
                fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
                os.close(self._lock_fd)
                self.config.lockfile_path.unlink(missing_ok=True)
            except Exception:
                pass
            self._lock_fd = None

    def setup(self) -> None:
        """Initialize components."""
        self.config.ensure_directories()

        # Database
        self.db = Database(self.config.database_path)
        self.db.initialize()

        # Recover any interrupted jobs
        recovered = self.db.recover_active_jobs()
        if recovered:
            logger.info(f"Recovered {recovered} interrupted jobs")

        # Dropbox client
        self.dropbox = DropboxClient(self.config.dropbox_token)
        if not self.dropbox.check_connection():
            raise RuntimeError("Failed to connect to Dropbox")

        # Scanner
        self.scanner = Scanner(self.config, self.db, self.dropbox)

        # Select encoder
        encoder = select_best_encoder(self.config)
        logger.info(f"Using encoder: {encoder.value}")

    def start_workers(self) -> None:
        """Start all worker threads."""
        assert self.db is not None
        assert self.dropbox is not None
        assert self.scanner is not None

        # Download workers
        for i in range(self.config.concurrency.download_workers):
            worker = DownloadWorker(
                i,
                self.config,
                self.db,
                self.dropbox,
                self.scanner,
                self.stop_event,
            )
            worker.start()
            self.workers.append(worker)

        # Transcode workers
        encoder = select_best_encoder(self.config, verify=False)
        for i in range(self.config.concurrency.transcode_workers):
            worker = TranscodeWorker(
                i,
                self.config,
                self.db,
                self.stop_event,
                encoder=encoder,
            )
            worker.start()
            self.workers.append(worker)

        # Upload workers
        for i in range(self.config.concurrency.upload_workers):
            worker = UploadWorker(
                i,
                self.config,
                self.db,
                self.dropbox,
                self.stop_event,
            )
            worker.start()
            self.workers.append(worker)

        # Watchdog
        watchdog = Watchdog(self.config, self.db, self.stop_event)
        watchdog.start()
        self.workers.append(watchdog)

        logger.info(
            f"Started {len(self.workers)} workers: "
            f"{self.config.concurrency.download_workers} download, "
            f"{self.config.concurrency.transcode_workers} transcode, "
            f"{self.config.concurrency.upload_workers} upload"
        )

    def run_scan_loop(self) -> None:
        """Run periodic scanning."""
        assert self.scanner is not None

        while not self.stop_event.is_set():
            try:
                logger.info("Starting scan...")
                stats = self.scanner.scan()
                logger.info(
                    f"Scan complete: {stats['new']} new, "
                    f"{stats['waiting_stable']} waiting, "
                    f"{stats['skipped_small']} too small"
                )
            except Exception as e:
                logger.error(f"Scan error: {e}")

            # Wait for next scan
            for _ in range(self.config.concurrency.scan_interval_sec):
                if self.stop_event.is_set():
                    break
                time.sleep(1)

    def stop(self) -> None:
        """Signal all workers to stop."""
        logger.info("Stopping daemon...")
        self.stop_event.set()

        # Wait for workers
        for worker in self.workers:
            worker.join(timeout=30)

        if self.db:
            self.db.close()

        self.release_lock()
        logger.info("Daemon stopped")

    def run(self) -> None:
        """Run the daemon."""
        if not self.acquire_lock():
            console.print("[red]Another instance is already running[/red]")
            sys.exit(1)

        try:
            self.setup()
            self.start_workers()
            self.run_scan_loop()
        finally:
            self.stop()


# Click CLI
@click.group()
@click.option('-c', '--config', 'config_path', type=click.Path(exists=True),
              help='Path to config file')
@click.option('-v', '--verbose', is_flag=True, help='Enable verbose logging')
@click.pass_context
def cli(ctx: click.Context, config_path: str | None, verbose: bool) -> None:
    """Dropbox Video Transcoder - H.264 to H.265/HEVC transcoding daemon."""
    ctx.ensure_object(dict)
    ctx.obj['config_path'] = config_path
    ctx.obj['verbose'] = verbose


@cli.command()
@click.pass_context
def start(ctx: click.Context) -> None:
    """Start the transcoder daemon (foreground)."""
    config_path = ctx.obj.get('config_path')
    verbose = ctx.obj.get('verbose', False)

    config = load_config(config_path)
    setup_logging(verbose, config.log_dir / 'transcoder.log')

    daemon = Daemon(config)

    # Setup signal handlers
    def signal_handler(signum: int, frame: any) -> None:
        logger.info(f"Received signal {signum}, shutting down...")
        daemon.stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    console.print("[green]Starting Dropbox Video Transcoder...[/green]")
    daemon.run()


@cli.command('run-once')
@click.pass_context
def run_once(ctx: click.Context) -> None:
    """Run a single scan and process iteration."""
    config_path = ctx.obj.get('config_path')
    verbose = ctx.obj.get('verbose', False)

    config = load_config(config_path)
    setup_logging(verbose)
    config.ensure_directories()

    # Initialize
    db = Database(config.database_path)
    db.initialize()

    dropbox = DropboxClient(config.dropbox_token)
    scanner = Scanner(config, db, dropbox)

    # Scan
    console.print("[blue]Running scan...[/blue]")
    stats = scanner.scan()

    table = Table(title="Scan Results")
    table.add_column("Metric", style="cyan")
    table.add_column("Count", style="green")

    for key, value in stats.items():
        table.add_row(key, str(value))

    console.print(table)
    db.close()


@cli.command('scan-now')
@click.option('--dry-run', is_flag=True, help='Scan without creating jobs')
@click.pass_context
def scan_now(ctx: click.Context, dry_run: bool) -> None:
    """Run a scan immediately."""
    config_path = ctx.obj.get('config_path')
    verbose = ctx.obj.get('verbose', False)

    config = load_config(config_path)
    setup_logging(verbose)
    config.ensure_directories()

    db = Database(config.database_path)
    db.initialize()

    dropbox = DropboxClient(config.dropbox_token)
    scanner = Scanner(config, db, dropbox)

    mode = "[yellow](dry run)[/yellow]" if dry_run else ""
    console.print(f"[blue]Scanning {config.dropbox_root}...[/blue] {mode}")

    stats = scanner.scan(dry_run=dry_run)

    table = Table(title="Scan Results")
    table.add_column("Metric", style="cyan")
    table.add_column("Count", style="green")

    for key, value in stats.items():
        table.add_row(key, str(value))

    console.print(table)
    db.close()


@cli.command()
@click.pass_context
def status(ctx: click.Context) -> None:
    """Show current queue status."""
    config_path = ctx.obj.get('config_path')

    config = load_config(config_path)
    config.ensure_directories()

    db = Database(config.database_path)
    db.initialize()

    stats = db.get_stats()

    table = Table(title="Queue Status")
    table.add_column("State", style="cyan")
    table.add_column("Count", style="green")

    state_counts = stats.get('state_counts', {})
    for state in JobState:
        count = state_counts.get(state.value, 0)
        if count > 0 or state in ACTIVE_STATES:
            table.add_row(state.value, str(count))

    console.print(table)

    # Summary
    total_done = state_counts.get(JobState.DONE.value, 0)
    total_bytes = stats.get('total_bytes_done', 0)
    avg_time = stats.get('avg_transcode_seconds', 0)

    console.print(f"\n[green]Total completed:[/green] {total_done}")
    console.print(f"[green]Total bytes processed:[/green] {total_bytes / (1024**3):.2f} GB")
    console.print(f"[green]Avg transcode time:[/green] {avg_time:.1f}s")

    db.close()


@cli.command('list-queue')
@click.option('--state', type=click.Choice([s.value for s in JobState]),
              help='Filter by state')
@click.option('--limit', default=50, help='Maximum jobs to show')
@click.pass_context
def list_queue(ctx: click.Context, state: str | None, limit: int) -> None:
    """List jobs in the queue."""
    config_path = ctx.obj.get('config_path')

    config = load_config(config_path)
    config.ensure_directories()

    db = Database(config.database_path)
    db.initialize()

    states = {JobState(state)} if state else None
    jobs = db.list_queue(states=states, limit=limit)

    table = Table(title=f"Queue ({len(jobs)} jobs)")
    table.add_column("ID", style="cyan")
    table.add_column("State", style="yellow")
    table.add_column("Path", style="white", max_width=50)
    table.add_column("Size", style="green")
    table.add_column("Retries", style="red")

    for job in jobs:
        size_gb = job.dropbox_size / (1024 ** 3)
        table.add_row(
            str(job.id),
            job.state.value,
            job.dropbox_path,
            f"{size_gb:.2f} GB",
            str(job.retry_count),
        )

    console.print(table)
    db.close()


@cli.command('retry-failed')
@click.pass_context
def retry_failed(ctx: click.Context) -> None:
    """Reset all FAILED jobs to retry."""
    config_path = ctx.obj.get('config_path')

    config = load_config(config_path)
    config.ensure_directories()

    db = Database(config.database_path)
    db.initialize()

    count = db.reset_failed_jobs()
    console.print(f"[green]Reset {count} failed jobs for retry[/green]")

    db.close()


@cli.command()
@click.pass_context
def doctor(ctx: click.Context) -> None:
    """Run health checks."""
    config_path = ctx.obj.get('config_path')

    config = load_config(config_path)
    checker = HealthChecker(config)

    console.print("[blue]Running health checks...[/blue]\n")

    results = checker.run_all_checks()

    table = Table(title="Health Check Results")
    table.add_column("Check", style="cyan")
    table.add_column("Status", style="white")
    table.add_column("Details", style="white")

    all_ok = True
    for name, result in results.items():
        status_icon = "[green]OK[/green]" if result['ok'] else "[red]FAIL[/red]"
        if not result['ok']:
            all_ok = False
        table.add_row(name, status_icon, result['message'])

    console.print(table)

    if all_ok:
        console.print("\n[green]All checks passed![/green]")
    else:
        console.print("\n[red]Some checks failed. Please fix issues before running.[/red]")


@cli.command('dry-run')
@click.argument('dropbox_path')
@click.pass_context
def dry_run(ctx: click.Context, dropbox_path: str) -> None:
    """Preview what would happen for a specific file."""
    config_path = ctx.obj.get('config_path')
    verbose = ctx.obj.get('verbose', False)

    config = load_config(config_path)
    setup_logging(verbose)

    from .utils import get_output_path, is_in_h265_folder

    console.print(f"[blue]Analyzing: {dropbox_path}[/blue]\n")

    # Check path patterns
    console.print("[cyan]Path Analysis:[/cyan]")
    console.print(f"  Output path: {get_output_path(dropbox_path)}")
    console.print(f"  In h265 folder: {is_in_h265_folder(dropbox_path)}")

    # Check Dropbox
    if config.dropbox_token:
        dropbox = DropboxClient(config.dropbox_token)
        metadata = dropbox.get_metadata(dropbox_path)

        if metadata:
            console.print(f"\n[cyan]Dropbox Metadata:[/cyan]")
            console.print(f"  Size: {metadata.size / (1024**3):.2f} GB")
            console.print(f"  Rev: {metadata.rev}")
            console.print(f"  Modified: {metadata.server_modified}")

            # Size check
            min_bytes = config.min_size_bytes()
            if metadata.size < min_bytes:
                console.print(f"\n[yellow]Would be SKIPPED: size < {config.min_size_gb} GB[/yellow]")
            else:
                console.print(f"\n[green]Size OK (>= {config.min_size_gb} GB)[/green]")

            # Output exists?
            output_path = get_output_path(dropbox_path)
            if dropbox.file_exists(output_path):
                console.print(f"[yellow]Output already exists: {output_path}[/yellow]")
        else:
            console.print(f"\n[red]File not found in Dropbox[/red]")
    else:
        console.print("\n[yellow]No Dropbox token configured[/yellow]")

    # Show encoder
    console.print(f"\n[cyan]Encoder Configuration:[/cyan]")
    console.print(f"  Preference: {config.encoder_preference.value}")
    console.print(f"  Profile: {config.profile.value}")
    console.print(f"  CQ Value: {config.cq_value}")
    console.print(get_encoder_info_string(config.ffmpeg_path))


@cli.command('init-config')
@click.argument('path', type=click.Path(), default='config.yaml')
def init_config(path: str) -> None:
    """Create an example configuration file."""
    config_path = Path(path)

    if config_path.exists():
        if not click.confirm(f"{config_path} already exists. Overwrite?"):
            return

    save_example_config(config_path)
    console.print(f"[green]Created example config at: {config_path}[/green]")
    console.print("\nEdit the file and set your DROPBOX_TOKEN environment variable.")


@cli.command('show-encoders')
def show_encoders() -> None:
    """Show available hardware encoders."""
    console.print(get_encoder_info_string())


def main() -> None:
    """Main entry point."""
    cli()


if __name__ == '__main__':
    main()
