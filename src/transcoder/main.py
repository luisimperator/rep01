#!/usr/bin/env python3
"""
Dropbox Video Transcoder - CLI and Daemon

Main entry point for the transcoder application.
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Optional

# Cross-platform file locking
if sys.platform == 'win32':
    import msvcrt
    LOCK_EX = msvcrt.LK_NBLCK
    LOCK_UN = msvcrt.LK_UNLCK
else:
    import fcntl
    LOCK_EX = fcntl.LOCK_EX | fcntl.LOCK_NB
    LOCK_UN = fcntl.LOCK_UN

import click
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

from .api import ApiServer
from .config import Config, EncoderPreference, TranscodeProfile, load_config, save_example_config
from .database import ACTIVE_STATES, Database, Job, JobState, TERMINAL_STATES
from .disk_budget import DiskBudget
from .dispatcher import JobDispatcher
from .dropbox_client import DropboxClient, make_client_from_config
from .encoder_detect import (
    EncoderType,
    detect_available_encoders,
    get_encoder_info_string,
    select_best_encoder,
)
from .rate_limit import TokenBucket
from .scanner import Scanner
from .updater import apply_update, check_for_update_async, installed_version
from .watchdog import HealthChecker, Watchdog
from .workers import DownloadWorker, TranscodeWorker, UploadWorker
from .inventory import (
    FileCategory,
    Inventory,
    InventoryScanner,
    format_inventory_report,
    format_top_files,
)

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
        # On Windows, FileHandler's default encoding is the local code page
        # (cp1252). Force UTF-8 so non-ASCII Dropbox paths don't crash logging.
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
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
        self.dispatcher: JobDispatcher | None = None
        self.rate_limiter: TokenBucket | None = None
        self.disk_budget: DiskBudget | None = None
        self.api_server: ApiServer | None = None
        self.stop_event = threading.Event()
        self.scan_trigger = threading.Event()
        self.started_at = time.time()
        self.workers: list[threading.Thread] = []
        self._lock_fd: int | None = None
        # Surface the last scan error so the dashboard can flag a stuck scan
        # rather than silently showing entries_seen=0 forever.
        self.last_scan_error: str | None = None
        self.last_scan_error_at: float | None = None

    def acquire_lock(self) -> bool:
        """Acquire exclusive lock to prevent multiple instances."""
        try:
            self.config.lockfile_path.parent.mkdir(parents=True, exist_ok=True)
            self._lock_fd = open(str(self.config.lockfile_path), 'w')
            if sys.platform == 'win32':
                msvcrt.locking(self._lock_fd.fileno(), LOCK_EX, 1)
            else:
                fcntl.flock(self._lock_fd.fileno(), LOCK_EX)
            self._lock_fd.write(str(os.getpid()))
            self._lock_fd.flush()
            return True
        except (OSError, IOError):
            return False

    def release_lock(self) -> None:
        """Release exclusive lock."""
        if self._lock_fd is not None:
            try:
                if sys.platform == 'win32':
                    msvcrt.locking(self._lock_fd.fileno(), LOCK_UN, 1)
                else:
                    fcntl.flock(self._lock_fd.fileno(), LOCK_UN)
                self._lock_fd.close()
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

        # Drop disk reservations for jobs that are no longer in flight after
        # recovery, so the staging budget doesn't leak across restarts.
        pruned = self.db.prune_stale_disk_reservations(ACTIVE_STATES)
        if pruned:
            logger.info(f"Pruned {pruned} stale disk reservations")

        # Rate-limit Dropbox API calls so we don't get throttled at scale
        self.rate_limiter = TokenBucket(
            rate_per_min=self.config.dropbox_api.rate_per_min,
            burst=self.config.dropbox_api.burst,
            name="dropbox",
        )

        # Dropbox client (refresh-token mode if configured, else short-lived)
        if not self.config.has_dropbox_auth():
            raise RuntimeError(
                "Dropbox auth missing. Run `hd auth` to set up a refresh "
                "token, or set dropbox_token in config.yaml."
            )
        self.dropbox = make_client_from_config(self.config, rate_limiter=self.rate_limiter)
        if not self.dropbox.check_connection():
            raise RuntimeError(
                "Failed to connect to Dropbox. If your token expired, "
                "run `hd auth` to set up a long-lived refresh token."
            )

        # Scanner (shares the daemon stop_event so bulk passes bail cleanly)
        self.scanner = Scanner(self.config, self.db, self.dropbox, self.stop_event)

        # Central job dispatcher feeds bounded queues consumed by workers
        self.dispatcher = JobDispatcher(self.config, self.db, self.stop_event)

        # Staging-disk budget. Disabled by default; flip config.disk_budget.enabled
        # to stall new downloads when near the cap.
        self.disk_budget = DiskBudget(
            staging_dir=self.config.local_staging_dir,
            db=self.db,
            max_staging_bytes=self.config.disk_budget.max_staging_bytes,
            min_free_bytes=self.config.disk_budget.min_free_bytes,
            poll_interval_sec=self.config.disk_budget.poll_interval_sec,
            enabled=self.config.disk_budget.enabled,
        )

        # Select encoder once. We pass verify=False because the synthetic
        # 64x64 testsrc clip in verify_encoder_works trips up some hardware
        # encoders (notably hevc_qsv with non-standard input formats) even
        # when they encode real footage fine; using the detection result
        # alone is more reliable in practice.
        encoder = select_best_encoder(self.config, verify=False)
        self._selected_encoder = encoder
        logger.info(f"Using encoder: {encoder.value}")

        # Fire-and-forget GitHub release check; the HTTP API surfaces the result
        # once it lands in the settings table.
        if self.config.updater.enabled:
            check_for_update_async(
                self.db,
                self.config.updater.github_repo,
                timeout_sec=self.config.updater.check_timeout_sec,
            )

    def start_workers(self) -> None:
        """Start the dispatcher and all worker threads."""
        assert self.db is not None
        assert self.dropbox is not None
        assert self.scanner is not None
        assert self.dispatcher is not None

        # Dispatcher must be running before workers try to consume
        self.dispatcher.start()
        self.workers.append(self.dispatcher)

        # Start the status API (loopback) so the GUI / curl can inspect us
        self.api_server = ApiServer(
            config=self.config,
            db=self.db,
            dispatcher=self.dispatcher,
            scan_trigger=self.scan_trigger,
            started_at_epoch=self.started_at,
        )
        # Expose the daemon back to the API so /api/status can read scan errors.
        self.api_server.daemon = self
        self.api_server.start()

        # Download workers
        for i in range(self.config.concurrency.download_workers):
            worker = DownloadWorker(
                i,
                self.config,
                self.db,
                self.dropbox,
                self.scanner,
                self.stop_event,
                self.dispatcher,
                disk_budget=self.disk_budget,
            )
            worker.start()
            self.workers.append(worker)

        # Transcode workers — reuse the encoder picked above so log + behavior
        # agree. Workers still hold a fallback path via select_best_encoder
        # inside _transcode_job for jobs created before encoder detection.
        encoder = self._selected_encoder
        for i in range(self.config.concurrency.transcode_workers):
            worker = TranscodeWorker(
                i,
                self.config,
                self.db,
                self.stop_event,
                self.dispatcher,
                encoder=encoder,
                disk_budget=self.disk_budget,
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
                self.dispatcher,
                disk_budget=self.disk_budget,
            )
            worker.start()
            self.workers.append(worker)

        # Watchdog
        watchdog = Watchdog(self.config, self.db, self.stop_event)
        watchdog.start()
        self.workers.append(watchdog)

        logger.info(
            f"Started dispatcher + {len(self.workers) - 2} workers + watchdog: "
            f"{self.config.concurrency.download_workers} download, "
            f"{self.config.concurrency.transcode_workers} transcode, "
            f"{self.config.concurrency.upload_workers} upload"
        )

    def run_scan_loop(self) -> None:
        """Run periodic scanning; /api/scan-now shortens the inter-scan sleep."""
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
                self.last_scan_error = None
            except Exception as e:
                logger.error(f"Scan error: {e}")
                self.last_scan_error = str(e)
                self.last_scan_error_at = time.time()

            # Sleep until either stop or scan-now trigger fires
            self.scan_trigger.clear()
            deadline = time.monotonic() + self.config.concurrency.scan_interval_sec
            while not self.stop_event.is_set():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                if self.scan_trigger.wait(timeout=min(remaining, 1.0)):
                    logger.info("scan-now triggered via API")
                    break

    def stop(self) -> None:
        """Signal all workers to stop."""
        logger.info("Stopping daemon...")
        self.stop_event.set()

        # Tear down the HTTP server first so new requests don't block shutdown
        if self.api_server is not None:
            self.api_server.shutdown()

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

    dropbox = make_client_from_config(config)
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

    dropbox = make_client_from_config(config)
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
    layout = config.output_layout.value
    mirror_root = config.output_mirror_root
    console.print("[cyan]Path Analysis:[/cyan]")
    console.print(
        f"  Output path: {get_output_path(dropbox_path, layout, config.dropbox_root, mirror_root)}"
    )
    console.print(f"  In h265 folder: {is_in_h265_folder(dropbox_path, mirror_root)}")

    # Check Dropbox
    if config.has_dropbox_auth():
        dropbox = make_client_from_config(config)
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
            output_path = get_output_path(
                dropbox_path, layout, config.dropbox_root, mirror_root
            )
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


@cli.command()
@click.option('--app-key', default=None, help='Dropbox app key. Defaults to the value in config.yaml or prompts for one.')
@click.option('--no-write', is_flag=True, help='Print the refresh token instead of writing it to config.yaml.')
@click.pass_context
def auth(ctx: click.Context, app_key: str | None, no_write: bool) -> None:
    """
    Obtain a long-lived Dropbox refresh token via PKCE OAuth.

    Run this once. The daemon then refreshes its access token automatically
    and runs unattended for months. Steps:

      1. You provide a Dropbox app key (from https://www.dropbox.com/developers/apps).
      2. This command opens an authorize URL in your browser.
      3. You click "Allow", Dropbox shows an authorization code.
      4. You paste the code back here.
      5. The refresh token is saved into config.yaml.
    """
    import base64
    import hashlib
    import json
    import re
    import secrets
    import urllib.parse
    import urllib.request
    import webbrowser

    config_path_str = ctx.obj.get('config_path')
    config = load_config(config_path_str)

    if app_key is None:
        app_key = config.dropbox_app_key

    if not app_key:
        console.print("[yellow]No Dropbox app key found in config.[/yellow]")
        console.print("Create or open an app at https://www.dropbox.com/developers/apps")
        console.print("Use 'Scoped access' + 'Full Dropbox' permissions including:")
        console.print("  files.content.read, files.content.write,")
        console.print("  files.metadata.read, files.metadata.write")
        app_key = click.prompt('Paste your Dropbox app key', type=str).strip()

    if not app_key:
        console.print("[red]App key is required.[/red]")
        sys.exit(1)

    # Generate PKCE code verifier (43-128 chars) and S256 challenge
    verifier = base64.urlsafe_b64encode(secrets.token_bytes(64)).rstrip(b'=').decode('ascii')
    challenge = base64.urlsafe_b64encode(
        hashlib.sha256(verifier.encode('ascii')).digest()
    ).rstrip(b'=').decode('ascii')

    authorize_url = (
        'https://www.dropbox.com/oauth2/authorize?'
        + urllib.parse.urlencode({
            'client_id': app_key,
            'response_type': 'code',
            'token_access_type': 'offline',
            'code_challenge': challenge,
            'code_challenge_method': 'S256',
        })
    )

    console.print("\n[cyan]Open this URL in your browser:[/cyan]")
    console.print(authorize_url)
    console.print()
    try:
        webbrowser.open(authorize_url)
    except Exception:
        pass

    auth_code = click.prompt('Paste the authorization code shown by Dropbox', type=str).strip()
    if not auth_code:
        console.print("[red]Authorization code is required.[/red]")
        sys.exit(1)

    # Exchange the code for a refresh token (PKCE: no app_secret needed)
    token_url = 'https://api.dropbox.com/oauth2/token'
    body = urllib.parse.urlencode({
        'code': auth_code,
        'grant_type': 'authorization_code',
        'code_verifier': verifier,
        'client_id': app_key,
    }).encode('ascii')

    req = urllib.request.Request(
        token_url,
        data=body,
        headers={'Content-Type': 'application/x-www-form-urlencoded'},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = json.loads(resp.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        err_body = e.read().decode('utf-8', errors='replace')
        console.print(f"[red]Token exchange failed: HTTP {e.code} {e.reason}[/red]")
        console.print(f"[red]{err_body}[/red]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]Token exchange failed: {e}[/red]")
        sys.exit(1)

    refresh_token = payload.get('refresh_token')
    if not refresh_token:
        console.print(f"[red]Dropbox response did not include a refresh_token: {payload}[/red]")
        sys.exit(1)

    console.print("[green]Got refresh token.[/green]")

    if no_write:
        console.print(f"\napp_key:       {app_key}")
        console.print(f"refresh_token: {refresh_token}")
        return

    # Locate the config.yaml to update
    if config_path_str:
        cfg_path = Path(config_path_str)
    else:
        for candidate in [Path('config.yaml'), Path('config.yml')]:
            if candidate.exists():
                cfg_path = candidate
                break
        else:
            cfg_path = Path('config.yaml')

    if cfg_path.exists():
        raw = cfg_path.read_text(encoding='utf-8')
    else:
        raw = ''

    def _set_yaml_key(text: str, key: str, value: str) -> str:
        """Update or insert a top-level scalar key in YAML preserving comments."""
        pattern = re.compile(rf'^{re.escape(key)}\s*:.*$', re.MULTILINE)
        line = f'{key}: "{value}"'
        if pattern.search(text):
            return pattern.sub(line, text, count=1)
        if text and not text.endswith('\n'):
            text += '\n'
        return text + line + '\n'

    raw = _set_yaml_key(raw, 'dropbox_app_key', app_key)
    raw = _set_yaml_key(raw, 'dropbox_refresh_token', refresh_token)
    cfg_path.write_text(raw, encoding='utf-8')

    console.print(f"[green]Saved app_key and refresh_token to {cfg_path}.[/green]")
    console.print("\nThe daemon will now refresh its access token automatically.")
    console.print("Restart it to apply:")
    console.print("  schtasks /End /TN HeavyDropsDaemon")
    console.print("  schtasks /Run /TN HeavyDropsDaemon")


@cli.command('reorganize-existing')
@click.option(
    '--min-age-days',
    type=int,
    default=None,
    help='Skip folders with any user activity in the last N days. '
         'Default: legacy_reorganize_min_age_days from config.yaml.',
)
@click.option('--dry-run', is_flag=True, help='Show what would happen without changing anything.')
@click.option(
    '--folder',
    default=None,
    help='Only consider this Dropbox subfolder (default: dropbox_root from config).',
)
@click.option(
    '--limit',
    type=int,
    default=None,
    help='Process at most N folders (useful for testing).',
)
@click.pass_context
def reorganize_existing(
    ctx: click.Context,
    min_age_days: int | None,
    dry_run: bool,
    folder: str | None,
    limit: int | None,
) -> None:
    """
    Retroactively apply the legacy reorganization to files already converted.

    Walks the Dropbox tree (or a single --folder), finds every pair of:
        /<parent>/<name>            (presumed H.264 original)
        /<parent>/h265/<name>       (H.265 output, e.g. from v6.0.2)
    where /<parent>/h264/<name> does NOT yet exist, and applies the swap so:
        /<parent>/<name>            -> H.265 (in original's place)
        /<parent>/h264/<name>       -> H.264 backup
        /<parent>/h265/h265 feito.txt updated

    Folders with recent user activity (any file modified within
    --min-age-days days) are SKIPPED so active projects aren't disturbed.
    """
    from .reorganize import find_unreorganized_pairs, is_folder_settled, reorganize_pair

    config_path = ctx.obj.get('config_path')
    verbose = ctx.obj.get('verbose', False)

    config = load_config(config_path)
    setup_logging(verbose)

    if not config.has_dropbox_auth():
        console.print("[red]Error: Dropbox auth not configured. Run `hd auth`.[/red]")
        sys.exit(1)

    threshold = config.legacy_reorganize_min_age_days if min_age_days is None else min_age_days
    root = folder or config.dropbox_root

    console.print(
        f"[blue]Scanning {root} for unreorganized H.264/H.265 pairs...[/blue]"
    )
    console.print(
        f"[blue]Threshold: {threshold} days "
        f"({'always reorganize' if threshold == 0 else f'skip folders with activity in the last {threshold} days'})[/blue]"
    )

    from .dropbox_client import make_client_from_config
    dropbox = make_client_from_config(config)

    if not dropbox.check_connection():
        console.print("[red]Error: Could not connect to Dropbox.[/red]")
        sys.exit(1)

    candidates = find_unreorganized_pairs(dropbox, root)
    if not candidates:
        console.print("[green]No unreorganized pairs found.[/green]")
        return

    console.print(f"[blue]Found {len(candidates)} folder(s) with H.265 outputs to consider.[/blue]\n")

    ready: list = []
    deferred: list = []

    for cand in candidates:
        activity = is_folder_settled(dropbox, cand.parent, threshold)
        cand.activity = activity
        if not activity.settled:
            cand.skip_reason = (
                f"active ({activity.days_since_newest:.1f}d "
                f"since last modify, < threshold {threshold}d)"
            )
            deferred.append(cand)
        else:
            ready.append(cand)

    table = Table(title="Reorganize plan")
    table.add_column("Folder", style="cyan", overflow="fold")
    table.add_column("Pairs", justify="right", style="green")
    table.add_column("Status", style="yellow")
    for cand in ready:
        table.add_row(cand.parent, str(len(cand.pairs)), "ready")
    for cand in deferred:
        table.add_row(cand.parent, str(len(cand.pairs)), f"defer: {cand.skip_reason}")
    console.print(table)

    if not ready:
        console.print("[yellow]No folders are settled enough to reorganize. Done.[/yellow]")
        return

    total_pairs = sum(len(c.pairs) for c in ready)
    console.print(
        f"\n[blue]{len(ready)} folder(s), {total_pairs} pair(s) ready to reorganize.[/blue]"
    )

    if dry_run:
        console.print("[yellow]Dry run — no changes made.[/yellow]")
        return

    if limit is not None:
        ready = ready[:limit]
        console.print(f"[yellow]--limit set: processing first {len(ready)} folder(s) only.[/yellow]")

    reorganized = 0
    failed = 0
    for cand in ready:
        console.print(f"\n[cyan]{cand.parent}[/cyan]  ({len(cand.pairs)} pair(s))")
        for pair in cand.pairs:
            try:
                reorganize_pair(
                    dropbox,
                    cand.parent,
                    pair.name,
                    int(pair.original.size or 0),
                    int(pair.h265.size or 0),
                )
                console.print(f"  [green]+[/green] {pair.name}")
                reorganized += 1
            except Exception as e:
                console.print(f"  [red]x[/red] {pair.name}: {e}")
                failed += 1

    console.print()
    console.print(f"[green]Reorganized: {reorganized}[/green]")
    if failed:
        console.print(f"[red]Failed: {failed}[/red]")
    console.print(f"[yellow]Deferred (active folders): {sum(len(c.pairs) for c in deferred)}[/yellow]")


@cli.command('show-encoders')
def show_encoders() -> None:
    """Show available hardware encoders."""
    console.print(get_encoder_info_string())


@cli.command()
@click.option(
    '--install-dir',
    type=click.Path(exists=True, file_okay=False),
    default=None,
    help='Path to the git checkout (default: the package source tree).',
)
def update(install_dir: str | None) -> None:
    """
    Apply an available update: git pull + pip install if pyproject changed.

    Does not restart the daemon — the operator (or Task Scheduler) is
    responsible for that. The command prints every git/pip line it runs.
    """
    if install_dir is None:
        # Default to the repository root that contains this source tree
        install_dir = str(Path(__file__).resolve().parents[2])

    console.print(f"[blue]Updating HeavyDrops Transcoder at {install_dir}[/blue]")
    console.print(f"[blue]Installed version: {installed_version()}[/blue]\n")

    rc = apply_update(Path(install_dir), log_fn=console.print)

    if rc == 0:
        console.print("\n[green]Update applied.[/green] Restart the daemon to load the new code.")
    else:
        console.print(f"\n[red]Update failed with exit code {rc}.[/red]")
        sys.exit(rc)


@cli.command('check-update')
@click.pass_context
def check_update(ctx: click.Context) -> None:
    """Check GitHub for a newer release and print the result."""
    config_path = ctx.obj.get('config_path')
    config = load_config(config_path)
    config.ensure_directories()

    db = Database(config.database_path)
    db.initialize()
    try:
        from .updater import check_for_update
        status = check_for_update(
            db,
            config.updater.github_repo,
            timeout_sec=config.updater.check_timeout_sec,
        )
    finally:
        db.close()

    console.print(f"Installed: [cyan]{status.current_version}[/cyan]")
    console.print(f"Latest:    [cyan]{status.latest_tag or '(unknown)'}[/cyan]")
    if status.update_available:
        console.print(f"\n[yellow]Update available.[/yellow] Run: [bold]hd update[/bold]")
    elif status.error:
        console.print(f"\n[red]Check failed:[/red] {status.error}")
    else:
        console.print("\n[green]Up to date.[/green]")


@cli.command()
@click.option('--full', is_flag=True, help='Include individual file details (larger output)')
@click.option('--save', 'save_path', type=click.Path(), help='Save inventory to JSON file')
@click.option('--load', 'load_path', type=click.Path(exists=True), help='Load and display existing inventory')
@click.option('--top', default=20, help='Show top N largest files needing transcoding')
@click.option('--speed', default=50.0, help='Processing speed in GB/hour for time estimates')
@click.pass_context
def inventory(
    ctx: click.Context,
    full: bool,
    save_path: str | None,
    load_path: str | None,
    top: int,
    speed: float,
) -> None:
    """
    Scan Dropbox and show complete inventory of transcoding work.

    This performs a full scan via API (no downloads) to map all files
    and show exactly what needs to be transcoded, what's already done,
    and estimates for completion time.

    Examples:

        # Quick scan with summary
        transcoder inventory

        # Full scan with file details, save to JSON
        transcoder inventory --full --save inventory.json

        # Load and display previous inventory
        transcoder inventory --load inventory.json

        # Adjust speed estimate (default 50 GB/hour)
        transcoder inventory --speed 30
    """
    config_path = ctx.obj.get('config_path')
    verbose = ctx.obj.get('verbose', False)

    # Load existing inventory
    if load_path:
        console.print(f"[blue]Loading inventory from {load_path}...[/blue]")
        inv = Inventory.load(Path(load_path))
        console.print(format_inventory_report(inv, gb_per_hour=speed))
        if inv.files and top > 0:
            console.print()
            console.print(format_top_files(inv, FileCategory.NEEDS_TRANSCODING, top))
        return

    # Perform new scan
    config = load_config(config_path)
    setup_logging(verbose)

    if not config.has_dropbox_auth():
        console.print("[red]Error: Dropbox auth not configured. Run `hd auth` or set dropbox_token.[/red]")
        return

    dropbox = make_client_from_config(config)

    if not dropbox.check_connection():
        console.print("[red]Error: Could not connect to Dropbox. Run `hd auth` if your token expired.[/red]")
        return

    scanner = InventoryScanner(config, dropbox)

    # Progress display
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.fields[files]} files"),
        console=console,
    ) as progress:
        task = progress.add_task(
            f"[cyan]Scanning {config.dropbox_root}...",
            total=None,
            files=0,
        )

        def update_progress(count: int, path: str) -> None:
            progress.update(task, files=count, description=f"[cyan]Scanning: {path[:50]}...")

        inv = scanner.scan(include_files=full, progress_callback=update_progress)

    # Display report
    console.print()
    console.print(format_inventory_report(inv, gb_per_hour=speed))

    # Show top files if we have file details
    if inv.files and top > 0:
        console.print()
        console.print(format_top_files(inv, FileCategory.NEEDS_TRANSCODING, top))

    # Save if requested
    if save_path:
        inv.save(Path(save_path))
        console.print(f"\n[green]Inventory saved to: {save_path}[/green]")


@cli.command('inventory-diff')
@click.argument('old_inventory', type=click.Path(exists=True))
@click.argument('new_inventory', type=click.Path(exists=True))
def inventory_diff(old_inventory: str, new_inventory: str) -> None:
    """
    Compare two inventory files and show progress.

    This helps track progress over time by comparing two inventory snapshots.
    """
    old_inv = Inventory.load(Path(old_inventory))
    new_inv = Inventory.load(Path(new_inventory))

    old_stats = old_inv.stats
    new_stats = new_inv.stats

    # Calculate differences
    done_diff = new_stats.already_done_count - old_stats.already_done_count
    done_bytes_diff = new_stats.already_done_bytes - old_stats.already_done_bytes
    remaining_diff = new_stats.needs_transcoding_count - old_stats.needs_transcoding_count

    console.print("=" * 60)
    console.print("INVENTORY COMPARISON")
    console.print("=" * 60)
    console.print()
    console.print(f"Old scan: {old_inv.scan_time}")
    console.print(f"New scan: {new_inv.scan_time}")
    console.print()
    console.print("-" * 60)
    console.print("PROGRESS")
    console.print("-" * 60)
    console.print()
    console.print(f"Files completed:     {done_diff:+,} ({done_bytes_diff / (1024**4):+.2f} TB)")
    console.print(f"Files remaining:     {remaining_diff:+,}")
    console.print()
    console.print(f"Old progress:        {old_stats.progress_percent:.1f}%")
    console.print(f"New progress:        {new_stats.progress_percent:.1f}%")
    console.print(f"Progress increase:   {new_stats.progress_percent - old_stats.progress_percent:+.1f}%")
    console.print()
    console.print("=" * 60)


@cli.command('gui')
@click.pass_context
def gui(ctx: click.Context) -> None:
    """
    Open the daemon's status dashboard in the default browser.

    This does not launch the daemon itself — run ``transcoder start`` (or
    let the Task Scheduler do it). The dashboard served at the API's bind
    address is the GUI.
    """
    config_path = ctx.obj.get('config_path')
    config = load_config(config_path)

    host = config.api.bind
    # When the API is bound to 0.0.0.0 (unusual), the dashboard is actually
    # reachable on localhost from the same machine.
    if host in ('', '0.0.0.0'):
        host = '127.0.0.1'
    url = f"http://{host}:{config.api.port}/"

    console.print(f"[blue]Opening dashboard at {url}[/blue]")

    import webbrowser
    opened = webbrowser.open(url, new=2)
    if not opened:
        console.print(
            f"[yellow]Could not launch a browser automatically.[/yellow] "
            f"Open [cyan]{url}[/cyan] manually."
        )


def main() -> None:
    """Main entry point."""
    cli()


def gui_main() -> None:
    """Entry point for `hd-gui`: open the dashboard without parsing CLI args."""
    try:
        config = load_config(None)
    except Exception:  # pragma: no cover — defensive
        config = Config()

    host = config.api.bind
    if host in ('', '0.0.0.0'):
        host = '127.0.0.1'
    url = f"http://{host}:{config.api.port}/"

    import webbrowser
    webbrowser.open(url, new=2)


if __name__ == '__main__':
    main()
