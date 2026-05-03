"""
Dropbox API client wrapper.

Provides file listing, metadata, download, and upload operations.
"""

from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import BinaryIO, Callable, Generator

import dropbox
from dropbox.exceptions import ApiError, AuthError
from dropbox.files import (
    FileMetadata,
    FolderMetadata,
    ListFolderResult,
    WriteMode,
)

from .rate_limit import TokenBucket

logger = logging.getLogger(__name__)


class BandwidthGovernor:
    """Leaky-bucket throttle shared by every download/upload chunk loop.

    When `throttled` is True, each chunk completion sleeps long enough
    that the cumulative byte rate stays at or below `max_bytes_per_sec`.
    Used by the deep-scan worker to claim ~95% of the WAN by capping
    the main pipeline's transfer rate to a trickle while it probes.

    Singleton (module-level GOVERNOR below). Thread-safe per-call: the
    bookkeeping is per-instance state guarded by a lock so concurrent
    workers see one shared bucket and divide the cap between them
    instead of each claiming the full quota.
    """

    def __init__(self) -> None:
        self.throttled = False
        # Default low cap — set when throttled. 1 MB/s gives the main
        # pipeline ~5% of a typical 200 Mbps WAN, leaving the rest for
        # deep-scan probes. Tunable via set_throttle().
        self.max_bytes_per_sec = 1_000_000
        self._lock = threading.Lock()
        self._bucket_sec = time.monotonic()  # window start
        self._bucket_bytes = 0               # bytes consumed in current window

    def set_throttle(self, on: bool, max_mbps: float = 1.0) -> None:
        with self._lock:
            self.throttled = bool(on)
            self.max_bytes_per_sec = max(50_000, int(max_mbps * 1_000_000))
            # Reset the bucket so the next chunk doesn't see stale credit.
            self._bucket_sec = time.monotonic()
            self._bucket_bytes = 0

    def consume(self, n_bytes: int) -> None:
        """Account `n_bytes` against the bucket; sleep if we're over the cap."""
        if not self.throttled or n_bytes <= 0:
            return
        sleep_sec = 0.0
        with self._lock:
            now = time.monotonic()
            window = now - self._bucket_sec
            # Roll the window each second so the bucket doesn't grow forever.
            if window >= 1.0:
                self._bucket_sec = now
                self._bucket_bytes = 0
                window = 0.0
            self._bucket_bytes += n_bytes
            allowed_bytes = int(self.max_bytes_per_sec * (window + 1.0))
            if self._bucket_bytes > allowed_bytes:
                # Sleep enough to bring effective rate back to the cap.
                excess_bytes = self._bucket_bytes - allowed_bytes
                sleep_sec = excess_bytes / self.max_bytes_per_sec
                # Cap any single sleep so a chunk burst doesn't stall a
                # whole worker for minutes.
                sleep_sec = min(sleep_sec, 5.0)
        if sleep_sec > 0:
            time.sleep(sleep_sec)


import threading
GOVERNOR = BandwidthGovernor()


@dataclass
class DropboxFileInfo:
    """Information about a file in Dropbox."""
    path: str
    name: str
    size: int
    rev: str
    server_modified: datetime
    content_hash: str | None = None

    @classmethod
    def from_metadata(cls, metadata: FileMetadata) -> DropboxFileInfo:
        """Create from Dropbox FileMetadata."""
        return cls(
            path=metadata.path_display or metadata.path_lower or "",
            name=metadata.name,
            size=metadata.size,
            rev=metadata.rev,
            server_modified=metadata.server_modified,
            content_hash=metadata.content_hash,
        )


class DropboxClientError(Exception):
    """Base exception for Dropbox client errors."""
    pass


class DropboxAuthError(DropboxClientError):
    """Authentication error."""
    pass


class DropboxNotFoundError(DropboxClientError):
    """File or folder not found."""
    pass


class DropboxRevChangedError(DropboxClientError):
    """File revision changed during operation."""
    pass


def _is_path_not_found(api_error) -> bool:
    """True iff the Dropbox ApiError represents a path-not-found in
    *whichever* operation-specific Error union it carries. Defensive
    against `is_not_found` not existing on the concrete error type
    (e.g. WriteError on uploads has no such method)."""
    try:
        if not hasattr(api_error, "error"):
            return False
        err = api_error.error
        if not getattr(err, "is_path", lambda: False)():
            return False
        path_err = err.get_path()
        is_nf = getattr(path_err, "is_not_found", None)
        return bool(is_nf() if callable(is_nf) else False)
    except Exception:
        return False


def make_client_from_config(config, rate_limiter: TokenBucket | None = None) -> "DropboxClient":
    """
    Build a DropboxClient from a Config object, picking the right auth mode.

    Prefers refresh-token auth (app_key + refresh_token) over short-lived
    access tokens. Used by every entry point so the auth wiring lives in
    exactly one place.
    """
    return DropboxClient(
        token=config.dropbox_token,
        app_key=config.dropbox_app_key,
        app_secret=config.dropbox_app_secret,
        refresh_token=config.dropbox_refresh_token,
        rate_limiter=rate_limiter,
    )


class DropboxClient:
    """
    Wrapper for Dropbox API operations.

    Provides retry logic and error handling for common operations.
    """

    CHUNK_SIZE = 4 * 1024 * 1024  # 4MB chunks for upload
    DOWNLOAD_CHUNK_SIZE = 8 * 1024 * 1024  # 8MB chunks for download

    def __init__(
        self,
        token: str = "",
        max_retries: int = 5,
        retry_delay: float = 2.0,
        rate_limiter: TokenBucket | None = None,
        app_key: str = "",
        app_secret: str = "",
        refresh_token: str = "",
    ):
        """
        Initialize Dropbox client.

        Two auth modes are supported:

        1) Refresh token (recommended for long-running daemons): pass
           `app_key` + `refresh_token` (and optionally `app_secret` for
           confidential clients). The SDK refreshes the short-lived access
           token automatically, so the daemon runs for months unattended.
        2) Short-lived access token (legacy / ad-hoc): pass `token` only.
           Modern Dropbox tokens generated in the App Console expire in
           around 4 hours.

        Args:
            token: Dropbox short-lived access token.
            max_retries: Maximum number of retries for transient errors.
            retry_delay: Base delay between retries (exponential backoff).
            rate_limiter: Optional token bucket. If provided, every retried
                operation acquires `weight` tokens before issuing a request.
            app_key: Dropbox app key (refresh-token mode).
            app_secret: Dropbox app secret (optional; PKCE clients omit it).
            refresh_token: Dropbox long-lived refresh token.
        """
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.rate_limiter = rate_limiter
        self.namespace = "home"          # "home" or "team"
        self.namespace_id: str | None = None

        if refresh_token and app_key:
            kwargs = {
                'oauth2_refresh_token': refresh_token,
                'app_key': app_key,
            }
            if app_secret:
                kwargs['app_secret'] = app_secret
            self._dbx = dropbox.Dropbox(**kwargs)
        elif token:
            self._dbx = dropbox.Dropbox(token)
        else:
            raise DropboxAuthError(
                "Dropbox auth missing: provide either dropbox_token "
                "or dropbox_app_key + dropbox_refresh_token. "
                "Run `hd auth` to set up a refresh token."
            )

        # Auto-switch to the team-space root for Dropbox Business accounts so
        # paths like /HeavyDrops/h265test (which live under the team folder
        # tree) are reachable. Without this, the SDK uses the user's personal
        # home namespace and team folders are invisible.
        self._configure_namespace()

    def _configure_namespace(self) -> None:
        from dropbox import common
        try:
            account = self._dbx.users_get_current_account()
        except Exception as e:
            logger.warning(f"could not fetch dropbox account info: {e}")
            return
        root_info = getattr(account, "root_info", None)
        if root_info is None:
            return
        home_ns = getattr(root_info, "home_namespace_id", None)
        root_ns = getattr(root_info, "root_namespace_id", None)
        if not home_ns or not root_ns:
            return
        if home_ns != root_ns:
            # Team account: switch to the team's root namespace so the user's
            # team folders show up at /. The SDK builds a fresh client wrapped
            # with the Dropbox-API-Path-Root header.
            self._dbx = self._dbx.with_path_root(common.PathRoot.root(root_ns))
            self.namespace = "team"
            self.namespace_id = root_ns
            logger.info(f"dropbox: switched to team namespace ({root_ns})")
        else:
            self.namespace = "home"
            self.namespace_id = home_ns
            logger.info(f"dropbox: using personal namespace ({home_ns})")

    def _normalize_path(self, path: str) -> str:
        """Normalize Dropbox path: leading slash, no trailing slash, root → ''.

        files_list_folder rejects paths that end in '/' with "not_found", so we
        strip them defensively. Empty paths and '/' are mapped to '' which is
        the form the SDK wants for the account root. Backslashes leak in when
        callers run pathlib.Path on Windows (WindowsPath uses '\\') — convert
        them so the SDK doesn't reject the request with malformed_path.
        """
        path = path.strip().replace('\\', '/')
        if not path.startswith('/'):
            path = '/' + path
        # Strip trailing slashes, but never let the path become empty before
        # the root check.
        while len(path) > 1 and path.endswith('/'):
            path = path[:-1]
        if path == '/':
            return ''
        return path

    def _retry_operation(
        self,
        operation: Callable[[], any],
        operation_name: str,
        weight: float = 1.0,
    ) -> any:
        """Execute operation with retry logic and optional token-bucket throttle."""
        last_error = None

        for attempt in range(self.max_retries):
            if self.rate_limiter is not None:
                self.rate_limiter.acquire(weight=weight)
            try:
                return operation()
            except AuthError as e:
                raise DropboxAuthError(f"Authentication failed: {e}") from e
            except ApiError as e:
                # `is_path()` exists on most operation-specific Errors, but
                # what `get_path()` returns differs by op:
                #   - ListFolderError.path -> LookupError (.is_not_found())
                #   - UploadError.path     -> UploadWriteFailed.reason ->
                #                             WriteError (NO .is_not_found())
                #   - GetMetadataError.path -> LookupError (.is_not_found())
                # Be tolerant: only flag DropboxNotFoundError when the
                # returned error union actually exposes is_not_found().
                if _is_path_not_found(e):
                    raise DropboxNotFoundError(f"Path not found: {e}") from e

                last_error = e
                # Server-driven backoff: respect Dropbox's retry hint when present
                retry_after = getattr(e, 'backoff', None)
                if retry_after and self.rate_limiter is not None:
                    try:
                        self.rate_limiter.on_throttle(float(retry_after))
                    except (TypeError, ValueError):
                        pass
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2 ** attempt)
                    if retry_after:
                        try:
                            delay = max(delay, float(retry_after))
                        except (TypeError, ValueError):
                            pass
                    logger.warning(
                        f"{operation_name} failed (attempt {attempt + 1}), "
                        f"retrying in {delay:.1f}s: {e}"
                    )
                    time.sleep(delay)
            except Exception as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    delay = self.retry_delay * (2 ** attempt)
                    logger.warning(
                        f"{operation_name} failed (attempt {attempt + 1}), "
                        f"retrying in {delay:.1f}s: {e}"
                    )
                    time.sleep(delay)

        raise DropboxClientError(
            f"{operation_name} failed after {self.max_retries} attempts: {last_error}"
        ) from last_error

    def check_connection(self) -> bool:
        """Check if connection and auth are valid."""
        try:
            self._dbx.users_get_current_account()
            return True
        except (AuthError, ApiError) as e:
            logger.error(f"Dropbox connection check failed: {e}")
            return False

    def get_account_info(self) -> dict:
        """Get current account information."""
        account = self._dbx.users_get_current_account()
        return {
            'account_id': account.account_id,
            'email': account.email,
            'name': account.name.display_name,
        }

    def get_space_usage(self) -> dict:
        """Get space usage information.

        Handles both individual and team (Business) allocations. For team
        accounts, `used` is the user's individual contribution; the team-
        wide total is exposed as `team_used` when present.
        """
        usage = self._dbx.users_get_space_usage()
        out: dict = {
            'used': usage.used,
            'allocated': None,
            'allocation_type': None,
            'team_used': None,
        }
        try:
            alloc = usage.allocation
            if alloc.is_individual():
                ind = alloc.get_individual()
                out['allocation_type'] = 'individual'
                out['allocated'] = ind.allocated
            elif alloc.is_team():
                team = alloc.get_team()
                out['allocation_type'] = 'team'
                # `team.allocated` is the whole-team quota; `team.used` is
                # the entire team's storage. user_within_team_space_used
                # is this user's slice when present.
                out['allocated'] = team.allocated
                out['team_used'] = team.used
        except Exception:
            # Defensive — newer Dropbox SDK shapes shouldn't break the call.
            pass
        return out

    def get_metadata(self, path: str) -> DropboxFileInfo | None:
        """
        Get file metadata.

        Args:
            path: Dropbox path to file.

        Returns:
            DropboxFileInfo or None if not found.
        """
        norm_path = self._normalize_path(path)

        def operation() -> DropboxFileInfo | None:
            metadata = self._dbx.files_get_metadata(norm_path)
            if isinstance(metadata, FileMetadata):
                return DropboxFileInfo.from_metadata(metadata)
            return None

        try:
            return self._retry_operation(operation, f"get_metadata({path})")
        except DropboxNotFoundError:
            return None

    def list_folder(
        self,
        path: str,
        recursive: bool = False,
    ) -> Generator[DropboxFileInfo, None, None]:
        """
        List files in folder.

        Args:
            path: Dropbox folder path.
            recursive: If True, list recursively.

        Yields:
            DropboxFileInfo for each file found.
        """
        for entry in self.list_folder_entries(path, recursive=recursive):
            if isinstance(entry, DropboxFileInfo):
                yield entry

    def list_folder_entries(
        self,
        path: str,
        recursive: bool = False,
    ) -> Generator[DropboxFileInfo | str, None, None]:
        """
        Low-level generator yielding DropboxFileInfo for each file, and the
        cursor string at each page boundary (as a bare str). Callers that want
        to persist cursors for resumable scans should check ``isinstance(x, str)``.
        """
        norm_path = self._normalize_path(path)

        def list_page(c: str | None) -> ListFolderResult:
            if c:
                return self._dbx.files_list_folder_continue(c)
            return self._dbx.files_list_folder(norm_path, recursive=recursive)

        cursor: str | None = None
        has_more = True

        while has_more:
            # Closure captures cursor by name — snapshot it for _retry_operation
            current = cursor
            result = self._retry_operation(
                lambda: list_page(current),
                f"list_folder({path})",
            )

            for entry in result.entries:
                if isinstance(entry, FileMetadata):
                    yield DropboxFileInfo.from_metadata(entry)

            cursor = result.cursor
            has_more = result.has_more
            # Yield the cursor after each page so callers can checkpoint
            yield cursor

    def list_folder_delta(
        self,
        start_cursor: str,
    ) -> Generator[tuple[str, object] | str, None, None]:
        """
        Yield incremental changes since ``start_cursor``.

        Each tuple is ``(kind, payload)`` where kind is ``"file"`` (payload is
        DropboxFileInfo) or ``"deleted"`` (payload is the deleted path as str).
        Folder entries are skipped. At every page boundary the new cursor is
        also yielded as a bare str so the caller can persist it.
        """
        from dropbox.files import DeletedMetadata  # local import keeps top clean

        cursor: str = start_cursor
        has_more = True

        while has_more:
            current = cursor
            result = self._retry_operation(
                lambda: self._dbx.files_list_folder_continue(current),
                "list_folder_continue",
            )

            for entry in result.entries:
                if isinstance(entry, FileMetadata):
                    yield ("file", DropboxFileInfo.from_metadata(entry))
                elif isinstance(entry, DeletedMetadata):
                    path = entry.path_display or entry.path_lower or ""
                    yield ("deleted", path)
                # FolderMetadata entries are ignored (scanner walks files only)

            cursor = result.cursor
            has_more = result.has_more
            yield cursor

    def folder_exists(self, path: str) -> bool:
        """Check if folder exists."""
        norm_path = self._normalize_path(path)
        try:
            metadata = self._dbx.files_get_metadata(norm_path)
            return isinstance(metadata, FolderMetadata)
        except ApiError as e:
            if _is_path_not_found(e):
                return False
            raise

    def create_folder(self, path: str) -> bool:
        """
        Create folder if it doesn't exist.

        Returns:
            True if folder was created, False if already exists.
        """
        norm_path = self._normalize_path(path)

        def operation() -> bool:
            try:
                self._dbx.files_create_folder_v2(norm_path)
                return True
            except ApiError as e:
                # Folder already exists
                if hasattr(e.error, 'is_path') and e.error.is_path():
                    path_error = e.error.get_path()
                    if hasattr(path_error, 'is_conflict') and path_error.is_conflict():
                        return False
                raise

        return self._retry_operation(operation, f"create_folder({path})")

    def download_file(
        self,
        dropbox_path: str,
        local_path: Path,
        expected_rev: str | None = None,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> str:
        """
        Download file from Dropbox.

        Args:
            dropbox_path: Source path in Dropbox.
            local_path: Destination local path.
            expected_rev: If provided, abort if rev doesn't match.
            progress_callback: Optional callback(bytes_downloaded, total_bytes).

        Returns:
            The file revision.

        Raises:
            DropboxRevChangedError: If rev changed during download.
        """
        norm_path = self._normalize_path(dropbox_path)

        # Get metadata first to check rev and size
        metadata = self._retry_operation(
            lambda: self._dbx.files_get_metadata(norm_path),
            f"get_metadata({dropbox_path})",
        )

        if not isinstance(metadata, FileMetadata):
            raise DropboxClientError(f"Path is not a file: {dropbox_path}")

        if expected_rev and metadata.rev != expected_rev:
            raise DropboxRevChangedError(
                f"File revision changed: expected {expected_rev}, got {metadata.rev}"
            )

        total_size = metadata.size
        current_rev = metadata.rev

        # Ensure parent directory exists
        local_path.parent.mkdir(parents=True, exist_ok=True)

        # Download with progress tracking
        def operation() -> str:
            _, response = self._dbx.files_download(norm_path, rev=current_rev)

            bytes_downloaded = 0
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=self.DOWNLOAD_CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
                        bytes_downloaded += len(chunk)
                        if progress_callback:
                            progress_callback(bytes_downloaded, total_size)
                        GOVERNOR.consume(len(chunk))

            return current_rev

        return self._retry_operation(operation, f"download({dropbox_path})")

    def download_file_with_rev_check(
        self,
        dropbox_path: str,
        local_path: Path,
        expected_rev: str,
        progress_callback: Callable[[int, int], None] | None = None,
        check_interval_mb: int = 100,
    ) -> str:
        """
        Download file with periodic revision checks.

        Checks the file revision periodically during download to detect
        changes early.

        Args:
            dropbox_path: Source path in Dropbox.
            local_path: Destination local path.
            expected_rev: Expected file revision.
            progress_callback: Optional callback(bytes_downloaded, total_bytes).
            check_interval_mb: Check rev every N megabytes downloaded.

        Returns:
            The file revision.

        Raises:
            DropboxRevChangedError: If rev changed during download.
        """
        norm_path = self._normalize_path(dropbox_path)
        check_interval = check_interval_mb * 1024 * 1024
        last_check_bytes = 0

        # Initial metadata check
        metadata = self._retry_operation(
            lambda: self._dbx.files_get_metadata(norm_path),
            f"get_metadata({dropbox_path})",
        )

        if not isinstance(metadata, FileMetadata):
            raise DropboxClientError(f"Path is not a file: {dropbox_path}")

        if metadata.rev != expected_rev:
            raise DropboxRevChangedError(
                f"File revision changed before download: expected {expected_rev}, got {metadata.rev}"
            )

        total_size = metadata.size
        local_path.parent.mkdir(parents=True, exist_ok=True)

        _, response = self._dbx.files_download(norm_path, rev=expected_rev)

        bytes_downloaded = 0
        try:
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=self.DOWNLOAD_CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
                        bytes_downloaded += len(chunk)

                        if progress_callback:
                            progress_callback(bytes_downloaded, total_size)
                        GOVERNOR.consume(len(chunk))

                        # Periodic rev check
                        if bytes_downloaded - last_check_bytes >= check_interval:
                            current_metadata = self._dbx.files_get_metadata(norm_path)
                            if isinstance(current_metadata, FileMetadata):
                                if current_metadata.rev != expected_rev:
                                    raise DropboxRevChangedError(
                                        f"File revision changed during download at "
                                        f"{bytes_downloaded / (1024*1024):.1f} MB"
                                    )
                            last_check_bytes = bytes_downloaded

        except DropboxRevChangedError:
            # Clean up partial download
            if local_path.exists():
                local_path.unlink()
            raise

        return expected_rev

    def upload_file(
        self,
        local_path: Path,
        dropbox_path: str,
        overwrite: bool = False,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> DropboxFileInfo:
        """
        Upload file to Dropbox.

        Args:
            local_path: Source local path.
            dropbox_path: Destination path in Dropbox.
            overwrite: If True, overwrite existing file.
            progress_callback: Optional callback(bytes_uploaded, total_bytes).

        Returns:
            DropboxFileInfo of uploaded file.
        """
        norm_path = self._normalize_path(dropbox_path)
        file_size = local_path.stat().st_size
        mode = WriteMode.overwrite if overwrite else WriteMode.add

        def operation() -> DropboxFileInfo:
            if file_size <= self.CHUNK_SIZE:
                # Small file: single upload. Account for it post-hoc since
                # we can't sub-chunk a single-shot upload.
                with open(local_path, 'rb') as f:
                    metadata = self._dbx.files_upload(
                        f.read(),
                        norm_path,
                        mode=mode,
                    )
                GOVERNOR.consume(file_size)
                if progress_callback:
                    progress_callback(file_size, file_size)
                return DropboxFileInfo.from_metadata(metadata)
            else:
                # Large file: chunked upload
                return self._chunked_upload(
                    local_path,
                    norm_path,
                    file_size,
                    mode,
                    progress_callback,
                )

        return self._retry_operation(operation, f"upload({dropbox_path})")

    def _chunked_upload(
        self,
        local_path: Path,
        dropbox_path: str,
        file_size: int,
        mode: WriteMode,
        progress_callback: Callable[[int, int], None] | None,
    ) -> DropboxFileInfo:
        """Upload large file in chunks."""
        with open(local_path, 'rb') as f:
            # Start upload session
            chunk = f.read(self.CHUNK_SIZE)
            session = self._dbx.files_upload_session_start(chunk)
            session_id = session.session_id
            bytes_uploaded = len(chunk)
            GOVERNOR.consume(len(chunk))

            if progress_callback:
                progress_callback(bytes_uploaded, file_size)

            # Upload remaining chunks
            cursor = dropbox.files.UploadSessionCursor(
                session_id=session_id,
                offset=bytes_uploaded,
            )

            while bytes_uploaded < file_size:
                chunk = f.read(self.CHUNK_SIZE)
                if not chunk:
                    break

                if bytes_uploaded + len(chunk) < file_size:
                    # More chunks to come
                    self._dbx.files_upload_session_append_v2(chunk, cursor)
                    bytes_uploaded += len(chunk)
                    cursor.offset = bytes_uploaded
                    GOVERNOR.consume(len(chunk))
                else:
                    # Final chunk
                    commit = dropbox.files.CommitInfo(
                        path=dropbox_path,
                        mode=mode,
                    )
                    metadata = self._dbx.files_upload_session_finish(
                        chunk,
                        cursor,
                        commit,
                    )
                    bytes_uploaded += len(chunk)
                    GOVERNOR.consume(len(chunk))
                    if progress_callback:
                        progress_callback(bytes_uploaded, file_size)
                    return DropboxFileInfo.from_metadata(metadata)

                if progress_callback:
                    progress_callback(bytes_uploaded, file_size)

        raise DropboxClientError("Upload failed: unexpected end of file")

    def list_subfolders(self, path: str) -> list[dict]:
        """List immediate subfolders + files under `path` for the dashboard browser.

        Returns a list of {"name": str, "is_folder": bool, "path": str} sorted
        with folders first, then alphabetical. `path` may be '/' or '' for the
        account root. Raises DropboxNotFoundError if the path doesn't exist.
        """
        norm_path = self._normalize_path(path)

        def operation() -> list[dict]:
            entries: list[dict] = []
            cursor: str | None = None
            while True:
                if cursor:
                    result = self._dbx.files_list_folder_continue(cursor)
                else:
                    result = self._dbx.files_list_folder(norm_path, recursive=False)
                for e in result.entries:
                    if isinstance(e, FolderMetadata):
                        entries.append({
                            "name": e.name,
                            "is_folder": True,
                            "path": e.path_display or e.path_lower or "",
                        })
                    elif isinstance(e, FileMetadata):
                        entries.append({
                            "name": e.name,
                            "is_folder": False,
                            "path": e.path_display or e.path_lower or "",
                            "size": e.size,
                        })
                if not result.has_more:
                    break
                cursor = result.cursor
            entries.sort(key=lambda x: (not x["is_folder"], x["name"].lower()))
            return entries

        return self._retry_operation(operation, f"list_subfolders({path})")

    def file_exists(self, path: str) -> bool:
        """Check if file exists."""
        metadata = self.get_metadata(path)
        return metadata is not None

    def read_text_file(self, path: str, encoding: str = 'utf-8') -> str | None:
        """
        Read text file content from Dropbox.

        Args:
            path: Dropbox path to text file.
            encoding: Text encoding (default utf-8).

        Returns:
            File content as string, or None if file not found.
        """
        norm_path = self._normalize_path(path)

        def operation() -> str:
            _, response = self._dbx.files_download(norm_path)
            return response.content.decode(encoding)

        try:
            return self._retry_operation(operation, f"read_text_file({path})")
        except DropboxNotFoundError:
            return None

    def download_partial(
        self,
        dropbox_path: str,
        local_path: Path,
        start_byte: int,
        length_bytes: int,
    ) -> int:
        """
        Range-download a slice of a Dropbox file. Used by the preflight
        codec probe so we can detect HEVC-native files (which have nothing
        to transcode) from the first few MB instead of the whole file.

        Goes through `files_get_temporary_link` because the SDK's
        `files_download` doesn't expose HTTP Range. The temp URL is valid
        for ~4 hours and is safe to GET with a Range header.

        Returns the number of bytes actually written, which may be less
        than `length_bytes` if the file is shorter or the server caps the
        range. Raises `DropboxNotFoundError` for missing paths.
        """
        import requests
        norm_path = self._normalize_path(dropbox_path)

        def operation() -> int:
            try:
                link_result = self._dbx.files_get_temporary_link(norm_path)
            except ApiError as e:
                if _is_path_not_found(e):
                    raise DropboxNotFoundError(dropbox_path)
                raise
            url = link_result.link
            end_byte = start_byte + length_bytes - 1
            headers = {'Range': f'bytes={start_byte}-{end_byte}'}
            with requests.get(url, headers=headers, stream=True, timeout=60) as r:
                # 206 = partial content, 200 = server ignored Range (returned full).
                # Both are fine — we just write up to length_bytes.
                if r.status_code not in (200, 206):
                    raise OSError(
                        f"partial download HTTP {r.status_code} for {dropbox_path}"
                    )
                local_path.parent.mkdir(parents=True, exist_ok=True)
                written = 0
                with local_path.open('wb') as fh:
                    for chunk in r.iter_content(chunk_size=64 * 1024):
                        if not chunk:
                            continue
                        remaining = length_bytes - written
                        if remaining <= 0:
                            break
                        if len(chunk) > remaining:
                            chunk = chunk[:remaining]
                        fh.write(chunk)
                        written += len(chunk)
                return written

        return self._retry_operation(operation, f"download_partial({dropbox_path}, {start_byte}+{length_bytes})")

    def get_temporary_link(self, path: str) -> str | None:
        """Return a short-lived CDN URL for the file, or None if missing.

        Wraps files_get_temporary_link. The URL is valid for ~4 hours and
        can be GET'd with a Range header — used by the deep-scan worker so
        ffprobe reads only the file's MOOV header without downloading the
        whole asset.
        """
        norm_path = self._normalize_path(path)

        def operation() -> str | None:
            try:
                result = self._dbx.files_get_temporary_link(norm_path)
            except ApiError as e:
                if _is_path_not_found(e):
                    return None
                raise
            return result.link

        try:
            return self._retry_operation(operation, f"get_temporary_link({path})")
        except DropboxNotFoundError:
            return None

    def delete_file(self, path: str) -> bool:
        """
        Delete file from Dropbox.

        Returns:
            True if deleted, False if not found.
        """
        norm_path = self._normalize_path(path)

        def operation() -> bool:
            try:
                self._dbx.files_delete_v2(norm_path)
                return True
            except ApiError as e:
                if e.error.is_path_lookup() and e.error.get_path_lookup().is_not_found():
                    return False
                raise

        return self._retry_operation(operation, f"delete({path})")

    def move_file(self, src: str, dst: str, allow_overwrite: bool = False) -> bool:
        """
        Move (rename) a file or folder inside Dropbox.

        Args:
            src: source path.
            dst: destination path.
            allow_overwrite: if True, deletes any existing file at `dst` first.
                files_move_v2 has no autorename-overwrite combination, so we
                fake it by deleting the destination beforehand. Use with care.

        Returns:
            True on success.
        """
        norm_src = self._normalize_path(src)
        norm_dst = self._normalize_path(dst)

        if allow_overwrite and self.file_exists(dst):
            self.delete_file(dst)

        def operation() -> bool:
            self._dbx.files_move_v2(norm_src, norm_dst, autorename=False)
            return True

        return self._retry_operation(operation, f"move({src} -> {dst})")

    def write_text_file(
        self,
        path: str,
        content: str,
        encoding: str = "utf-8",
    ) -> bool:
        """
        Write a text file to Dropbox, overwriting any existing content.
        """
        norm_path = self._normalize_path(path)
        data = content.encode(encoding)

        def operation() -> bool:
            self._dbx.files_upload(data, norm_path, mode=WriteMode("overwrite"))
            return True

        return self._retry_operation(operation, f"write_text({path})")

    def compute_content_hash(self, local_path: Path) -> str:
        """
        Compute Dropbox content hash for local file.

        This matches Dropbox's content_hash algorithm.
        """
        block_size = 4 * 1024 * 1024  # 4MB blocks
        block_hashes = []

        with open(local_path, 'rb') as f:
            while True:
                block = f.read(block_size)
                if not block:
                    break
                block_hashes.append(hashlib.sha256(block).digest())

        return hashlib.sha256(b''.join(block_hashes)).hexdigest()
