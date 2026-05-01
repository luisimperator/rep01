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

    def _normalize_path(self, path: str) -> str:
        """Normalize Dropbox path (lowercase, leading slash)."""
        path = path.strip()
        if not path.startswith('/'):
            path = '/' + path
        # Dropbox API wants empty string for root
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
                if e.error.is_path() and e.error.get_path().is_not_found():
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
        """Get space usage information."""
        usage = self._dbx.users_get_space_usage()
        return {
            'used': usage.used,
            'allocated': usage.allocation.get_individual().allocated
            if usage.allocation.is_individual() else None,
        }

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
            if e.error.is_path() and e.error.get_path().is_not_found():
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
                # Small file: single upload
                with open(local_path, 'rb') as f:
                    metadata = self._dbx.files_upload(
                        f.read(),
                        norm_path,
                        mode=mode,
                    )
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
                    if progress_callback:
                        progress_callback(bytes_uploaded, file_size)
                    return DropboxFileInfo.from_metadata(metadata)

                if progress_callback:
                    progress_callback(bytes_uploaded, file_size)

        raise DropboxClientError("Upload failed: unexpected end of file")

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
