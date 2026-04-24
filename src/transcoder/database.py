"""
SQLite database management for job queue and state tracking.

Provides idempotent job management with state transitions and metrics.
"""

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Generator


class JobState(str, Enum):
    """Job state machine states."""
    NEW = "NEW"
    STABLE_WAIT = "STABLE_WAIT"
    DOWNLOADING = "DOWNLOADING"
    DOWNLOADED = "DOWNLOADED"
    PROBING = "PROBING"
    TRANSCODING = "TRANSCODING"
    UPLOADING = "UPLOADING"
    DONE = "DONE"
    SKIPPED_HEVC = "SKIPPED_HEVC"
    SKIPPED_ALREADY_EXISTS = "SKIPPED_ALREADY_EXISTS"
    SKIPPED_TOO_SMALL = "SKIPPED_TOO_SMALL"
    FAILED = "FAILED"
    RETRY_WAIT = "RETRY_WAIT"


# States that indicate a job is "in progress" and should be recovered on restart
ACTIVE_STATES = {
    JobState.DOWNLOADING,
    JobState.DOWNLOADED,
    JobState.PROBING,
    JobState.TRANSCODING,
    JobState.UPLOADING,
}

# Terminal states that don't need further processing
TERMINAL_STATES = {
    JobState.DONE,
    JobState.SKIPPED_HEVC,
    JobState.SKIPPED_ALREADY_EXISTS,
    JobState.SKIPPED_TOO_SMALL,
}

# States that can be retried
RETRYABLE_STATES = {
    JobState.FAILED,
    JobState.RETRY_WAIT,
}


@dataclass
class Job:
    """Represents a transcoding job."""
    id: int
    dropbox_path: str
    dropbox_rev: str
    dropbox_size: int
    output_path: str
    state: JobState
    retry_count: int
    error_message: str | None
    local_input_path: str | None
    local_output_path: str | None
    input_codec: str | None
    output_codec: str | None
    input_duration_sec: float | None
    output_duration_sec: float | None
    input_bitrate_kbps: int | None
    output_bitrate_kbps: int | None
    encoder_used: str | None
    transcode_start: datetime | None
    transcode_end: datetime | None
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> Job:
        """Create Job from database row."""
        return cls(
            id=row['id'],
            dropbox_path=row['dropbox_path'],
            dropbox_rev=row['dropbox_rev'],
            dropbox_size=row['dropbox_size'],
            output_path=row['output_path'],
            state=JobState(row['state']),
            retry_count=row['retry_count'],
            error_message=row['error_message'],
            local_input_path=row['local_input_path'],
            local_output_path=row['local_output_path'],
            input_codec=row['input_codec'],
            output_codec=row['output_codec'],
            input_duration_sec=row['input_duration_sec'],
            output_duration_sec=row['output_duration_sec'],
            input_bitrate_kbps=row['input_bitrate_kbps'],
            output_bitrate_kbps=row['output_bitrate_kbps'],
            encoder_used=row['encoder_used'],
            transcode_start=_parse_datetime(row['transcode_start']),
            transcode_end=_parse_datetime(row['transcode_end']),
            created_at=_parse_datetime(row['created_at']) or datetime.now(timezone.utc),
            updated_at=_parse_datetime(row['updated_at']) or datetime.now(timezone.utc),
        )


@dataclass
class ScanState:
    """Persisted cursor + progress of the recursive Dropbox scan."""
    dropbox_root: str
    cursor: str | None
    bulk_pass_complete: bool
    bulk_started_at: datetime | None
    bulk_completed_at: datetime | None
    last_delta_at: datetime | None
    entries_seen: int


@dataclass
class StabilityCheck:
    """Represents a file stability check record."""
    id: int
    dropbox_path: str
    check_time: datetime
    size: int
    rev: str
    server_modified: str
    content_hash: str | None


def _parse_datetime(value: str | None) -> datetime | None:
    """Parse ISO format datetime string."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None


def _now_iso() -> str:
    """Get current UTC time in ISO format."""
    return datetime.now(timezone.utc).isoformat()


SCHEMA = """
-- Jobs table: main job queue and state tracking
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dropbox_path TEXT NOT NULL,
    dropbox_rev TEXT NOT NULL,
    dropbox_size INTEGER NOT NULL,
    output_path TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'NEW',
    retry_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    local_input_path TEXT,
    local_output_path TEXT,
    input_codec TEXT,
    output_codec TEXT,
    input_duration_sec REAL,
    output_duration_sec REAL,
    input_bitrate_kbps INTEGER,
    output_bitrate_kbps INTEGER,
    encoder_used TEXT,
    transcode_start TEXT,
    transcode_end TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(dropbox_path, dropbox_rev)
);

-- Index for efficient state queries
CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state);
CREATE INDEX IF NOT EXISTS idx_jobs_path ON jobs(dropbox_path);
CREATE INDEX IF NOT EXISTS idx_jobs_updated ON jobs(updated_at);

-- One active row per dropbox_path: blocks duplicate queued/in-progress jobs
-- when the same file is rediscovered between scans.
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_active_path
    ON jobs(dropbox_path)
    WHERE state NOT IN ('DONE','FAILED','SKIPPED_HEVC','SKIPPED_ALREADY_EXISTS','SKIPPED_TOO_SMALL');

-- Stability checks table: tracks file stability over time (R2)
CREATE TABLE IF NOT EXISTS stability_checks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dropbox_path TEXT NOT NULL,
    check_time TEXT NOT NULL DEFAULT (datetime('now')),
    size INTEGER NOT NULL,
    rev TEXT NOT NULL,
    server_modified TEXT NOT NULL,
    content_hash TEXT
);

-- Index for stability queries
CREATE INDEX IF NOT EXISTS idx_stability_path ON stability_checks(dropbox_path);
CREATE INDEX IF NOT EXISTS idx_stability_time ON stability_checks(dropbox_path, check_time DESC);

-- Metrics table: aggregated statistics
CREATE TABLE IF NOT EXISTS metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    metric_name TEXT NOT NULL,
    metric_value REAL NOT NULL,
    recorded_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_metrics_name ON metrics(metric_name);

-- Settings table: runtime settings persistence
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Scan cursor persistence (singleton) — see schema.sql for docs.
CREATE TABLE IF NOT EXISTS scan_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    dropbox_root TEXT NOT NULL,
    cursor TEXT,
    bulk_pass_complete INTEGER NOT NULL DEFAULT 0,
    bulk_started_at TEXT,
    bulk_completed_at TEXT,
    last_delta_at TEXT,
    entries_seen INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS feito_cache (
    folder_path TEXT PRIMARY KEY,
    filenames TEXT NOT NULL,
    last_checked TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS disk_reservations (
    job_id INTEGER PRIMARY KEY,
    reserved_bytes INTEGER NOT NULL,
    reserved_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Trigger to update updated_at on jobs
CREATE TRIGGER IF NOT EXISTS jobs_updated_at
    AFTER UPDATE ON jobs
    BEGIN
        UPDATE jobs SET updated_at = datetime('now') WHERE id = NEW.id;
    END;
"""


class Database:
    """Thread-safe SQLite database manager for job queue."""

    def __init__(self, db_path: Path | str):
        """
        Initialize database connection.

        Args:
            db_path: Path to SQLite database file.
        """
        self.db_path = Path(db_path)
        self._local = threading.local()
        self._init_lock = threading.Lock()
        self._initialized = False

    def _get_connection(self) -> sqlite3.Connection:
        """Get thread-local database connection."""
        if not hasattr(self._local, 'connection') or self._local.connection is None:
            self._local.connection = sqlite3.connect(
                str(self.db_path),
                check_same_thread=False,
                timeout=30.0,
            )
            self._local.connection.row_factory = sqlite3.Row
            self._local.connection.execute("PRAGMA journal_mode=WAL")
            self._local.connection.execute("PRAGMA busy_timeout=30000")
            self._local.connection.execute("PRAGMA foreign_keys=ON")
        return self._local.connection

    def initialize(self) -> None:
        """Initialize database schema."""
        with self._init_lock:
            if self._initialized:
                return
            conn = self._get_connection()
            conn.executescript(SCHEMA)
            conn.commit()
            self._initialized = True

    @contextmanager
    def transaction(self) -> Generator[sqlite3.Connection, None, None]:
        """Context manager for database transactions."""
        conn = self._get_connection()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def close(self) -> None:
        """Close thread-local connection."""
        if hasattr(self._local, 'connection') and self._local.connection:
            self._local.connection.close()
            self._local.connection = None

    # =========================================================================
    # Job Operations
    # =========================================================================

    def create_job(
        self,
        dropbox_path: str,
        dropbox_rev: str,
        dropbox_size: int,
        output_path: str,
        state: JobState = JobState.NEW,
    ) -> Job | None:
        """
        Create a new job (idempotent by path+rev).

        Returns:
            Created or existing Job, or None if conflict.
        """
        with self.transaction() as conn:
            # Try to insert new job
            try:
                cursor = conn.execute(
                    """
                    INSERT INTO jobs (dropbox_path, dropbox_rev, dropbox_size, output_path, state)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (dropbox_path, dropbox_rev, dropbox_size, output_path, state.value),
                )
                job_id = cursor.lastrowid
            except sqlite3.IntegrityError:
                # Job already exists, fetch it
                cursor = conn.execute(
                    "SELECT * FROM jobs WHERE dropbox_path = ? AND dropbox_rev = ?",
                    (dropbox_path, dropbox_rev),
                )
                row = cursor.fetchone()
                return Job.from_row(row) if row else None

            # Fetch the created job
            cursor = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
            row = cursor.fetchone()
            return Job.from_row(row) if row else None

    def get_job(self, job_id: int) -> Job | None:
        """Get job by ID."""
        conn = self._get_connection()
        cursor = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
        row = cursor.fetchone()
        return Job.from_row(row) if row else None

    def get_job_by_path(self, dropbox_path: str) -> Job | None:
        """Get most recent job for a Dropbox path."""
        conn = self._get_connection()
        cursor = conn.execute(
            "SELECT * FROM jobs WHERE dropbox_path = ? ORDER BY created_at DESC LIMIT 1",
            (dropbox_path,),
        )
        row = cursor.fetchone()
        return Job.from_row(row) if row else None

    def get_jobs_by_state(self, state: JobState, limit: int = 100) -> list[Job]:
        """Get jobs in a specific state."""
        conn = self._get_connection()
        cursor = conn.execute(
            "SELECT * FROM jobs WHERE state = ? ORDER BY created_at ASC LIMIT ?",
            (state.value, limit),
        )
        return [Job.from_row(row) for row in cursor.fetchall()]

    def get_jobs_by_states(self, states: set[JobState], limit: int = 100) -> list[Job]:
        """Get jobs in multiple states."""
        conn = self._get_connection()
        placeholders = ','.join('?' * len(states))
        cursor = conn.execute(
            f"SELECT * FROM jobs WHERE state IN ({placeholders}) ORDER BY created_at ASC LIMIT ?",
            [s.value for s in states] + [limit],
        )
        return [Job.from_row(row) for row in cursor.fetchall()]

    def get_dispatchable_jobs(
        self,
        states: set[JobState],
        limit: int,
    ) -> list[Job]:
        """
        Get jobs eligible for dispatch into a worker queue.

        Ordered FIFO by created_at to ensure first-discovered files are processed
        first across long-running scans.
        """
        if not states or limit <= 0:
            return []
        conn = self._get_connection()
        placeholders = ','.join('?' * len(states))
        cursor = conn.execute(
            f"SELECT * FROM jobs WHERE state IN ({placeholders}) "
            f"ORDER BY created_at ASC LIMIT ?",
            [s.value for s in states] + [limit],
        )
        return [Job.from_row(row) for row in cursor.fetchall()]

    def update_job_state(
        self,
        job_id: int,
        state: JobState,
        error_message: str | None = None,
        **kwargs: Any,
    ) -> bool:
        """
        Update job state and optional fields.

        Args:
            job_id: Job ID to update.
            state: New state.
            error_message: Optional error message.
            **kwargs: Additional fields to update.

        Returns:
            True if job was updated.
        """
        fields = ['state = ?']
        values: list[Any] = [state.value]

        if error_message is not None:
            fields.append('error_message = ?')
            values.append(error_message)

        # Handle additional fields
        for key, value in kwargs.items():
            if key in (
                'local_input_path', 'local_output_path', 'input_codec', 'output_codec',
                'input_duration_sec', 'output_duration_sec', 'input_bitrate_kbps',
                'output_bitrate_kbps', 'encoder_used', 'transcode_start', 'transcode_end',
                'retry_count', 'dropbox_rev',
            ):
                fields.append(f'{key} = ?')
                if isinstance(value, datetime):
                    values.append(value.isoformat())
                else:
                    values.append(value)

        values.append(job_id)

        with self.transaction() as conn:
            cursor = conn.execute(
                f"UPDATE jobs SET {', '.join(fields)} WHERE id = ?",
                values,
            )
            return cursor.rowcount > 0

    def increment_retry(self, job_id: int, max_retries: int) -> tuple[int, bool]:
        """
        Increment retry count and check if max reached.

        Returns:
            Tuple of (new_retry_count, should_fail_permanently).
        """
        with self.transaction() as conn:
            cursor = conn.execute(
                "UPDATE jobs SET retry_count = retry_count + 1 WHERE id = ?",
                (job_id,),
            )
            if cursor.rowcount == 0:
                return (0, True)

            cursor = conn.execute("SELECT retry_count FROM jobs WHERE id = ?", (job_id,))
            row = cursor.fetchone()
            retry_count = row['retry_count'] if row else 0
            return (retry_count, retry_count >= max_retries)

    def recover_active_jobs(self) -> int:
        """
        Move active jobs to RETRY_WAIT state after restart.

        Returns:
            Number of jobs recovered.
        """
        with self.transaction() as conn:
            placeholders = ','.join('?' * len(ACTIVE_STATES))
            cursor = conn.execute(
                f"UPDATE jobs SET state = ? WHERE state IN ({placeholders})",
                [JobState.RETRY_WAIT.value] + [s.value for s in ACTIVE_STATES],
            )
            return cursor.rowcount

    def reset_failed_jobs(self) -> int:
        """
        Reset all FAILED jobs to RETRY_WAIT.

        Returns:
            Number of jobs reset.
        """
        with self.transaction() as conn:
            cursor = conn.execute(
                "UPDATE jobs SET state = ?, retry_count = 0 WHERE state = ?",
                (JobState.RETRY_WAIT.value, JobState.FAILED.value),
            )
            return cursor.rowcount

    def check_output_exists(self, output_path: str) -> bool:
        """Check if a DONE job exists for the output path."""
        conn = self._get_connection()
        cursor = conn.execute(
            "SELECT 1 FROM jobs WHERE output_path = ? AND state = ? LIMIT 1",
            (output_path, JobState.DONE.value),
        )
        return cursor.fetchone() is not None

    # =========================================================================
    # Stability Check Operations (R2)
    # =========================================================================

    def add_stability_check(
        self,
        dropbox_path: str,
        size: int,
        rev: str,
        server_modified: str,
        content_hash: str | None = None,
    ) -> int:
        """
        Add a stability check record.

        Returns:
            ID of the new record.
        """
        with self.transaction() as conn:
            cursor = conn.execute(
                """
                INSERT INTO stability_checks
                    (dropbox_path, size, rev, server_modified, content_hash)
                VALUES (?, ?, ?, ?, ?)
                """,
                (dropbox_path, size, rev, server_modified, content_hash),
            )
            return cursor.lastrowid or 0

    def get_recent_stability_checks(
        self,
        dropbox_path: str,
        limit: int = 3,
    ) -> list[StabilityCheck]:
        """Get most recent stability checks for a file."""
        conn = self._get_connection()
        cursor = conn.execute(
            """
            SELECT * FROM stability_checks
            WHERE dropbox_path = ?
            ORDER BY check_time DESC
            LIMIT ?
            """,
            (dropbox_path, limit),
        )
        rows = cursor.fetchall()
        return [
            StabilityCheck(
                id=row['id'],
                dropbox_path=row['dropbox_path'],
                check_time=_parse_datetime(row['check_time']) or datetime.now(timezone.utc),
                size=row['size'],
                rev=row['rev'],
                server_modified=row['server_modified'],
                content_hash=row['content_hash'],
            )
            for row in rows
        ]

    def clear_stability_checks(self, dropbox_path: str) -> int:
        """Clear stability checks for a file (when it changes)."""
        with self.transaction() as conn:
            cursor = conn.execute(
                "DELETE FROM stability_checks WHERE dropbox_path = ?",
                (dropbox_path,),
            )
            return cursor.rowcount

    def cleanup_old_stability_checks(self, days: int = 7) -> int:
        """Remove old stability checks."""
        with self.transaction() as conn:
            cursor = conn.execute(
                """
                DELETE FROM stability_checks
                WHERE check_time < datetime('now', ? || ' days')
                """,
                (-days,),
            )
            return cursor.rowcount

    # =========================================================================
    # Metrics Operations
    # =========================================================================

    def record_metric(self, name: str, value: float) -> None:
        """Record a metric value."""
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO metrics (metric_name, metric_value) VALUES (?, ?)",
                (name, value),
            )

    def get_stats(self) -> dict[str, Any]:
        """Get aggregated statistics."""
        conn = self._get_connection()

        # Job counts by state
        cursor = conn.execute(
            "SELECT state, COUNT(*) as count FROM jobs GROUP BY state"
        )
        state_counts = {row['state']: row['count'] for row in cursor.fetchall()}

        # Total bytes processed
        cursor = conn.execute(
            "SELECT SUM(dropbox_size) as total FROM jobs WHERE state = ?",
            (JobState.DONE.value,),
        )
        row = cursor.fetchone()
        total_bytes_done = row['total'] if row and row['total'] else 0

        # Average transcode time
        cursor = conn.execute(
            """
            SELECT AVG(
                julianday(transcode_end) - julianday(transcode_start)
            ) * 86400 as avg_seconds
            FROM jobs
            WHERE state = ? AND transcode_start IS NOT NULL AND transcode_end IS NOT NULL
            """,
            (JobState.DONE.value,),
        )
        row = cursor.fetchone()
        avg_transcode_sec = row['avg_seconds'] if row and row['avg_seconds'] else 0

        return {
            'state_counts': state_counts,
            'total_bytes_done': total_bytes_done,
            'avg_transcode_seconds': avg_transcode_sec,
            'total_jobs': sum(state_counts.values()),
        }

    # =========================================================================
    # Settings Operations
    # =========================================================================

    def get_setting(self, key: str, default: str | None = None) -> str | None:
        """Get a setting value."""
        conn = self._get_connection()
        cursor = conn.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = cursor.fetchone()
        return row['value'] if row else default

    def set_setting(self, key: str, value: str) -> None:
        """Set a setting value."""
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO settings (key, value, updated_at)
                VALUES (?, ?, datetime('now'))
                ON CONFLICT(key) DO UPDATE SET value = ?, updated_at = datetime('now')
                """,
                (key, value, value),
            )

    # =========================================================================
    # Scan State (cursor persistence for incremental Dropbox scans)
    # =========================================================================

    def get_scan_state(self, dropbox_root: str) -> ScanState:
        """
        Fetch the singleton scan-state row, creating it on first access.

        If the configured dropbox_root changed, the cursor is invalidated
        (different root = different cursor namespace in Dropbox).
        """
        conn = self._get_connection()
        cursor = conn.execute("SELECT * FROM scan_state WHERE id = 1")
        row = cursor.fetchone()

        if row is None:
            with self.transaction() as txn:
                txn.execute(
                    "INSERT INTO scan_state (id, dropbox_root) VALUES (1, ?)",
                    (dropbox_root,),
                )
            return ScanState(
                dropbox_root=dropbox_root,
                cursor=None,
                bulk_pass_complete=False,
                bulk_started_at=None,
                bulk_completed_at=None,
                last_delta_at=None,
                entries_seen=0,
            )

        if row['dropbox_root'] != dropbox_root:
            # Root changed — drop the cursor so we rescan under the new root
            with self.transaction() as txn:
                txn.execute(
                    """
                    UPDATE scan_state
                       SET dropbox_root = ?,
                           cursor = NULL,
                           bulk_pass_complete = 0,
                           bulk_started_at = NULL,
                           bulk_completed_at = NULL,
                           last_delta_at = NULL,
                           entries_seen = 0,
                           updated_at = datetime('now')
                     WHERE id = 1
                    """,
                    (dropbox_root,),
                )
            return ScanState(
                dropbox_root=dropbox_root,
                cursor=None,
                bulk_pass_complete=False,
                bulk_started_at=None,
                bulk_completed_at=None,
                last_delta_at=None,
                entries_seen=0,
            )

        return ScanState(
            dropbox_root=row['dropbox_root'],
            cursor=row['cursor'],
            bulk_pass_complete=bool(row['bulk_pass_complete']),
            bulk_started_at=_parse_datetime(row['bulk_started_at']),
            bulk_completed_at=_parse_datetime(row['bulk_completed_at']),
            last_delta_at=_parse_datetime(row['last_delta_at']),
            entries_seen=row['entries_seen'] or 0,
        )

    def save_scan_cursor(self, cursor: str, entries_seen: int) -> None:
        """Persist the Dropbox list_folder cursor after a page of results."""
        with self.transaction() as conn:
            conn.execute(
                """
                UPDATE scan_state
                   SET cursor = ?,
                       entries_seen = ?,
                       updated_at = datetime('now')
                 WHERE id = 1
                """,
                (cursor, entries_seen),
            )

    def mark_bulk_started(self) -> None:
        with self.transaction() as conn:
            conn.execute(
                """
                UPDATE scan_state
                   SET bulk_started_at = COALESCE(bulk_started_at, datetime('now')),
                       updated_at = datetime('now')
                 WHERE id = 1
                """
            )

    def mark_bulk_complete(self) -> None:
        with self.transaction() as conn:
            conn.execute(
                """
                UPDATE scan_state
                   SET bulk_pass_complete = 1,
                       bulk_completed_at = datetime('now'),
                       updated_at = datetime('now')
                 WHERE id = 1
                """
            )

    def mark_delta_pass(self) -> None:
        with self.transaction() as conn:
            conn.execute(
                """
                UPDATE scan_state
                   SET last_delta_at = datetime('now'),
                       updated_at = datetime('now')
                 WHERE id = 1
                """
            )

    # =========================================================================
    # feito.txt cache (avoid re-reading logs on every scan)
    # =========================================================================

    def get_feito_cache(
        self,
        folder_path: str,
        ttl_sec: int,
    ) -> set[str] | None:
        """
        Return the cached filenames set if present and not expired, else None.
        """
        conn = self._get_connection()
        cursor = conn.execute(
            """
            SELECT filenames, last_checked
              FROM feito_cache
             WHERE folder_path = ?
               AND last_checked > datetime('now', ? || ' seconds')
            """,
            (folder_path, -ttl_sec),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return set(filter(None, (row['filenames'] or '').split('\n')))

    def put_feito_cache(self, folder_path: str, filenames: set[str]) -> None:
        joined = '\n'.join(sorted(filenames))
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO feito_cache (folder_path, filenames, last_checked)
                VALUES (?, ?, datetime('now'))
                ON CONFLICT(folder_path) DO UPDATE
                    SET filenames = excluded.filenames,
                        last_checked = datetime('now')
                """,
                (folder_path, joined),
            )

    def invalidate_feito_cache(self, folder_path: str) -> None:
        with self.transaction() as conn:
            conn.execute(
                "DELETE FROM feito_cache WHERE folder_path = ?",
                (folder_path,),
            )

    # =========================================================================
    # Disk reservations (staging-dir budget)
    # =========================================================================

    def reserve_disk(self, job_id: int, size_bytes: int) -> None:
        with self.transaction() as conn:
            conn.execute(
                """
                INSERT INTO disk_reservations (job_id, reserved_bytes)
                VALUES (?, ?)
                ON CONFLICT(job_id) DO UPDATE SET reserved_bytes = excluded.reserved_bytes,
                                                   reserved_at = datetime('now')
                """,
                (job_id, size_bytes),
            )

    def release_disk(self, job_id: int) -> None:
        with self.transaction() as conn:
            conn.execute(
                "DELETE FROM disk_reservations WHERE job_id = ?",
                (job_id,),
            )

    def total_reserved_bytes(self) -> int:
        conn = self._get_connection()
        cursor = conn.execute("SELECT COALESCE(SUM(reserved_bytes), 0) AS s FROM disk_reservations")
        row = cursor.fetchone()
        return int(row['s']) if row else 0

    def prune_stale_disk_reservations(self, active_states: set[JobState]) -> int:
        """
        Drop reservations whose jobs are no longer in an active state.
        Called at startup after recover_active_jobs.
        """
        conn = self._get_connection()
        placeholders = ','.join('?' * len(active_states))
        with self.transaction() as txn:
            cursor = txn.execute(
                f"""
                DELETE FROM disk_reservations
                 WHERE job_id NOT IN (
                     SELECT id FROM jobs WHERE state IN ({placeholders})
                 )
                """,
                [s.value for s in active_states],
            )
            return cursor.rowcount

    # =========================================================================
    # Queue Operations
    # =========================================================================

    def list_queue(
        self,
        states: set[JobState] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Job]:
        """List jobs in queue with optional state filter."""
        conn = self._get_connection()

        if states:
            placeholders = ','.join('?' * len(states))
            cursor = conn.execute(
                f"""
                SELECT * FROM jobs
                WHERE state IN ({placeholders})
                ORDER BY created_at DESC
                LIMIT ? OFFSET ?
                """,
                [s.value for s in states] + [limit, offset],
            )
        else:
            cursor = conn.execute(
                "SELECT * FROM jobs ORDER BY created_at DESC LIMIT ? OFFSET ?",
                (limit, offset),
            )

        return [Job.from_row(row) for row in cursor.fetchall()]

    def count_jobs(self, state: JobState | None = None) -> int:
        """Count jobs, optionally filtered by state."""
        conn = self._get_connection()
        if state:
            cursor = conn.execute(
                "SELECT COUNT(*) as count FROM jobs WHERE state = ?",
                (state.value,),
            )
        else:
            cursor = conn.execute("SELECT COUNT(*) as count FROM jobs")
        row = cursor.fetchone()
        return row['count'] if row else 0
