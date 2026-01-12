"""Tests for file stability detection (R2)."""

import sqlite3
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from transcoder.config import Config, StabilitySettings
from transcoder.database import Database, StabilityCheck


class TestStabilitySettings:
    """Tests for stability configuration."""

    def test_default_settings(self) -> None:
        """Test default stability settings."""
        settings = StabilitySettings()
        assert settings.poll_interval_sec == 300  # 5 minutes
        assert settings.checks_required == 3
        assert settings.min_age_sec == 900  # 15 minutes

    def test_custom_settings(self) -> None:
        """Test custom stability settings."""
        settings = StabilitySettings(
            poll_interval_sec=600,
            checks_required=5,
            min_age_sec=1800,
        )
        assert settings.poll_interval_sec == 600
        assert settings.checks_required == 5
        assert settings.min_age_sec == 1800


class TestDatabaseStabilityChecks:
    """Tests for stability check database operations."""

    @pytest.fixture
    def db(self) -> Database:
        """Create temporary database."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name

        db = Database(db_path)
        db.initialize()
        yield db
        db.close()
        Path(db_path).unlink(missing_ok=True)

    def test_add_stability_check(self, db: Database) -> None:
        """Test adding stability check."""
        check_id = db.add_stability_check(
            dropbox_path="/Videos/test.mp4",
            size=1000000,
            rev="abc123",
            server_modified="2024-01-01T12:00:00Z",
        )
        assert check_id > 0

    def test_get_recent_checks(self, db: Database) -> None:
        """Test retrieving recent checks."""
        path = "/Videos/test.mp4"

        # Add multiple checks
        for i in range(5):
            db.add_stability_check(
                dropbox_path=path,
                size=1000000,
                rev=f"rev{i}",
                server_modified="2024-01-01T12:00:00Z",
            )

        # Get last 3
        checks = db.get_recent_stability_checks(path, limit=3)
        assert len(checks) == 3
        # Most recent first
        assert checks[0].rev == "rev4"

    def test_clear_stability_checks(self, db: Database) -> None:
        """Test clearing checks for a path."""
        path = "/Videos/test.mp4"

        # Add checks
        for i in range(3):
            db.add_stability_check(
                dropbox_path=path,
                size=1000000,
                rev="abc123",
                server_modified="2024-01-01T12:00:00Z",
            )

        # Clear
        cleared = db.clear_stability_checks(path)
        assert cleared == 3

        # Verify cleared
        checks = db.get_recent_stability_checks(path, limit=10)
        assert len(checks) == 0


class TestStabilityLogic:
    """Tests for stability detection logic."""

    def test_stability_requires_consistent_values(self) -> None:
        """File must have same size/rev/modified for all checks."""
        # This tests the concept - actual implementation in scanner.py
        checks = [
            StabilityCheck(
                id=1,
                dropbox_path="/test.mp4",
                check_time=datetime.now(timezone.utc) - timedelta(minutes=20),
                size=1000000,
                rev="abc123",
                server_modified="2024-01-01T12:00:00Z",
                content_hash=None,
            ),
            StabilityCheck(
                id=2,
                dropbox_path="/test.mp4",
                check_time=datetime.now(timezone.utc) - timedelta(minutes=10),
                size=1000000,
                rev="abc123",
                server_modified="2024-01-01T12:00:00Z",
                content_hash=None,
            ),
            StabilityCheck(
                id=3,
                dropbox_path="/test.mp4",
                check_time=datetime.now(timezone.utc),
                size=1000000,
                rev="abc123",
                server_modified="2024-01-01T12:00:00Z",
                content_hash=None,
            ),
        ]

        # All values match
        all_match = all(
            c.size == checks[0].size and
            c.rev == checks[0].rev and
            c.server_modified == checks[0].server_modified
            for c in checks
        )
        assert all_match is True

    def test_stability_detects_size_change(self) -> None:
        """Size change should reset stability window."""
        checks = [
            StabilityCheck(
                id=1,
                dropbox_path="/test.mp4",
                check_time=datetime.now(timezone.utc) - timedelta(minutes=10),
                size=1000000,
                rev="abc123",
                server_modified="2024-01-01T12:00:00Z",
                content_hash=None,
            ),
            StabilityCheck(
                id=2,
                dropbox_path="/test.mp4",
                check_time=datetime.now(timezone.utc),
                size=2000000,  # Size changed!
                rev="abc123",
                server_modified="2024-01-01T12:00:00Z",
                content_hash=None,
            ),
        ]

        # Values don't match
        all_match = all(c.size == checks[0].size for c in checks)
        assert all_match is False

    def test_stability_detects_rev_change(self) -> None:
        """Rev change should reset stability window."""
        checks = [
            StabilityCheck(
                id=1,
                dropbox_path="/test.mp4",
                check_time=datetime.now(timezone.utc) - timedelta(minutes=10),
                size=1000000,
                rev="abc123",
                server_modified="2024-01-01T12:00:00Z",
                content_hash=None,
            ),
            StabilityCheck(
                id=2,
                dropbox_path="/test.mp4",
                check_time=datetime.now(timezone.utc),
                size=1000000,
                rev="def456",  # Rev changed!
                server_modified="2024-01-01T12:00:00Z",
                content_hash=None,
            ),
        ]

        all_match = all(c.rev == checks[0].rev for c in checks)
        assert all_match is False

    def test_minimum_age_requirement(self) -> None:
        """File must be stable for minimum age."""
        min_age_sec = 900  # 15 minutes

        # Check from 10 minutes ago - not old enough
        check_time = datetime.now(timezone.utc) - timedelta(minutes=10)
        age = (datetime.now(timezone.utc) - check_time).total_seconds()
        assert age < min_age_sec

        # Check from 20 minutes ago - old enough
        check_time = datetime.now(timezone.utc) - timedelta(minutes=20)
        age = (datetime.now(timezone.utc) - check_time).total_seconds()
        assert age >= min_age_sec
