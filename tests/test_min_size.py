"""Tests for minimum size filter (R5)."""

import pytest

from transcoder.config import Config


class TestMinSizeFilter:
    """Tests for minimum file size filtering."""

    def test_default_min_size_gb(self) -> None:
        """Default min size should be 6 GB."""
        config = Config()
        assert config.min_size_gb == 6.0

    def test_min_size_bytes_calculation(self) -> None:
        """Test bytes calculation from GB."""
        config = Config(min_size_gb=6.0)
        expected = 6 * 1024 * 1024 * 1024  # 6 GB in bytes
        assert config.min_size_bytes() == expected

    def test_min_size_bytes_custom(self) -> None:
        """Test custom size values."""
        config = Config(min_size_gb=10.0)
        expected = 10 * 1024 * 1024 * 1024
        assert config.min_size_bytes() == expected

    def test_min_size_bytes_zero(self) -> None:
        """Test zero min size (process all files)."""
        config = Config(min_size_gb=0.0)
        assert config.min_size_bytes() == 0

    def test_min_size_bytes_fractional(self) -> None:
        """Test fractional GB values."""
        config = Config(min_size_gb=0.5)
        expected = int(0.5 * 1024 * 1024 * 1024)
        assert config.min_size_bytes() == expected


class TestSizeFiltering:
    """Tests for size-based filtering logic."""

    def test_file_below_threshold(self) -> None:
        """Files below threshold should be filtered."""
        config = Config(min_size_gb=6.0)
        min_bytes = config.min_size_bytes()

        # 5 GB file should be filtered
        file_size = 5 * 1024 * 1024 * 1024
        assert file_size < min_bytes

    def test_file_at_threshold(self) -> None:
        """Files at exact threshold should NOT be filtered."""
        config = Config(min_size_gb=6.0)
        min_bytes = config.min_size_bytes()

        # 6 GB file should pass
        file_size = 6 * 1024 * 1024 * 1024
        assert file_size >= min_bytes

    def test_file_above_threshold(self) -> None:
        """Files above threshold should pass."""
        config = Config(min_size_gb=6.0)
        min_bytes = config.min_size_bytes()

        # 10 GB file should pass
        file_size = 10 * 1024 * 1024 * 1024
        assert file_size >= min_bytes

    def test_large_file(self) -> None:
        """Very large files should pass."""
        config = Config(min_size_gb=6.0)
        min_bytes = config.min_size_bytes()

        # 500 GB file
        file_size = 500 * 1024 * 1024 * 1024
        assert file_size >= min_bytes
