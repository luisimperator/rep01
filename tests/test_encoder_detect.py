"""Tests for hardware encoder detection."""

from unittest.mock import MagicMock, patch

import pytest

from transcoder.encoder_detect import (
    ENCODER_DEFINITIONS,
    EncoderInfo,
    EncoderType,
    detect_available_encoders,
)


class TestEncoderType:
    """Tests for encoder type enum."""

    def test_encoder_values(self) -> None:
        """Test FFmpeg codec names."""
        assert EncoderType.QSV.value == "hevc_qsv"
        assert EncoderType.NVENC.value == "hevc_nvenc"
        assert EncoderType.CPU.value == "libx265"


class TestEncoderDefinitions:
    """Tests for encoder definitions."""

    def test_all_encoders_defined(self) -> None:
        """All encoder types should have definitions."""
        assert EncoderType.QSV in ENCODER_DEFINITIONS
        assert EncoderType.NVENC in ENCODER_DEFINITIONS
        assert EncoderType.CPU in ENCODER_DEFINITIONS

    def test_qsv_is_hardware(self) -> None:
        """QSV should be marked as hardware."""
        assert ENCODER_DEFINITIONS[EncoderType.QSV].hardware is True

    def test_nvenc_is_hardware(self) -> None:
        """NVENC should be marked as hardware."""
        assert ENCODER_DEFINITIONS[EncoderType.NVENC].hardware is True

    def test_cpu_is_software(self) -> None:
        """CPU encoder should not be hardware."""
        assert ENCODER_DEFINITIONS[EncoderType.CPU].hardware is False

    def test_priority_order(self) -> None:
        """QSV should have highest priority, then NVENC, then CPU."""
        qsv_priority = ENCODER_DEFINITIONS[EncoderType.QSV].priority
        nvenc_priority = ENCODER_DEFINITIONS[EncoderType.NVENC].priority
        cpu_priority = ENCODER_DEFINITIONS[EncoderType.CPU].priority

        assert qsv_priority < nvenc_priority  # Lower is better
        assert nvenc_priority < cpu_priority


class TestDetectAvailableEncoders:
    """Tests for encoder detection."""

    @patch('subprocess.run')
    def test_detects_qsv(self, mock_run: MagicMock) -> None:
        """Should detect QSV encoder."""
        mock_run.return_value = MagicMock(
            stdout="""Encoders:
 V..... hevc_qsv             HEVC (Intel Quick Sync Video acceleration)
 V..... libx265              libx265 H.265 / HEVC
""",
            stderr="",
            returncode=0,
        )

        encoders = detect_available_encoders("ffmpeg")

        assert encoders[EncoderType.QSV].available is True
        assert encoders[EncoderType.CPU].available is True
        assert encoders[EncoderType.NVENC].available is False

    @patch('subprocess.run')
    def test_detects_nvenc(self, mock_run: MagicMock) -> None:
        """Should detect NVENC encoder."""
        mock_run.return_value = MagicMock(
            stdout="""Encoders:
 V..... hevc_nvenc           NVIDIA NVENC hevc encoder
 V..... libx265              libx265 H.265 / HEVC
""",
            stderr="",
            returncode=0,
        )

        encoders = detect_available_encoders("ffmpeg")

        assert encoders[EncoderType.NVENC].available is True
        assert encoders[EncoderType.CPU].available is True
        assert encoders[EncoderType.QSV].available is False

    @patch('subprocess.run')
    def test_detects_all_encoders(self, mock_run: MagicMock) -> None:
        """Should detect all encoders when available."""
        mock_run.return_value = MagicMock(
            stdout="""Encoders:
 V..... hevc_qsv             HEVC (Intel Quick Sync Video acceleration)
 V..... hevc_nvenc           NVIDIA NVENC hevc encoder
 V..... libx265              libx265 H.265 / HEVC
""",
            stderr="",
            returncode=0,
        )

        encoders = detect_available_encoders("ffmpeg")

        assert encoders[EncoderType.QSV].available is True
        assert encoders[EncoderType.NVENC].available is True
        assert encoders[EncoderType.CPU].available is True

    @patch('subprocess.run')
    def test_detects_cpu_only(self, mock_run: MagicMock) -> None:
        """Should handle CPU-only systems."""
        mock_run.return_value = MagicMock(
            stdout="""Encoders:
 V..... libx265              libx265 H.265 / HEVC
 V..... libx264              libx264 H.264 / AVC
""",
            stderr="",
            returncode=0,
        )

        encoders = detect_available_encoders("ffmpeg")

        assert encoders[EncoderType.QSV].available is False
        assert encoders[EncoderType.NVENC].available is False
        assert encoders[EncoderType.CPU].available is True

    @patch('subprocess.run')
    def test_handles_ffmpeg_not_found(self, mock_run: MagicMock) -> None:
        """Should handle missing FFmpeg gracefully."""
        mock_run.side_effect = FileNotFoundError("ffmpeg not found")

        encoders = detect_available_encoders("ffmpeg")

        # All should be unavailable
        assert encoders[EncoderType.QSV].available is False
        assert encoders[EncoderType.NVENC].available is False
        assert encoders[EncoderType.CPU].available is False

    @patch('subprocess.run')
    def test_handles_timeout(self, mock_run: MagicMock) -> None:
        """Should handle timeout gracefully."""
        import subprocess
        mock_run.side_effect = subprocess.TimeoutExpired("ffmpeg", 30)

        encoders = detect_available_encoders("ffmpeg")

        # All should be unavailable
        assert encoders[EncoderType.QSV].available is False
        assert encoders[EncoderType.NVENC].available is False
        assert encoders[EncoderType.CPU].available is False
