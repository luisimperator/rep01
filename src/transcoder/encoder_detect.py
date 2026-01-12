"""
Hardware encoder detection for FFmpeg.

Detects available HEVC encoders: Intel QuickSync (QSV), NVIDIA NVENC, and CPU fallback.
"""

from __future__ import annotations

import logging
import re
import subprocess
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .config import Config

logger = logging.getLogger(__name__)


class EncoderType(str, Enum):
    """Available encoder types."""
    QSV = "hevc_qsv"
    NVENC = "hevc_nvenc"
    CPU = "libx265"


@dataclass
class EncoderInfo:
    """Information about an available encoder."""
    encoder_type: EncoderType
    name: str
    description: str
    available: bool
    hardware: bool
    priority: int  # Lower is higher priority

    @property
    def ffmpeg_codec(self) -> str:
        """Get the FFmpeg codec name."""
        return self.encoder_type.value


# Encoder definitions with priorities
ENCODER_DEFINITIONS = {
    EncoderType.QSV: EncoderInfo(
        encoder_type=EncoderType.QSV,
        name="Intel QuickSync",
        description="Intel hardware acceleration (QSV)",
        available=False,
        hardware=True,
        priority=1,
    ),
    EncoderType.NVENC: EncoderInfo(
        encoder_type=EncoderType.NVENC,
        name="NVIDIA NVENC",
        description="NVIDIA hardware acceleration",
        available=False,
        hardware=True,
        priority=2,
    ),
    EncoderType.CPU: EncoderInfo(
        encoder_type=EncoderType.CPU,
        name="CPU (libx265)",
        description="Software encoding with x265",
        available=False,
        hardware=False,
        priority=3,
    ),
}


def detect_available_encoders(ffmpeg_path: str = "ffmpeg") -> dict[EncoderType, EncoderInfo]:
    """
    Detect which HEVC encoders are available.

    Args:
        ffmpeg_path: Path to ffmpeg binary.

    Returns:
        Dictionary mapping encoder types to their info.
    """
    # Start with copies of the definitions
    encoders = {
        k: EncoderInfo(
            encoder_type=v.encoder_type,
            name=v.name,
            description=v.description,
            available=False,
            hardware=v.hardware,
            priority=v.priority,
        )
        for k, v in ENCODER_DEFINITIONS.items()
    }

    try:
        result = subprocess.run(
            [ffmpeg_path, "-hide_banner", "-encoders"],
            capture_output=True,
            text=True,
            timeout=30,
        )

        output = result.stdout + result.stderr

        # Check for each encoder
        if re.search(r'hevc_qsv', output):
            encoders[EncoderType.QSV].available = True
            logger.info("Detected Intel QuickSync (hevc_qsv)")

        if re.search(r'hevc_nvenc', output):
            encoders[EncoderType.NVENC].available = True
            logger.info("Detected NVIDIA NVENC (hevc_nvenc)")

        if re.search(r'libx265', output):
            encoders[EncoderType.CPU].available = True
            logger.info("Detected CPU encoder (libx265)")

    except subprocess.TimeoutExpired:
        logger.error("Timeout detecting encoders")
    except FileNotFoundError:
        logger.error(f"FFmpeg not found at: {ffmpeg_path}")
    except Exception as e:
        logger.error(f"Error detecting encoders: {e}")

    return encoders


def verify_encoder_works(
    encoder_type: EncoderType,
    ffmpeg_path: str = "ffmpeg",
) -> bool:
    """
    Verify that an encoder actually works by running a test encode.

    Args:
        encoder_type: Encoder to test.
        ffmpeg_path: Path to ffmpeg binary.

    Returns:
        True if encoder works.
    """
    try:
        # Generate a tiny test pattern and try to encode it
        cmd = [
            ffmpeg_path,
            "-hide_banner",
            "-f", "lavfi",
            "-i", "testsrc=duration=1:size=64x64:rate=1",
            "-c:v", encoder_type.value,
            "-frames:v", "1",
            "-f", "null",
            "-",
        ]

        # Add hardware-specific options
        if encoder_type == EncoderType.QSV:
            cmd.insert(3, "-hwaccel")
            cmd.insert(4, "qsv")
        elif encoder_type == EncoderType.NVENC:
            # NVENC doesn't need special hwaccel for encoding
            pass

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60,
        )

        if result.returncode == 0:
            logger.info(f"Encoder {encoder_type.value} verified working")
            return True
        else:
            logger.warning(f"Encoder {encoder_type.value} failed verification: {result.stderr}")
            return False

    except subprocess.TimeoutExpired:
        logger.warning(f"Encoder {encoder_type.value} verification timed out")
        return False
    except Exception as e:
        logger.warning(f"Encoder {encoder_type.value} verification error: {e}")
        return False


def select_best_encoder(
    config: Config,
    available_encoders: dict[EncoderType, EncoderInfo] | None = None,
    verify: bool = True,
) -> EncoderType:
    """
    Select the best available encoder based on config preference.

    Args:
        config: Application config.
        available_encoders: Pre-detected encoders (optional).
        verify: If True, verify encoder works before selecting.

    Returns:
        Selected encoder type.

    Raises:
        RuntimeError: If no suitable encoder is available.
    """
    from .config import EncoderPreference

    if available_encoders is None:
        available_encoders = detect_available_encoders(config.ffmpeg_path)

    preference = config.encoder_preference

    # If specific encoder is requested
    if preference == EncoderPreference.QSV:
        if available_encoders[EncoderType.QSV].available:
            if not verify or verify_encoder_works(EncoderType.QSV, config.ffmpeg_path):
                return EncoderType.QSV
        raise RuntimeError("Intel QuickSync (QSV) encoder not available or not working")

    elif preference == EncoderPreference.NVENC:
        if available_encoders[EncoderType.NVENC].available:
            if not verify or verify_encoder_works(EncoderType.NVENC, config.ffmpeg_path):
                return EncoderType.NVENC
        raise RuntimeError("NVIDIA NVENC encoder not available or not working")

    elif preference == EncoderPreference.CPU:
        if available_encoders[EncoderType.CPU].available:
            return EncoderType.CPU
        raise RuntimeError("CPU encoder (libx265) not available")

    # Auto selection: try in priority order
    candidates = sorted(
        [e for e in available_encoders.values() if e.available],
        key=lambda x: x.priority,
    )

    for encoder_info in candidates:
        if encoder_info.hardware:
            if verify and not verify_encoder_works(encoder_info.encoder_type, config.ffmpeg_path):
                logger.warning(f"Skipping {encoder_info.name}: verification failed")
                continue
        logger.info(f"Selected encoder: {encoder_info.name}")
        return encoder_info.encoder_type

    raise RuntimeError("No suitable HEVC encoder available")


def get_encoder_info_string(ffmpeg_path: str = "ffmpeg") -> str:
    """Get a human-readable string describing available encoders."""
    encoders = detect_available_encoders(ffmpeg_path)

    lines = ["Available HEVC encoders:"]
    for encoder_type in [EncoderType.QSV, EncoderType.NVENC, EncoderType.CPU]:
        info = encoders[encoder_type]
        status = "✓ Available" if info.available else "✗ Not available"
        lines.append(f"  {info.name} ({info.ffmpeg_codec}): {status}")

    return "\n".join(lines)
