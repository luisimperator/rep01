"""
FFmpeg command builder for HEVC transcoding.

Builds FFmpeg commands with proper settings for each encoder type and profile.
Ensures metadata preservation and proper output format.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from .encoder_detect import EncoderType

if TYPE_CHECKING:
    from .config import Config, TranscodeProfile

logger = logging.getLogger(__name__)


# Values ffmpeg's encoders accept for -color_primaries / -color_trc /
# -colorspace. Everything else (notably 'gbr', which ffprobe reports for
# RGB24 sources like PNGs in mov containers) gets dropped by
# _color_metadata_args because passing them produces the lovely:
#   [hevc_qsv] Unable to parse "colorspace" option value "gbr"
# and the encode never starts.
_VALID_COLOR_PRIMARIES = frozenset({
    "bt709", "bt470m", "bt470bg", "smpte170m", "smpte240m", "film",
    "bt2020", "smpte428", "smpte431", "smpte432", "ebu3213", "unknown",
})
_VALID_COLOR_TRC = frozenset({
    "bt709", "bt470m", "bt470bg", "smpte170m", "smpte240m", "linear",
    "log100", "log316", "iec61966-2-4", "bt1361e", "iec61966-2-1",
    "bt2020-10", "bt2020-12", "smpte2084", "smpte428", "arib-std-b67",
    "unknown",
})
_VALID_COLORSPACE = frozenset({
    "bt709", "fcc", "bt470bg", "smpte170m", "smpte240m", "ycgco",
    "bt2020nc", "bt2020c", "smpte2085", "chroma-derived-nc",
    "chroma-derived-c", "ictcp", "unknown",
})


@dataclass
class VideoInfo:
    """Information about input video from ffprobe."""
    codec_name: str
    width: int
    height: int
    fps: float
    duration_sec: float
    bitrate_kbps: int
    bit_depth: int = 8
    pix_fmt: str = "yuv420p"
    has_audio: bool = True
    has_subtitles: bool = False
    audio_codec: str | None = None
    # Timecode preserved from input (e.g. "14:39:32;17"). When present we
    # pass it as `-timecode` so Premiere/Resolve etc. read the same start
    # frame as the original H.264 — critical for projects relying on
    # timecode-based sync across cameras.
    timecode: str | None = None
    # Chroma subsampling — '420' | '422' | '444'. Derived from pix_fmt /
    # profile in prober. Drives the per-job encoder + output pix_fmt
    # choice when the user enables Preserve Chroma.
    chroma: str = "420"
    # Color metadata (None when ffprobe didn't report — e.g. Sony S-Log3
    # which writes none of these). Builder passes through whatever exists
    # without fabricating; for log/raw sources omitting is the correct
    # behavior so the NLE doesn't mis-interpret the curve.
    color_primaries: str | None = None
    color_transfer: str | None = None
    color_space: str | None = None
    color_range: str | None = None  # 'tv' | 'pc' | None


@dataclass
class FFmpegCommand:
    """Represents a complete FFmpeg command."""
    args: list[str]
    input_path: Path
    output_path: Path
    temp_output_path: Path
    encoder: EncoderType
    profile: str
    description: str
    expected_duration_sec: float

    def as_list(self) -> list[str]:
        """Return command as list for subprocess."""
        return self.args

    def as_string(self) -> str:
        """Return command as shell string (for logging)."""
        import shlex
        return shlex.join(self.args)


class FFmpegCommandBuilder:
    """
    Builder for FFmpeg transcode commands.

    Supports QSV, NVENC, and CPU encoders with balanced and quality profiles.
    Ensures metadata preservation per R6.
    """

    def __init__(self, config: Config):
        """
        Initialize builder with config.

        Args:
            config: Application configuration.
        """
        self.config = config

    def build_transcode_command(
        self,
        input_path: Path,
        output_path: Path,
        video_info: VideoInfo,
        encoder: EncoderType,
    ) -> FFmpegCommand:
        """
        Build FFmpeg transcode command.

        Args:
            input_path: Path to input video.
            output_path: Path for output video.
            video_info: Probed video information.
            encoder: Encoder to use.

        Returns:
            FFmpegCommand ready for execution.
        """
        from .config import TranscodeProfile

        profile = self.config.profile
        # Insert .tmp BEFORE the real extension instead of appending it.
        # ffmpeg infers the muxer from the output extension; an
        # 'output.mp4.tmp' filename leaves the muxer unknown and ffmpeg
        # bails with "Unable to choose an output format" before any
        # frames are processed (which surfaces as the silent -22 we
        # were chasing). 'output.tmp.mp4' parses cleanly.
        temp_output = output_path.with_name(
            output_path.stem + '.tmp' + output_path.suffix
        )

        # Start building command
        args: list[str] = [self.config.ffmpeg_path, "-hide_banner", "-y"]

        # Add hardware acceleration for input (if applicable). When the
        # source chroma is 4:2:2/4:4:4, the QSV / NVENC consumer hardware
        # decoders refuse the input (most Intel iGPUs only decode H.264
        # High 4:2:0; consumer NVENC same story). In that case we fall
        # back to software decode while keeping the QSV/NVENC encoder —
        # ~5-10x faster than pure libx265 because the encode itself is
        # still hardware. Detected upfront so we don't waste two failed
        # ffmpeg launches before the auto-fallback kicks in.
        args.extend(self._get_input_hwaccel(encoder, video_info))

        # Input file
        args.extend(["-i", str(input_path)])

        # Mapping: copy video + audio + subtitles, drop tmcd data streams
        # (MP4 muxer can't write them via copy and bails with "Could not
        # find tag for codec none"). The TIMECODE itself is preserved via
        # -timecode below — Premiere / Resolve / FCP read that as the
        # clip's start TC, so multi-cam sync from the original H.264
        # project keeps working on the H.265 replacement.
        args.extend([
            "-map", "0:v?",            # video tracks (none ok)
            "-map", "0:a?",            # audio tracks (none ok)
            "-map", "0:s?",            # subtitle tracks (none ok)
            "-map_metadata", "0",      # copy container-level metadata
            "-map_metadata:s:v", "0:s:v",   # copy video stream metadata
            "-map_metadata:s:a", "0:s:a",   # copy audio stream metadata
            "-dn",                     # drop data streams (tmcd, etc)
        ])
        if video_info.timecode:
            # ffmpeg's -timecode writes a tmcd track in MP4 output without
            # the "codec none" muxer issue, AND records it on the video
            # stream's metadata. Both are read by NLEs.
            args.extend(["-timecode", video_info.timecode])

        # Frame-perfect sync with the H.264 original (so the H.265 can
        # drop into a Premiere/Resolve project as a direct replacement
        # without re-syncing): keep input timestamps, don't drop or dup
        # frames, don't override the input fps.
        args.extend(["-fps_mode", "passthrough"])

        # Color metadata passthrough. Pass each tag explicitly when the
        # input declared it; omit when ffprobe returned None so we don't
        # mis-tag log/raw sources. Sony A7siii in S-Log3 is the canonical
        # case where color_primaries / color_transfer / color_space are
        # all absent — fabricating a tag would lie to the NLE.
        # Range is the one value we always set (defaulting to 'tv' for
        # broadcast/limited if the source didn't say) because most
        # encoders pick 'tv' silently anyway.
        args.extend(self._color_metadata_args(video_info))

        # Software chroma conversion when running QSV / NVENC in hybrid
        # mode (sw decode for 4:2:2/4:4:4 inputs the HW decoder rejects).
        # No-op for libx265 (pix_fmt covers it) and no-op when full HW
        # decode is in play.
        args.extend(self._get_video_filter_args(encoder, video_info))

        # Video encoder settings
        args.extend(self._get_video_encoder_args(encoder, profile, video_info))

        # Audio handling
        args.extend(self._get_audio_args(video_info))

        # Subtitle handling (copy if present)
        if video_info.has_subtitles:
            args.extend(["-c:s", "copy"])

        # GOP size
        args.extend(["-g", str(self.config.gop_size)])

        # Extra args from config
        args.extend(self.config.ffmpeg_extra_args)

        # Output file (temp first)
        args.append(str(temp_output))

        description = (
            f"{encoder.value} {profile.value} transcode: "
            f"{video_info.codec_name} -> hevc, "
            f"{video_info.width}x{video_info.height}, "
            f"{video_info.duration_sec:.1f}s"
        )

        return FFmpegCommand(
            args=args,
            input_path=input_path,
            output_path=output_path,
            temp_output_path=temp_output,
            encoder=encoder,
            profile=profile.value,
            description=description,
            expected_duration_sec=video_info.duration_sec,
        )

    def _hw_can_decode_input(self, video_info: VideoInfo | None) -> bool:
        """Heuristic: can the QSV / NVENC hardware decoder read this input?

        QSV: most Intel iGPUs decode H.264 / HEVC Main + Main10 4:2:0
        only. H.264 High 4:2:2 (the A7siii XAVC-S-I container) and
        anything 4:4:4 fail at decode time, so ffmpeg returns -22
        (Invalid argument) before any frame is encoded.

        NVENC consumer: similar restrictions on consumer GeForce — Pro
        cards (Quadro, A-series) handle 4:2:2 fine but we can't tell
        them apart at runtime, so be conservative.

        We treat 4:2:0 as the only safe input. Returns True for unknown
        video_info to preserve the original behavior on probe failure.
        """
        if video_info is None:
            return True
        chroma = video_info.chroma or "420"
        return chroma == "420"

    def _get_input_hwaccel(
        self,
        encoder: EncoderType,
        video_info: VideoInfo | None = None,
    ) -> list[str]:
        """Get input hardware acceleration arguments."""
        if encoder == EncoderType.QSV:
            if self._hw_can_decode_input(video_info):
                return ["-hwaccel", "qsv", "-hwaccel_output_format", "qsv"]
            # Skip hwaccel — let ffmpeg software-decode the 4:2:2/4:4:4
            # input into system memory. The QSV encoder still gets used
            # downstream; it'll auto-upload the converted frames. A
            # `-vf format=...` filter (added by build_transcode_command)
            # reduces chroma to whatever the encoder supports.
            logger.info(
                "ffmpeg: QSV hardware decode skipped (input chroma=%s); "
                "using software decode + QSV hardware encode (hybrid mode)",
                video_info.chroma if video_info else "?",
            )
            return []
        elif encoder == EncoderType.NVENC:
            if self._hw_can_decode_input(video_info):
                return ["-hwaccel", "cuda", "-hwaccel_output_format", "cuda"]
            logger.info(
                "ffmpeg: NVENC CUDA decode skipped (input chroma=%s); "
                "using software decode + NVENC hardware encode (hybrid mode)",
                video_info.chroma if video_info else "?",
            )
            return []
        return []

    def _get_video_filter_args(
        self,
        encoder: EncoderType,
        video_info: VideoInfo,
    ) -> list[str]:
        """Software chroma/format conversion filter, used when:
          - the hardware encoder needs 4:2:0 but the source is 4:2:2; AND
          - the input is software-decoded (no GPU surface), so we can
            apply -vf format=... before the encoder picks it up.

        For libx265 the `-pix_fmt` flag handles conversion natively, no
        filter needed. For QSV/NVENC with hybrid mode (sw decode + hw
        encode), the format filter is what bridges the chroma gap.
        """
        if encoder == EncoderType.CPU:
            return []
        # If hwaccel is active (frames live on GPU surfaces), no
        # software filter is appropriate.
        if self._hw_can_decode_input(video_info):
            return []
        # Hybrid mode: pick the same pix_fmt the encoder will produce so
        # the chroma reduction happens once, in software, before upload.
        out_pix_fmt, _, _, _ = self._resolve_output_format(video_info)
        return ["-vf", f"format={out_pix_fmt}"]

    def _resolve_output_format(self, video_info: VideoInfo) -> tuple[str, str, int, str]:
        """Decide output pix_fmt + profile + bit depth + chroma from input.

        Rules (also documented in the v6.2.0 release notes):
          - Bit depth: never downgrade. 8-bit upgrades to 10-bit because
            x265 compresses 10-bit better with no perceptible cost (the
            extra precision absorbs the rate-distortion penalty). 12-bit
            inputs stay at 12. >12 caps at 12 (HEVC limit) with warning.
          - Chroma: 4:2:0 by default. When preserve_chroma_422 is on AND
            input is 4:2:2, output 4:2:2 (forces libx265 because QSV /
            NVENC consumer don't support Main 4:2:2). 4:4:4 is treated
            as 4:2:2 (still preserves more than 4:2:0) with warning.

        Returns (pix_fmt, profile, bit_depth, chroma).
        """
        in_depth = max(8, int(video_info.bit_depth or 8))
        if in_depth > 12:
            logger.warning(
                "ffmpeg: source bit_depth=%d is above HEVC's 12-bit limit; "
                "capping output at 12-bit (bitdepth_capped_at_12)", in_depth,
            )
            in_depth = 12
        # Round 9/11 to next even — HEVC profiles only define main / main10 / main12.
        out_depth = 8 if in_depth <= 8 else (10 if in_depth <= 10 else 12)

        in_chroma = video_info.chroma or "420"
        if in_chroma == "444":
            logger.warning(
                "ffmpeg: source chroma 4:4:4 not supported in this pipeline; "
                "downgrading to 4:2:0 (chroma_444_downgraded)"
            )
            in_chroma = "420"

        preserve_422 = bool(getattr(self.config, "preserve_chroma_422", False))
        out_chroma = in_chroma if (preserve_422 and in_chroma == "422") else "420"

        # Build pix_fmt + profile per output spec. The 'p' (no bit-suffix)
        # form maps to 8-bit yuv420p / yuv422p; main10 / main12 require
        # the explicit 10le / 12le suffix.
        if out_chroma == "422":
            if out_depth == 8:
                pix_fmt = "yuv422p"
                profile = "main"  # libx265 handles main 4:2:2 8-bit
            elif out_depth == 10:
                pix_fmt = "yuv422p10le"
                profile = "main-422-10"
            else:  # 12
                pix_fmt = "yuv422p12le"
                profile = "main-422-12"
        else:  # 420
            if out_depth == 8:
                pix_fmt = "yuv420p"
                profile = "main"
            elif out_depth == 10:
                pix_fmt = "yuv420p10le"
                profile = "main10"
            else:  # 12
                pix_fmt = "yuv420p12le"
                profile = "main12"

        return pix_fmt, profile, out_depth, out_chroma

    def _color_metadata_args(self, video_info: VideoInfo) -> list[str]:
        """Build -color_* flags from probe data.

        Each tag is emitted only when the input declared it AND the value
        is in the encoder-accepted whitelist. Tags like 'gbr' (which
        ffprobe reports for RGB24 sources like PNGs in mov containers)
        are NOT valid `-colorspace` values for QSV / NVENC / libx265 —
        they reject the option with "Undefined constant" and the whole
        encode bails. Since we always convert RGB sources to YUV anyway,
        the original RGB matrix tag has no meaning post-conversion.

        Range is the exception — most muxers / NLEs assume 'tv' so we
        set it explicitly to match the input or default 'tv'.
        """
        out: list[str] = []
        prim = video_info.color_primaries
        if prim and prim.lower() in _VALID_COLOR_PRIMARIES:
            out.extend(["-color_primaries", prim])
        trc = video_info.color_transfer
        if trc and trc.lower() in _VALID_COLOR_TRC:
            out.extend(["-color_trc", trc])
        cs = video_info.color_space
        if cs and cs.lower() in _VALID_COLORSPACE:
            out.extend(["-colorspace", cs])
        # Range: pass through what we have, else default tv. Sony S-Log3
        # is the canonical "pc" (full range) source.
        out.extend(["-color_range", video_info.color_range or "tv"])

        # Heuristic info log when we suspect a log/raw camera source so
        # the operator can confirm the daemon understood. The signature:
        # 10-bit, primaries+transfer+space all None, range=pc.
        if (
            (video_info.bit_depth or 8) >= 10
            and not video_info.color_primaries
            and not video_info.color_transfer
            and not video_info.color_space
            and (video_info.color_range or "").lower() == "pc"
        ):
            logger.info(
                "ffmpeg: detected likely log/raw source (e.g. Sony S-Log3) — "
                "passing color metadata through as-is, no conversion"
            )
        return out

    def _get_video_encoder_args(
        self,
        encoder: EncoderType,
        profile: TranscodeProfile,
        video_info: VideoInfo,
    ) -> list[str]:
        """Get video encoder arguments based on encoder and profile."""
        out_pix_fmt, out_profile, out_depth, out_chroma = self._resolve_output_format(video_info)

        # 4:2:2 output requires libx265 — QSV/NVENC consumer chips don't
        # implement Main 4:2:2 10/12. Caller (workers._transcode_job)
        # already overrode the encoder to CPU when appropriate; this
        # function trusts that decision but logs a hard error if the
        # combination is impossible (defensive — should never fire).
        if out_chroma == "422" and encoder != EncoderType.CPU:
            logger.error(
                "ffmpeg: 4:2:2 output requires libx265 but encoder=%s — "
                "falling back to libx265 args anyway. Job will run on CPU.",
                encoder.value,
            )
            encoder = EncoderType.CPU

        if encoder == EncoderType.QSV:
            args = self._get_qsv_args(profile, out_pix_fmt, out_profile)
        elif encoder == EncoderType.NVENC:
            args = self._get_nvenc_args(profile, out_pix_fmt, out_profile)
        else:  # CPU
            args = self._get_cpu_args(profile, out_pix_fmt, out_profile)
        return args

    def _get_qsv_args(self, profile: TranscodeProfile, pix_fmt: str, profile_str: str) -> list[str]:
        """Get Intel QuickSync encoder arguments. QSV only handles 4:2:0
        Main / Main10. The pix_fmt is set on the QSV-side via the encoder
        (not via -pix_fmt because the pipeline runs in qsv_surface format
        from -hwaccel_output_format qsv)."""
        from .config import TranscodeProfile

        args = ["-c:v", "hevc_qsv", "-profile:v", profile_str, "-preset", "medium"]

        if profile == TranscodeProfile.QUALITY:
            # CQ mode with global_quality (R6).
            # NOTE: -look_ahead 1 was here previously. On older Intel iGPUs
            # (anything pre-11th gen Tiger Lake) the lookahead path falls
            # back to a software/hybrid implementation that drops hevc_qsv
            # throughput from ~200 fps to ~20 fps (0.6x real-time).
            args.extend(["-global_quality:v", str(self.config.cq_value)])
        else:  # BALANCED
            args.extend([
                "-b:v", f"{self.config.bitrate.target_mbps}M",
                "-maxrate", f"{self.config.bitrate.max_mbps}M",
                "-bufsize", f"{self.config.bitrate.bufsize_mbps}M",
            ])

        return args

    def _get_nvenc_args(self, profile: TranscodeProfile, pix_fmt: str, profile_str: str) -> list[str]:
        """Get NVIDIA NVENC encoder arguments. Consumer NVENC supports
        Main / Main10 4:2:0 only — 4:2:2 / 4:4:4 are Pro-card features."""
        from .config import TranscodeProfile

        args = ["-c:v", "hevc_nvenc", "-profile:v", profile_str, "-preset", "p5", "-tune", "hq"]

        if profile == TranscodeProfile.QUALITY:
            args.extend([
                "-rc:v", "vbr",
                "-cq:v", str(self.config.cq_value),
                "-b:v", "0",
                "-maxrate", f"{self.config.bitrate.max_mbps}M",
                "-bufsize", f"{self.config.bitrate.bufsize_mbps}M",
            ])
        else:  # BALANCED
            args.extend([
                "-rc:v", "vbr",
                "-b:v", f"{self.config.bitrate.target_mbps}M",
                "-maxrate", f"{self.config.bitrate.max_mbps}M",
                "-bufsize", f"{self.config.bitrate.bufsize_mbps}M",
            ])

        args.extend(["-bf", "4"])
        return args

    def _get_cpu_args(self, profile: TranscodeProfile, pix_fmt: str, profile_str: str) -> list[str]:
        """Get CPU (libx265) encoder arguments. Handles every Main /
        Main10 / Main12 / Main 4:2:2 / Main 4:4:4 combination — used as
        the fallback for chroma-preserving jobs."""
        from .config import TranscodeProfile

        args = ["-c:v", "libx265", "-preset", "medium"]
        x265_params = [f"profile={profile_str}"]

        if profile == TranscodeProfile.QUALITY:
            x265_params.append(f"crf={self.config.cpu_crf_equivalent}")
            x265_params.append("aq-mode=3")
        else:  # BALANCED
            args.extend([
                "-b:v", f"{self.config.bitrate.target_mbps}M",
                "-maxrate", f"{self.config.bitrate.max_mbps}M",
                "-bufsize", f"{self.config.bitrate.bufsize_mbps}M",
            ])

        args.extend(["-pix_fmt", pix_fmt])
        args.extend(["-x265-params", ":".join(x265_params)])
        return args

    def _get_audio_args(self, video_info: VideoInfo) -> list[str]:
        """Get audio handling arguments."""
        if not video_info.has_audio:
            return ["-an"]

        # Try to copy audio; fallback to re-encode handled in transcoder
        return ["-c:a", "copy"]

    def build_audio_fallback_command(
        self,
        input_path: Path,
        output_path: Path,
        video_info: VideoInfo,
        encoder: EncoderType,
    ) -> FFmpegCommand:
        """
        Build command with audio re-encoding fallback.

        Used when audio copy fails (incompatible codec).
        """
        cmd = self.build_transcode_command(input_path, output_path, video_info, encoder)

        # Replace audio copy with re-encode
        new_args = []
        i = 0
        while i < len(cmd.args):
            if cmd.args[i] == "-c:a" and i + 1 < len(cmd.args) and cmd.args[i + 1] == "copy":
                new_args.extend([
                    "-c:a", self.config.audio_fallback_codec,
                    "-b:a", self.config.audio_fallback_bitrate,
                ])
                i += 2
            else:
                new_args.append(cmd.args[i])
                i += 1

        return FFmpegCommand(
            args=new_args,
            input_path=cmd.input_path,
            output_path=cmd.output_path,
            temp_output_path=cmd.temp_output_path,
            encoder=cmd.encoder,
            profile=cmd.profile,
            description=cmd.description + " (audio re-encode)",
            expected_duration_sec=cmd.expected_duration_sec,
        )


def build_probe_command(
    input_path: Path,
    ffprobe_path: str = "ffprobe",
) -> list[str]:
    """
    Build ffprobe command to analyze video.

    Returns:
        Command list for subprocess.
    """
    return [
        ffprobe_path,
        "-v", "quiet",
        "-print_format", "json",
        "-show_format",
        "-show_streams",
        str(input_path),
    ]


def build_validation_probe_command(
    output_path: Path,
    ffprobe_path: str = "ffprobe",
) -> list[str]:
    """
    Build ffprobe command to validate output.

    Returns:
        Command list for subprocess.
    """
    return [
        ffprobe_path,
        "-v", "error",
        "-print_format", "json",
        "-show_format",
        "-show_streams",
        "-show_error",
        str(output_path),
    ]
