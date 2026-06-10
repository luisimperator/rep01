"""Regression tests for the deterministic truncated-transcode loop.

HEAVY7 field case: hevc_qsv transcode of a 4178.7s file exited 0 but the
output stopped at exactly 2157.4s on EVERY attempt — the QSV decoder hit a
broken spot in the bitstream and bailed quietly. The old pipeline treated
the "Output validation failed: Duration mismatch" as transient and re-ran
the identical command forever (~35 min of GPU per lap).

New behavior:
  * first truncation with hardware decode → one in-line retry with software
    decode (hybrid mode: sw decode + same HW encoder), which usually powers
    through corrupt regions;
  * truncation despite software decode → terminal SKIPPED_CORRUPT via the
    TRUNCATED_DESPITE_SW_DECODE error marker.
"""
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from transcoder.database import JobState  # noqa: E402
from transcoder.encoder_detect import EncoderType  # noqa: E402
from transcoder.ffmpeg_builder import FFmpegCommandBuilder, VideoInfo  # noqa: E402


def _video_info(**overrides) -> VideoInfo:
    base = dict(
        codec_name="h264",
        width=1920,
        height=1080,
        fps=29.97,
        duration_sec=4178.7,
        bitrate_kbps=20_000,
        bit_depth=8,
        chroma="420",
        has_audio=True,
    )
    base.update(overrides)
    return VideoInfo(**base)


def _config():
    from transcoder.config import TranscodeProfile

    return SimpleNamespace(
        profile=TranscodeProfile.QUALITY,
        ffmpeg_path="ffmpeg",
        gop_size=60,
        ffmpeg_extra_args=[],
        cq_value=26,
        preserve_chroma_422=False,
        cpu_crf_equivalent=23,
        bitrate=SimpleNamespace(target_mbps=40, max_mbps=60, bufsize_mbps=120),
        audio_fallback_codec="aac",
        audio_fallback_bitrate="256k",
    )


# --- builder: force_sw_decode --------------------------------------------------

def test_qsv_default_uses_hw_decode(tmp_path):
    builder = FFmpegCommandBuilder(_config())
    cmd = builder.build_transcode_command(
        tmp_path / "in.mp4", tmp_path / "out.mp4", _video_info(), EncoderType.QSV,
    )
    assert "-hwaccel" in cmd.args
    assert "hevc_qsv" in cmd.args


def test_force_sw_decode_drops_hwaccel_keeps_hw_encoder(tmp_path):
    builder = FFmpegCommandBuilder(_config())
    cmd = builder.build_transcode_command(
        tmp_path / "in.mp4", tmp_path / "out.mp4", _video_info(), EncoderType.QSV,
        force_sw_decode=True,
    )
    assert "-hwaccel" not in cmd.args, "sw decode must not init the hw decoder"
    assert "hevc_qsv" in cmd.args, "the ENCODER stays hardware (hybrid mode)"
    # sw-decoded frames need the explicit format bridge to the hw encoder
    assert "-vf" in cmd.args
    vf = cmd.args[cmd.args.index("-vf") + 1]
    assert vf.startswith("format=")
    assert "(forced sw decode)" in cmd.description


def test_force_sw_decode_audio_fallback_passthrough(tmp_path):
    builder = FFmpegCommandBuilder(_config())
    cmd = builder.build_audio_fallback_command(
        tmp_path / "in.mp4", tmp_path / "out.mp4", _video_info(), EncoderType.QSV,
        force_sw_decode=True,
    )
    assert "-hwaccel" not in cmd.args
    assert "aac" in cmd.args, "audio re-encode preserved on the sw-decode retry"


def test_force_sw_decode_noop_for_cpu(tmp_path):
    builder = FFmpegCommandBuilder(_config())
    cmd = builder.build_transcode_command(
        tmp_path / "in.mp4", tmp_path / "out.mp4", _video_info(), EncoderType.CPU,
        force_sw_decode=True,
    )
    assert "-hwaccel" not in cmd.args
    assert "libx265" in cmd.args


# --- TranscodeWorker._handle_failure classification ----------------------------

class _FakeDB:
    def __init__(self):
        self.updates = []
        self.retry_calls = 0

    def update_job_state(self, job_id, state, **kw):
        self.updates.append((job_id, state, kw))

    def increment_retry(self, job_id, max_retries):
        self.retry_calls += 1
        return (1, False)

    @property
    def last_state(self):
        return self.updates[-1][1]


def _bare_transcode_worker():
    import threading

    from transcoder.workers import TranscodeWorker

    worker = object.__new__(TranscodeWorker)
    # Workers are Thread subclasses; the name property needs Thread.__init__.
    threading.Thread.__init__(worker, name="transcoder-test", daemon=True)
    worker.db = _FakeDB()
    worker.config = SimpleNamespace(watchdog=SimpleNamespace(max_retries=10))
    worker.disk_budget = None
    worker.incident_reporter = None
    worker._last_ffmpeg_log = None
    return worker


def _job():
    return SimpleNamespace(id=17610, dropbox_path="/HD/x/MAIN.mp4", local_input_path=None)


def test_truncated_despite_sw_decode_is_terminal_corrupt():
    from transcoder.workers import TRUNCATED_DESPITE_SW_DECODE

    worker = _bare_transcode_worker()
    worker._handle_failure(
        _job(),
        f"{TRUNCATED_DESPITE_SW_DECODE} Duration mismatch: expected 4178.7s, "
        f"got 2157.4s (diff 2021.4s > 41.8s)",
    )
    assert worker.db.last_state == JobState.SKIPPED_CORRUPT
    assert worker.db.retry_calls == 0


def test_probe_failed_still_terminal_corrupt():
    worker = _bare_transcode_worker()
    worker._handle_failure(_job(), "Probe failed: moov atom not found")
    assert worker.db.last_state == JobState.SKIPPED_CORRUPT


def test_plain_validation_failure_still_retries():
    """Only the explicit sw-decode marker is terminal — a bare validation
    failure (e.g. before the sw retry ran) keeps the retry path."""
    worker = _bare_transcode_worker()
    worker._handle_failure(
        _job(), "Output validation failed: Output file is empty",
    )
    assert worker.db.last_state == JobState.RETRY_WAIT
    assert worker.db.retry_calls == 1
