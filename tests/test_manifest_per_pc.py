"""Tests for per-PC manifest system (no race conditions)."""
import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


class TestManifestPerPC:
    """Test the GlobalManifestManager from src/transcoder/manifest.py."""

    def _make_manager(self, tmp_path, pc_name="Heavy1"):
        with patch("transcoder.manifest.get_pc_name", return_value=pc_name):
            from transcoder.manifest import GlobalManifestManager
            return GlobalManifestManager(base_dropbox_path=str(tmp_path), auto_save_interval=1)

    def test_creates_manifests_dir(self, tmp_path):
        mgr = self._make_manager(tmp_path)
        assert (tmp_path / "manifests").is_dir()

    def test_creates_per_pc_file(self, tmp_path):
        mgr = self._make_manager(tmp_path, "Heavy1")
        mgr.save(force=True)
        assert (tmp_path / "manifests" / "Heavy1.json").exists()

    def test_no_global_manifest_created(self, tmp_path):
        mgr = self._make_manager(tmp_path)
        mgr.save(force=True)
        assert not (tmp_path / "global_manifest.json").exists()

    def test_record_success_in_own_manifest(self, tmp_path):
        mgr = self._make_manager(tmp_path, "Heavy1")
        mgr.record_success(
            "C:\\Videos\\test.mp4", "C:\\Videos\\h265\\test.mp4",
            1_000_000, 500_000, "hevc_qsv", 24
        )
        assert mgr.is_processed("C:\\Videos\\test.mp4")
        with open(tmp_path / "manifests" / "Heavy1.json") as f:
            data = json.load(f)
        assert len(data['processed_files']) == 1

    def test_two_pcs_independent_writes(self, tmp_path):
        """Two PCs should write to separate files without conflicts."""
        mgr1 = self._make_manager(tmp_path, "Heavy1")
        mgr2 = self._make_manager(tmp_path, "Heavy2")

        mgr1.record_success("C:\\V\\a.mp4", "", 1000, 500, "qsv", 24)
        mgr2.record_success("C:\\V\\b.mp4", "", 2000, 1000, "nvenc", 24)

        assert (tmp_path / "manifests" / "Heavy1.json").exists()
        assert (tmp_path / "manifests" / "Heavy2.json").exists()

        assert mgr1.is_processed("C:\\V\\a.mp4")
        assert mgr2.is_processed("C:\\V\\b.mp4")

    def test_cross_pc_visibility(self, tmp_path):
        """After refresh, each PC should see the other's processed files."""
        mgr1 = self._make_manager(tmp_path, "Heavy1")
        mgr1.record_success("C:\\V\\a.mp4", "", 1000, 500, "qsv", 24)

        mgr2 = self._make_manager(tmp_path, "Heavy2")
        assert mgr2.is_processed("C:\\V\\a.mp4")

    def test_skipped_cross_pc(self, tmp_path):
        mgr1 = self._make_manager(tmp_path, "Heavy1")
        mgr1.record_skipped("C:\\V\\small.mp4", "too_small", 100)

        mgr2 = self._make_manager(tmp_path, "Heavy2")
        assert mgr2.is_skipped("C:\\V\\small.mp4")

    def test_failed_cross_pc(self, tmp_path):
        mgr1 = self._make_manager(tmp_path, "Heavy1")
        mgr1.record_failure("C:\\V\\bad.mp4", "corrupt file")

        mgr2 = self._make_manager(tmp_path, "Heavy2")
        assert mgr2.is_failed("C:\\V\\bad.mp4")

    def test_dashboard_merges_all_pcs(self, tmp_path):
        mgr1 = self._make_manager(tmp_path, "Heavy1")
        mgr1.record_success("C:\\V\\a.mp4", "", 1_000_000, 500_000, "qsv", 24)

        mgr2 = self._make_manager(tmp_path, "Heavy2")
        mgr2.record_success("C:\\V\\b.mp4", "", 2_000_000, 1_000_000, "nvenc", 24)

        mgr2.refresh()
        dashboard = mgr2.get_dashboard_data()
        assert dashboard['total_processed'] == 2
        assert 'Heavy1' in dashboard['active_pcs']
        assert 'Heavy2' in dashboard['active_pcs']

    def test_stats_summary_merged(self, tmp_path):
        mgr1 = self._make_manager(tmp_path, "Heavy1")
        mgr1.record_success("C:\\V\\a.mp4", "", 1_000_000, 500_000, "qsv", 24)

        mgr2 = self._make_manager(tmp_path, "Heavy2")
        mgr2.record_success("C:\\V\\b.mp4", "", 2_000_000, 1_000_000, "nvenc", 24)

        mgr2.refresh()
        stats = mgr2.get_stats_summary()
        assert stats['processed'] == 2

    def test_reset_failed(self, tmp_path):
        mgr = self._make_manager(tmp_path, "Heavy1")
        mgr.record_failure("C:\\V\\bad.mp4", "error")
        assert mgr.is_failed("C:\\V\\bad.mp4")

        mgr.reset_failed()
        assert not mgr.is_failed("C:\\V\\bad.mp4")

    def test_cleanup_old_history(self, tmp_path):
        mgr = self._make_manager(tmp_path, "Heavy1")
        from transcoder.manifest import DailyProgress
        mgr.manifest.daily_history['2020-01-01'] = DailyProgress(
            date='2020-01-01', files_processed=1,
            bytes_processed=100, bytes_saved=50, by_pc={'Heavy1': 1},
        )
        mgr.manifest.daily_history['2025-12-01'] = DailyProgress(
            date='2025-12-01', files_processed=1,
            bytes_processed=100, bytes_saved=50, by_pc={'Heavy1': 1},
        )
        removed = mgr.cleanup_old_history(max_days=90)
        assert removed == 1
        assert '2020-01-01' not in mgr.manifest.daily_history
        assert '2025-12-01' in mgr.manifest.daily_history

    def test_path_normalization(self, tmp_path):
        mgr = self._make_manager(tmp_path, "Heavy1")
        mgr.record_success("C:\\Videos\\Test.MP4", "", 1000, 500, "qsv", 24)
        assert mgr.is_processed("c:/videos/test.mp4")
        assert mgr.is_processed("C:\\VIDEOS\\TEST.MP4")

    def test_should_process(self, tmp_path):
        mgr = self._make_manager(tmp_path, "Heavy1")
        assert mgr.should_process("C:\\V\\new.mp4")
        mgr.record_success("C:\\V\\new.mp4", "", 1000, 500, "qsv", 24)
        assert not mgr.should_process("C:\\V\\new.mp4")

    def test_dedup_across_pcs(self, tmp_path):
        """Same file processed by both PCs should count once in stats."""
        mgr1 = self._make_manager(tmp_path, "Heavy1")
        mgr1.record_success("C:\\V\\same.mp4", "", 1000, 500, "qsv", 24)

        mgr2 = self._make_manager(tmp_path, "Heavy2")
        mgr2.record_success("C:\\V\\same.mp4", "", 1000, 500, "nvenc", 24)

        mgr2.refresh()
        stats = mgr2.get_stats_summary()
        assert stats['processed'] == 1  # Deduped by path

    def test_get_manifest_path_returns_per_pc(self, tmp_path):
        mgr = self._make_manager(tmp_path, "Heavy1")
        path = mgr.get_manifest_path()
        assert path == tmp_path / "manifests" / "Heavy1.json"
