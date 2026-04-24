"""Shared test fixtures."""
import json
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def tmp_dropbox(tmp_path):
    """Create a temporary Dropbox-like directory structure."""
    return tmp_path


@pytest.fixture
def manifest_data():
    """Sample manifest data for testing."""
    return {
        'pc_name': 'TestPC',
        'created_at': '2025-01-01T00:00:00',
        'last_updated': '2025-01-01T12:00:00',
        'last_updated_by': 'TestPC',
        'stats': {
            'total_files_processed': 2,
            'total_input_bytes': 2_000_000_000,
            'total_output_bytes': 1_000_000_000,
            'total_saved_bytes': 1_000_000_000,
            'total_transcode_seconds': 600,
            'total_files_to_process': 10,
            'total_bytes_to_process': 10_000_000_000,
        },
        'processed_files': {
            'c:/videos/file1.mp4': {
                'original_path': 'C:\\Videos\\file1.mp4',
                'output_path': 'C:\\Videos\\h265\\file1.mp4',
                'input_size_bytes': 1_000_000_000,
                'output_size_bytes': 500_000_000,
                'compression_ratio': 0.5,
                'processed_at': '2025-01-01T10:00:00',
                'processed_by_pc': 'TestPC',
                'encoder_used': 'hevc_qsv',
                'cq_value': 24,
            },
            'c:/videos/file2.mp4': {
                'original_path': 'C:\\Videos\\file2.mp4',
                'output_path': 'C:\\Videos\\h265\\file2.mp4',
                'input_size_bytes': 1_000_000_000,
                'output_size_bytes': 500_000_000,
                'compression_ratio': 0.5,
                'processed_at': '2025-01-01T11:00:00',
                'processed_by_pc': 'TestPC',
                'encoder_used': 'hevc_qsv',
                'cq_value': 24,
            },
        },
        'skipped_files': {
            'c:/videos/small.mp4': {
                'path': 'C:\\Videos\\small.mp4',
                'reason': 'too_small',
                'size_bytes': 1000,
                'checked_at': '2025-01-01T10:00:00',
                'checked_by_pc': 'TestPC',
            },
        },
        'failed_files': {},
        'daily_history': {
            '2025-01-01': {
                'date': '2025-01-01',
                'files_processed': 2,
                'bytes_processed': 2_000_000_000,
                'bytes_saved': 1_000_000_000,
                'by_pc': {'TestPC': 2},
            },
        },
        'active_pcs': {'TestPC': '2025-01-01T12:00:00'},
        'imported_h265_logs': {},
    }
