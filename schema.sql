-- Dropbox Video Transcoder Database Schema
-- =========================================
-- SQLite database for job queue, state tracking, and metrics.

-- Jobs table: main job queue and state tracking
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Dropbox file info
    dropbox_path TEXT NOT NULL,           -- Full path in Dropbox
    dropbox_rev TEXT NOT NULL,            -- File revision for change detection
    dropbox_size INTEGER NOT NULL,        -- File size in bytes
    output_path TEXT NOT NULL,            -- Target path in Dropbox (/h265/ subfolder)

    -- Job state
    state TEXT NOT NULL DEFAULT 'NEW',    -- Current state (see JobState enum)
    retry_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,                   -- Last error message if any

    -- Local paths
    local_input_path TEXT,                -- Path to downloaded input file
    local_output_path TEXT,               -- Path to transcoded output file

    -- Video info (from ffprobe)
    input_codec TEXT,                     -- Original codec (e.g., h264)
    output_codec TEXT,                    -- Output codec (hevc)
    input_duration_sec REAL,              -- Input duration in seconds
    output_duration_sec REAL,             -- Output duration for validation
    input_bitrate_kbps INTEGER,           -- Original bitrate
    output_bitrate_kbps INTEGER,          -- Output bitrate

    -- Transcode info
    encoder_used TEXT,                    -- Encoder used (hevc_qsv, hevc_nvenc, libx265)
    transcode_start TEXT,                 -- ISO timestamp of transcode start
    transcode_end TEXT,                   -- ISO timestamp of transcode end

    -- Timestamps
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),

    -- Unique constraint: one job per path+revision combo
    UNIQUE(dropbox_path, dropbox_rev)
);

-- Indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state);
CREATE INDEX IF NOT EXISTS idx_jobs_path ON jobs(dropbox_path);
CREATE INDEX IF NOT EXISTS idx_jobs_updated ON jobs(updated_at);

-- Stability checks table: tracks file stability over time (R2)
-- Used to ensure files are fully synced before processing
CREATE TABLE IF NOT EXISTS stability_checks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dropbox_path TEXT NOT NULL,           -- File being monitored
    check_time TEXT NOT NULL DEFAULT (datetime('now')),
    size INTEGER NOT NULL,                -- File size at check time
    rev TEXT NOT NULL,                    -- File revision at check time
    server_modified TEXT NOT NULL,        -- Server modified timestamp
    content_hash TEXT                     -- Optional content hash
);

-- Indexes for stability queries
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

-- Trigger to update updated_at on jobs table
CREATE TRIGGER IF NOT EXISTS jobs_updated_at
    AFTER UPDATE ON jobs
    BEGIN
        UPDATE jobs SET updated_at = datetime('now') WHERE id = NEW.id;
    END;

-- Job States:
-- -----------
-- NEW              - Job queued, waiting for download
-- STABLE_WAIT      - Waiting for file stability confirmation
-- DOWNLOADING      - Download in progress
-- DOWNLOADED       - Download complete, ready for transcode
-- PROBING          - Running ffprobe analysis
-- TRANSCODING      - FFmpeg transcode in progress
-- UPLOADING        - Upload to Dropbox in progress
-- DONE             - Successfully completed
-- SKIPPED_HEVC     - Skipped because input is already HEVC
-- SKIPPED_ALREADY_EXISTS - Skipped because output already exists
-- SKIPPED_TOO_SMALL - Skipped because file is below min_size_gb
-- FAILED           - Failed after maximum retries
-- RETRY_WAIT       - Waiting for retry after transient error
