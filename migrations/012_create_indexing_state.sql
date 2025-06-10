-- Create comprehensive indexing state management tables
-- Optimized for ClickHouse Cloud with minimal RAM usage

-- Main indexing state table
CREATE TABLE IF NOT EXISTS {{database}}.indexing_state
(
    `mode` String,                    -- Indexing mode (default, full, minimal, etc.)
    `dataset` String,                 -- Dataset name (blocks, transactions, etc.)
    `start_block` UInt32,            -- Start of the range
    `end_block` UInt32,              -- End of the range (exclusive)
    `status` String,                  -- pending, processing, completed, failed
    `worker_id` String DEFAULT '',    -- Worker processing this range
    `attempt_count` UInt8 DEFAULT 0,  -- Number of attempts
    `created_at` DateTime DEFAULT now(),
    `started_at` Nullable(DateTime),
    `completed_at` Nullable(DateTime),
    `rows_indexed` Nullable(UInt64),
    `error_message` Nullable(String),
    `batch_id` String DEFAULT '',     -- For grouping related ranges
    `version` DateTime DEFAULT now(), -- Version column for ReplacingMergeTree
    
    -- Indexes for efficient queries
    INDEX idx_status (status) TYPE minmax GRANULARITY 4,
    INDEX idx_mode_dataset (mode, dataset) TYPE minmax GRANULARITY 4
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(created_at)
ORDER BY (mode, dataset, start_block)
SETTINGS index_granularity = 8192;

-- Lightweight progress tracking view
CREATE VIEW IF NOT EXISTS {{database}}.indexing_progress AS
SELECT 
    mode,
    dataset,
    countIf(status = 'completed') as completed_ranges,
    countIf(status = 'processing') as processing_ranges,
    countIf(status = 'failed') as failed_ranges,
    countIf(status = 'pending') as pending_ranges,
    maxIf(end_block, status = 'completed') as highest_completed_block,
    sum(rows_indexed) as total_rows_indexed
FROM {{database}}.indexing_state
GROUP BY mode, dataset;

-- Table for tracking continuous sync position
CREATE TABLE IF NOT EXISTS {{database}}.sync_position
(
    `mode` String,
    `dataset` String,
    `last_synced_block` UInt32,
    `updated_at` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (mode, dataset)
SETTINGS index_granularity = 8192;

-- Insert migration record
INSERT INTO {{database}}.migrations (name) VALUES ('012_create_indexing_state');