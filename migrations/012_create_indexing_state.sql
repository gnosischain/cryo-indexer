-- Simplified indexing state management table

CREATE TABLE IF NOT EXISTS {{database}}.indexing_state
(
    `mode` String,                    -- Indexing mode (minimal, full, etc.)
    `dataset` String,                 -- Dataset name (blocks, transactions, etc.)
    `start_block` UInt32,            -- Start of the range
    `end_block` UInt32,              -- End of the range (exclusive)
    `status` String,                  -- pending, processing, completed, failed
    `worker_id` String DEFAULT '',    -- Worker processing this range
    `attempt_count` UInt8 DEFAULT 0,  -- Number of attempts
    `created_at` DateTime DEFAULT now(),
    `completed_at` Nullable(DateTime),
    `rows_indexed` Nullable(UInt64),
    `error_message` Nullable(String),
    `insert_version` UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9)),
    
    INDEX idx_status (status) TYPE minmax GRANULARITY 4,
    INDEX idx_mode_dataset (mode, dataset) TYPE minmax GRANULARITY 4
)
ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toYYYYMM(created_at)
ORDER BY (mode, dataset, start_block)
SETTINGS index_granularity = 8192;

-- Simple progress view
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

INSERT INTO {{database}}.migrations (name) VALUES ('012_create_indexing_state');