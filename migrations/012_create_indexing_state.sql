-- Indexing state management table (mode-independent)

CREATE TABLE IF NOT EXISTS {{database}}.indexing_state
(
    `dataset` String,
    `start_block` UInt32,
    `end_block` UInt32,
    `status` String,                  -- pending, processing, completed, failed
    `worker_id` String DEFAULT '',
    `attempt_count` UInt8 DEFAULT 0,
    `created_at` DateTime DEFAULT now(),
    `completed_at` Nullable(DateTime),
    `rows_indexed` Nullable(UInt64),
    `error_message` Nullable(String),
    `insert_version` UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9)),

    INDEX idx_status (status) TYPE minmax GRANULARITY 4,
    INDEX idx_dataset (dataset) TYPE minmax GRANULARITY 4
)
ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toYYYYMM(created_at)
ORDER BY (dataset, start_block, end_block)
SETTINGS index_granularity = 8192;

-- Drop old views in case they reference the old schema (with mode column)
DROP VIEW IF EXISTS {{database}}.indexing_summary;
DROP VIEW IF EXISTS {{database}}.continuous_ranges;
DROP VIEW IF EXISTS {{database}}.indexing_progress;

-- Indexing progress view
CREATE VIEW {{database}}.indexing_progress AS
WITH latest_state AS (
    SELECT
        dataset,
        start_block,
        end_block,
        argMax(status, created_at) as latest_status,
        argMax(worker_id, created_at) as latest_worker_id,
        argMax(attempt_count, created_at) as latest_attempt_count,
        argMax(created_at, created_at) as latest_created_at,
        argMax(completed_at, created_at) as latest_completed_at,
        argMax(rows_indexed, created_at) as latest_rows_indexed,
        argMax(error_message, created_at) as latest_error_message
    FROM {{database}}.indexing_state
    GROUP BY dataset, start_block, end_block
)
SELECT
    dataset,
    COUNT(*) as total_ranges,
    countIf(latest_status = 'completed') as completed_ranges,
    countIf(latest_status = 'processing') as processing_ranges,
    countIf(latest_status = 'failed') as failed_ranges,
    countIf(latest_status = 'pending') as pending_ranges,
    MIN(start_block) as lowest_block_attempted,
    MAX(end_block) as highest_block_attempted,
    maxIf(end_block, latest_status = 'completed') as highest_block_completed,
    SUM(latest_rows_indexed) as total_rows_indexed,
    CASE
        WHEN COUNT(*) > 0 THEN
            ROUND(countIf(latest_status = 'completed') / COUNT(*) * 100, 2)
        ELSE 0
    END as completion_percentage,
    CASE
        WHEN countIf(latest_status = 'completed') = COUNT(*) THEN 'complete'
        WHEN countIf(latest_status = 'processing') > 0 THEN 'in_progress'
        WHEN countIf(latest_status = 'failed') > 0 AND countIf(latest_status = 'pending') = 0 AND countIf(latest_status = 'processing') = 0 THEN 'failed'
        WHEN countIf(latest_status = 'pending') > 0 THEN 'pending'
        ELSE 'mixed'
    END as overall_status,
    MIN(latest_created_at) as first_attempt,
    MAX(latest_completed_at) as last_completion
FROM latest_state
GROUP BY dataset
ORDER BY dataset;

-- Continuous ranges view
CREATE VIEW {{database}}.continuous_ranges AS
WITH completed_ranges AS (
    SELECT
        dataset,
        start_block,
        end_block,
        argMax(status, created_at) as latest_status
    FROM {{database}}.indexing_state
    GROUP BY dataset, start_block, end_block
    HAVING latest_status = 'completed'
),
ranges_with_groups AS (
    SELECT
        dataset,
        start_block,
        end_block,
        start_block - SUM(end_block - start_block) OVER (
            PARTITION BY dataset
            ORDER BY start_block
            ROWS UNBOUNDED PRECEDING
        ) as group_key
    FROM completed_ranges
)
SELECT
    dataset,
    MIN(start_block) as continuous_start,
    MAX(end_block) as continuous_end,
    COUNT(*) as num_segments,
    MAX(end_block) - MIN(start_block) as total_blocks,
    CONCAT(toString(MIN(start_block)), '-', toString(MAX(end_block))) as range_description,
    ROUND(COUNT(*) * 100.0 / (MAX(end_block) - MIN(start_block)) * 100, 2) as coverage_percentage,
    CASE
        WHEN COUNT(*) = (MAX(end_block) - MIN(start_block)) / 100 THEN 'complete'
        WHEN COUNT(*) < (MAX(end_block) - MIN(start_block)) / 100 THEN 'has_gaps'
        ELSE 'overlapping'
    END as continuity_status
FROM ranges_with_groups
GROUP BY dataset, group_key
ORDER BY dataset, continuous_start;

-- Indexing summary view
CREATE VIEW {{database}}.indexing_summary AS
SELECT
    p.dataset,
    p.total_ranges,
    p.completed_ranges,
    p.processing_ranges,
    p.failed_ranges,
    p.pending_ranges,
    p.completion_percentage,
    p.overall_status,
    p.highest_block_completed,
    p.total_rows_indexed,
    COUNT(c.continuous_start) as continuous_segments,
    MIN(c.continuous_start) as first_continuous_block,
    MAX(c.continuous_end) as last_continuous_block,
    CASE
        WHEN COUNT(c.continuous_start) = 1 THEN 'fully_continuous'
        WHEN COUNT(c.continuous_start) > 1 THEN CONCAT('fragmented_', toString(COUNT(c.continuous_start)), '_segments')
        ELSE 'no_completed_data'
    END as continuity_description
FROM {{database}}.indexing_progress p
LEFT JOIN {{database}}.continuous_ranges c
    ON p.dataset = c.dataset
GROUP BY
    p.dataset, p.total_ranges, p.completed_ranges, p.processing_ranges,
    p.failed_ranges, p.pending_ranges, p.completion_percentage, p.overall_status,
    p.highest_block_completed, p.total_rows_indexed
ORDER BY p.dataset;

INSERT INTO {{database}}.migrations (name) VALUES ('012_create_indexing_state');
