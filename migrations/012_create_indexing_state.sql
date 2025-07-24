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

-- Create updated indexing_progress view that always gets latest status
CREATE VIEW IF NOT EXISTS {{database}}.indexing_progress AS
WITH latest_state AS (
    SELECT 
        mode,
        dataset,
        start_block,
        end_block,
        -- Get the latest record for each range by timestamp
        argMax(status, created_at) as latest_status,
        argMax(worker_id, created_at) as latest_worker_id,
        argMax(attempt_count, created_at) as latest_attempt_count,
        argMax(created_at, created_at) as latest_created_at,
        argMax(completed_at, created_at) as latest_completed_at,
        argMax(rows_indexed, created_at) as latest_rows_indexed,
        argMax(error_message, created_at) as latest_error_message
    FROM {{database}}.indexing_state
    GROUP BY mode, dataset, start_block, end_block
)
SELECT 
    mode,
    dataset,
    COUNT(*) as total_ranges,
    
    -- Status counts based on latest status per range
    countIf(latest_status = 'completed') as completed_ranges,
    countIf(latest_status = 'processing') as processing_ranges,
    countIf(latest_status = 'failed') as failed_ranges,
    countIf(latest_status = 'pending') as pending_ranges,
    
    -- Block progress
    MIN(start_block) as lowest_block_attempted,
    MAX(end_block) as highest_block_attempted,
    maxIf(end_block, latest_status = 'completed') as highest_block_completed,
    
    -- Data metrics
    SUM(latest_rows_indexed) as total_rows_indexed,
    
    -- Progress percentage
    CASE 
        WHEN COUNT(*) > 0 THEN 
            ROUND(countIf(latest_status = 'completed') / COUNT(*) * 100, 2)
        ELSE 0 
    END as completion_percentage,
    
    -- Status summary
    CASE 
        WHEN countIf(latest_status = 'completed') = COUNT(*) THEN 'complete'
        WHEN countIf(latest_status = 'processing') > 0 THEN 'in_progress'
        WHEN countIf(latest_status = 'failed') > 0 AND countIf(latest_status = 'pending') = 0 AND countIf(latest_status = 'processing') = 0 THEN 'failed'
        WHEN countIf(latest_status = 'pending') > 0 THEN 'pending'
        ELSE 'mixed'
    END as overall_status,
    
    -- Timestamps
    MIN(latest_created_at) as first_attempt,
    MAX(latest_completed_at) as last_completion
FROM latest_state
GROUP BY mode, dataset
ORDER BY mode, dataset;

-- Create new view for continuous ranges analysis
CREATE VIEW IF NOT EXISTS {{database}}.continuous_ranges AS
WITH completed_ranges AS (
    SELECT 
        mode,
        dataset,
        start_block,
        end_block,
        -- Get latest status for each range
        argMax(status, created_at) as latest_status
    FROM {{database}}.indexing_state
    GROUP BY mode, dataset, start_block, end_block
    HAVING latest_status = 'completed'
),
ranges_with_groups AS (
    SELECT 
        mode,
        dataset,
        start_block,
        end_block,
        -- Create a group identifier that changes when there's a gap
        -- This works by calculating a running sum and subtracting it from start_block
        start_block - SUM(end_block - start_block) OVER (
            PARTITION BY mode, dataset 
            ORDER BY start_block 
            ROWS UNBOUNDED PRECEDING
        ) as group_key
    FROM completed_ranges
)
SELECT 
    mode,
    dataset,
    MIN(start_block) as continuous_start,
    MAX(end_block) as continuous_end,
    COUNT(*) as num_segments,
    MAX(end_block) - MIN(start_block) as total_blocks,
    CONCAT(toString(MIN(start_block)), '-', toString(MAX(end_block))) as range_description,
    
    -- Calculate coverage statistics
    ROUND(COUNT(*) * 100.0 / (MAX(end_block) - MIN(start_block)) * 100, 2) as coverage_percentage,
    
    -- Identify potential gaps (when num_segments doesn't match expected segments)
    CASE 
        WHEN COUNT(*) = (MAX(end_block) - MIN(start_block)) / 100 THEN 'complete'
        WHEN COUNT(*) < (MAX(end_block) - MIN(start_block)) / 100 THEN 'has_gaps'
        ELSE 'overlapping'
    END as continuity_status
FROM ranges_with_groups
GROUP BY mode, dataset, group_key
ORDER BY mode, dataset, continuous_start;

-- Create a summary view that combines progress and continuity
CREATE VIEW IF NOT EXISTS {{database}}.indexing_summary AS
SELECT 
    p.mode,
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
    
    -- Continuity information
    COUNT(c.continuous_start) as continuous_segments,
    MIN(c.continuous_start) as first_continuous_block,
    MAX(c.continuous_end) as last_continuous_block,
    
    -- Gap analysis
    CASE 
        WHEN COUNT(c.continuous_start) = 1 THEN 'fully_continuous'
        WHEN COUNT(c.continuous_start) > 1 THEN CONCAT('fragmented_', toString(COUNT(c.continuous_start)), '_segments')
        ELSE 'no_completed_data'
    END as continuity_description
    
FROM {{database}}.indexing_progress p
LEFT JOIN {{database}}.continuous_ranges c 
    ON p.mode = c.mode AND p.dataset = c.dataset
GROUP BY 
    p.mode, p.dataset, p.total_ranges, p.completed_ranges, p.processing_ranges, 
    p.failed_ranges, p.pending_ranges, p.completion_percentage, p.overall_status,
    p.highest_block_completed, p.total_rows_indexed
ORDER BY p.mode, p.dataset;

INSERT INTO {{database}}.migrations (name) VALUES ('012_create_indexing_state');