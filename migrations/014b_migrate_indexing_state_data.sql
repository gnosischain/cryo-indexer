-- Migration 014b: Backfill indexing_state_v2, swap tables, recreate views
-- Only runs on databases that have the OLD indexing_state (with mode column).
-- Skipped on fresh databases (execution_live) where indexing_state_old won't exist.
--
-- IMPORTANT: This migration assumes all indexers are STOPPED.
-- Run snapshot BEFORE this migration:
--   CREATE TABLE IF NOT EXISTS {{database}}.indexing_state_snapshot AS {{database}}.indexing_state;
--   INSERT INTO {{database}}.indexing_state_snapshot SELECT * FROM {{database}}.indexing_state;

-- Step 1: Backfill v2 from old table
-- When the same (dataset, start_block, end_block) exists under multiple modes,
-- 'completed' wins over 'processing' over 'failed' over 'pending'.
INSERT INTO {{database}}.indexing_state_v2
    (dataset, start_block, end_block, status, worker_id,
     attempt_count, created_at, completed_at, rows_indexed, error_message)
SELECT
    dataset,
    start_block,
    end_block,
    argMax(status, priority) as status,
    argMax(worker_id, priority) as worker_id,
    max(attempt_count) as attempt_count,
    argMax(created_at, priority) as created_at,
    argMax(completed_at, priority) as completed_at,
    argMax(rows_indexed, priority) as rows_indexed,
    argMax(error_message, priority) as error_message
FROM (
    SELECT *,
        multiIf(
            status = 'completed', 4,
            status = 'processing', 3,
            status = 'failed', 2,
            1
        ) as priority
    FROM {{database}}.indexing_state
)
GROUP BY dataset, start_block, end_block;

-- Step 2: Drop old views (they reference the old table schema with mode)
DROP VIEW IF EXISTS {{database}}.indexing_summary;
DROP VIEW IF EXISTS {{database}}.continuous_ranges;
DROP VIEW IF EXISTS {{database}}.indexing_progress;

-- Step 3: Rename tables (atomic swap)
RENAME TABLE {{database}}.indexing_state TO {{database}}.indexing_state_old;
RENAME TABLE {{database}}.indexing_state_v2 TO {{database}}.indexing_state;

-- Step 4: Recreate indexing_progress view (no mode)
CREATE VIEW IF NOT EXISTS {{database}}.indexing_progress AS
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

-- Step 5: Recreate continuous_ranges view (no mode)
CREATE VIEW IF NOT EXISTS {{database}}.continuous_ranges AS
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

-- Step 6: Recreate indexing_summary view (no mode)
CREATE VIEW IF NOT EXISTS {{database}}.indexing_summary AS
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

INSERT INTO {{database}}.migrations (name) VALUES ('014b_migrate_indexing_state_data');
