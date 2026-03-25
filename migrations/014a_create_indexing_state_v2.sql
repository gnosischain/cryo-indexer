-- Migration 014a: Create indexing_state_v2 (mode-independent)
-- This migration creates the new table only. Backfill and swap happen in 014b.
-- Safe to run on both existing (execution) and fresh (execution_live) databases.

CREATE TABLE IF NOT EXISTS {{database}}.indexing_state_v2
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

INSERT INTO {{database}}.migrations (name) VALUES ('014a_create_indexing_state_v2');
