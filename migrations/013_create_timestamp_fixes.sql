-- Create table to track timestamp fix operations
CREATE TABLE IF NOT EXISTS {{database}}.timestamp_fixes
(
    `table_name` String,
    `total_affected` UInt64,
    `total_fixed` UInt64,
    `status` String,  -- 'completed', 'partial', 'failed'
    `created_at` DateTime DEFAULT now(),
    `error_message` String DEFAULT '',
    `insert_version` UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
)
ENGINE = ReplacingMergeTree(insert_version)
ORDER BY (table_name, created_at)
SETTINGS index_granularity = 8192;

-- Create index for quick lookups
ALTER TABLE {{database}}.timestamp_fixes
ADD INDEX idx_status (status) TYPE minmax GRANULARITY 4;

INSERT INTO {{database}}.migrations (name) VALUES ('013_create_timestamp_fixes');