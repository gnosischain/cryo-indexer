-- Create table to track timestamp fix operations (if not exists)
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


-- Record migration completion (only if not already recorded)
INSERT INTO {{database}}.migrations (name) 
SELECT '013_create_timestamp_fixes' 
WHERE NOT EXISTS (
    SELECT 1 FROM {{database}}.migrations 
    WHERE name = '013_create_timestamp_fixes'
);