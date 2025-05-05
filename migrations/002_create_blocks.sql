CREATE TABLE IF NOT EXISTS {{database}}.blocks
(
    `block_number` Nullable(UInt32),
    `block_hash` Nullable(String),
    `author` Nullable(String),
    `gas_used` Nullable(UInt64),
    `gas_limit` Nullable(UInt64),
    `extra_data` Nullable(String),
    `timestamp` Nullable(UInt32),
    `base_fee_per_gas` Nullable(UInt64),
    `chain_id` Nullable(UInt64),
    `block_timestamp` DateTime MATERIALIZED toDateTime(timestamp)
)
ENGINE = ReplacingMergeTree()
PARTITION BY toStartOfMonth(block_timestamp)
ORDER BY block_number
SETTINGS allow_nullable_key = 1,
replicated_deduplication_window         = 10,   -- last 10 parts
replicated_deduplication_window_seconds = 900;  -- or 15 minutes