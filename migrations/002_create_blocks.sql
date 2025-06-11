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
    `block_timestamp` DateTime MATERIALIZED toDateTime(timestamp),
    `insert_version` UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
)
ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(block_timestamp)
ORDER BY block_number
SETTINGS allow_nullable_key = 1,
replicated_deduplication_window         = 10,   
replicated_deduplication_window_seconds = 900; 


INSERT INTO {{database}}.migrations (name) VALUES ('002_create_blocks');