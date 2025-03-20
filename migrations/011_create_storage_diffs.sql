-- Storage Diffs table
CREATE TABLE IF NOT EXISTS {{database}}.storage_diffs
(
    `block_number` Nullable(UInt32),
    `transaction_index` Nullable(UInt32),
    `transaction_hash` Nullable(String),
    `address` Nullable(String),
    `slot` Nullable(String),
    `from_value` Nullable(String),
    `to_value` Nullable(String),
    `chain_id` Nullable(UInt64),
    `block_timestamp` Nullable(DateTime64(0, 'UTC')) MATERIALIZED 
        toDateTime64(coalesce((SELECT timestamp FROM {{database}}.blocks WHERE blocks.block_number = block_number LIMIT 1), 0), 0, 'UTC'),
    `month` String MATERIALIZED formatDateTime(block_timestamp, '%Y-%m', 'UTC')
)
ENGINE = ReplacingMergeTree()
PARTITION BY month
ORDER BY (block_number, transaction_index)
SETTINGS allow_nullable_key = 1;