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
    `block_timestamp` Nullable(DateTime64(0, 'UTC')),
    `month` String
)
ENGINE = ReplacingMergeTree()
PARTITION BY month
ORDER BY (block_number, transaction_index)
SETTINGS allow_nullable_key = 1;