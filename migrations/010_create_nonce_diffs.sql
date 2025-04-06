CREATE TABLE IF NOT EXISTS {{database}}.nonce_diffs
(
    `block_number` Nullable(UInt32),
    `transaction_index` Nullable(UInt32),
    `transaction_hash` Nullable(String),
    `address` Nullable(String),
    `from_value` Nullable(UInt64),
    `to_value` Nullable(UInt64),
    `chain_id` Nullable(UInt64),
    `block_timestamp` DateTime64(0, 'UTC')
)
ENGINE = ReplacingMergeTree()
PARTITION BY toStartOfMonth(block_timestamp)
ORDER BY (block_number, transaction_index)
SETTINGS allow_nullable_key = 1;