CREATE TABLE IF NOT EXISTS {{database}}.balance_diffs
(
    `block_number` Nullable(UInt32),
    `transaction_index` Nullable(UInt32),
    `transaction_hash` Nullable(String),
    `address` Nullable(String),
    `from_value_binary` Nullable(String),
    `from_value_string` Nullable(String),
    `from_value_f64` Nullable(Float64),
    `to_value_binary` Nullable(String),
    `to_value_string` Nullable(String),
    `to_value_f64` Nullable(Float64),
    `chain_id` Nullable(UInt64),
    `block_timestamp` DateTime64(0, 'UTC')
)
ENGINE = ReplacingMergeTree()
PARTITION BY toStartOfMonth(block_timestamp)
ORDER BY (block_number, transaction_index)
SETTINGS allow_nullable_key = 1;