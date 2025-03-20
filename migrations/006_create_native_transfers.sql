-- Native Transfers table
CREATE TABLE IF NOT EXISTS {{database}}.native_transfers
(
    `block_number` Nullable(UInt32),
    `block_hash` Nullable(String),
    `transaction_index` Nullable(UInt32),
    `transfer_index` Nullable(UInt32),
    `transaction_hash` Nullable(String),
    `from_address` Nullable(String),
    `to_address` Nullable(String),
    `value_binary` Nullable(String),
    `value_string` Nullable(String),
    `value_f64` Nullable(Float64),
    `chain_id` Nullable(UInt64),
    `block_timestamp` Nullable(DateTime64(0, 'UTC')) MATERIALIZED 
        toDateTime64(coalesce((SELECT timestamp FROM {{database}}.blocks WHERE blocks.block_number = block_number LIMIT 1), 0), 0, 'UTC'),
    `month` String MATERIALIZED formatDateTime(block_timestamp, '%Y-%m', 'UTC')
)
ENGINE = ReplacingMergeTree()
PARTITION BY month
ORDER BY (block_number, transfer_index)
SETTINGS allow_nullable_key = 1;