CREATE TABLE IF NOT EXISTS {{database}}.logs
(
    `block_number` Nullable(UInt32),
    `transaction_index` Nullable(UInt32),
    `log_index` Nullable(UInt32),
    `transaction_hash` Nullable(String),
    `address` Nullable(String),
    `topic0` Nullable(String),
    `topic1` Nullable(String),
    `topic2` Nullable(String),
    `topic3` Nullable(String),
    `data` Nullable(String),
    `n_data_bytes` Nullable(UInt32),
    `chain_id` Nullable(UInt64),
    `block_timestamp` DateTime64(0, 'UTC')
)
ENGINE = ReplacingMergeTree()
PARTITION BY toStartOfMonth(block_timestamp)
ORDER BY (block_number, transaction_index, log_index)
SETTINGS allow_nullable_key = 1;