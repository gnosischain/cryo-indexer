-- Logs table
CREATE TABLE IF NOT EXISTS {{database}}.logs
(
    `block_number` Nullable(UInt32),
    `block_hash` Nullable(String),
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
    `chain_id` Nullable(UInt64)
)
ENGINE = MergeTree()
ORDER BY (block_number, transaction_index, log_index)
SETTINGS allow_nullable_key = 1;