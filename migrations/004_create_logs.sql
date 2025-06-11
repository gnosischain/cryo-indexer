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
    `block_timestamp` DateTime64(0, 'UTC'),
    `insert_version` UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
)
ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(block_timestamp)
ORDER BY (block_number, transaction_index, log_index)
SETTINGS allow_nullable_key = 1;


INSERT INTO {{database}}.migrations (name) VALUES ('004_create_logs');