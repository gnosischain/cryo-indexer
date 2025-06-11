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
    `block_timestamp` DateTime64(0, 'UTC'),
    `insert_version` UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
)
ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(block_timestamp)
ORDER BY (block_number, transaction_index)
SETTINGS allow_nullable_key = 1;


INSERT INTO {{database}}.migrations (name) VALUES ('011_create_storage_diffs');