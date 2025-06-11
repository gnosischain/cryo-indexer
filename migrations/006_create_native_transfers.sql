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
    `block_timestamp` DateTime64(0, 'UTC'),
    `insert_version` UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
)
ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(block_timestamp)
ORDER BY (block_number, transfer_index)
SETTINGS allow_nullable_key = 1;


INSERT INTO {{database}}.migrations (name) VALUES ('006_create_native_transfers');