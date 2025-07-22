CREATE TABLE IF NOT EXISTS {{database}}.withdrawals
(
    `block_number` Nullable(UInt32),
    `block_hash` Nullable(String),
    `withdrawals_root` Nullable(String),
    `withdrawal_index` Nullable(String),
    `validator_index` Nullable(String),
    `address` Nullable(String),
    `amount` Nullable(String),
    `chain_id` Nullable(UInt64),
    `block_timestamp` DateTime64(0, 'UTC'),
    `insert_version` UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
)
ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(block_timestamp)
ORDER BY (block_number, withdrawal_index)
SETTINGS allow_nullable_key = 1;


INSERT INTO {{database}}.migrations (name) VALUES ('013_create_withdrawals');