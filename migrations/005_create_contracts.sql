CREATE TABLE IF NOT EXISTS {{database}}.contracts
(
    `block_number` Nullable(UInt32),
    `block_hash` Nullable(String),
    `create_index` Nullable(UInt32),
    `transaction_hash` Nullable(String),
    `contract_address` Nullable(String),
    `deployer` Nullable(String),
    `factory` Nullable(String),
    `init_code` Nullable(String),
    `code` Nullable(String),
    `init_code_hash` Nullable(String),
    `n_init_code_bytes` Nullable(UInt32),
    `n_code_bytes` Nullable(UInt32),
    `code_hash` Nullable(String),
    `chain_id` Nullable(UInt64),
    `block_timestamp` DateTime64(0, 'UTC'),
    `insert_version` UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
)
ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(block_timestamp)
ORDER BY (block_number, create_index)
SETTINGS allow_nullable_key = 1;


INSERT INTO {{database}}.migrations (name) VALUES ('005_create_contracts');