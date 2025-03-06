-- Contracts table
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
    `chain_id` Nullable(UInt64)
)
ENGINE = MergeTree()
ORDER BY (block_number, create_index)
SETTINGS allow_nullable_key = 1;