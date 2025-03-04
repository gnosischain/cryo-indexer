-- Contracts table
CREATE TABLE IF NOT EXISTS {{database}}.contracts
(
    `block_number` UInt32,
    `block_hash` String,
    `create_index` UInt32,
    `transaction_hash` Nullable(String),
    `contract_address` String,
    `deployer` String,
    `factory` String,
    `init_code` String,
    `code` String,
    `init_code_hash` String,
    `n_init_code_bytes` UInt32,
    `n_code_bytes` UInt32,
    `code_hash` String,
    `chain_id` UInt64
)
ENGINE = MergeTree()
ORDER BY (block_number, create_index);

-- Insert migration record
INSERT INTO {{database}}.migrations (name) VALUES ('05_contracts');