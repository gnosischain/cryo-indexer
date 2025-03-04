-- Blocks table
CREATE TABLE IF NOT EXISTS {{database}}.blocks
(
    `block_number` UInt32,
    `block_hash` String,
    `parent_hash` String,
    `uncles_hash` String,
    `author` String,
    `state_root` String,
    `transactions_root` String,
    `receipts_root` String,
    `gas_used` UInt64,
    `gas_limit` UInt64,
    `extra_data` String,
    `logs_bloom` String,
    `timestamp` UInt32,
    `difficulty` UInt64,
    `total_difficulty` String,
    `size` Nullable(UInt64),
    `mix_hash` Nullable(String),
    `nonce` Nullable(String),
    `base_fee_per_gas` Nullable(UInt64),
    `withdrawals_root` Nullable(String),
    `chain_id` UInt64
)
ENGINE = MergeTree()
ORDER BY (block_number);

-- Insert migration record
INSERT INTO {{database}}.migrations (name) VALUES ('02_blocks');