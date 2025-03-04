-- Transactions table
CREATE TABLE IF NOT EXISTS {{database}}.transactions
(
    `block_number` UInt32,
    `transaction_index` UInt64,
    `transaction_hash` String,
    `nonce` UInt64,
    `from_address` String,
    `to_address` Nullable(String),
    `value` String,
    `input` String,
    `gas_limit` UInt64,
    `gas_used` Nullable(UInt64),
    `gas_price` Nullable(UInt64),
    `transaction_type` UInt32,
    `max_priority_fee_per_gas` Nullable(UInt64),
    `max_fee_per_gas` Nullable(UInt64),
    `success` UInt8,
    `n_input_bytes` UInt32,
    `n_input_zero_bytes` UInt32,
    `n_input_nonzero_bytes` UInt32,
    `n_rlp_bytes` UInt32,
    `block_hash` String,
    `chain_id` UInt64,
    `timestamp` UInt32,
    `r` String,
    `s` String,
    `v` UInt8
)
ENGINE = MergeTree()
ORDER BY (block_number, transaction_index);

-- Insert migration record
INSERT INTO {{database}}.migrations (name) VALUES ('03_transactions');