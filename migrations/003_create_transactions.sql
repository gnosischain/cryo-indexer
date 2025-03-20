-- Transactions table
CREATE TABLE IF NOT EXISTS {{database}}.transactions
(
    `block_number` Nullable(UInt32),
    `transaction_index` Nullable(UInt64),
    `transaction_hash` Nullable(String),
    `nonce` Nullable(UInt64),
    `from_address` Nullable(String),
    `to_address` Nullable(String),
    `value_binary` Nullable(String),
    `value_string` Nullable(String),
    `value_f64` Nullable(Float64),
    `input` Nullable(String),
    `gas_limit` Nullable(UInt64),
    `gas_used` Nullable(UInt64),
    `gas_price` Nullable(UInt64),
    `transaction_type` Nullable(UInt32),
    `max_priority_fee_per_gas` Nullable(UInt64),
    `max_fee_per_gas` Nullable(UInt64),
    `success` Nullable(UInt8),
    `n_input_bytes` Nullable(UInt32),
    `n_input_zero_bytes` Nullable(UInt32),
    `n_input_nonzero_bytes` Nullable(UInt32),
    `n_rlp_bytes` Nullable(UInt32),
    `block_hash` Nullable(String),
    `chain_id` Nullable(UInt64),
    `timestamp` Nullable(UInt32),
    `r` Nullable(String),
    `s` Nullable(String),
    `v` Nullable(UInt8),
    `block_timestamp` Nullable(DateTime64(0, 'UTC')) MATERIALIZED 
        toDateTime64(coalesce((SELECT timestamp FROM {{database}}.blocks WHERE blocks.block_number = block_number LIMIT 1), 0), 0, 'UTC'),
    `month` String MATERIALIZED formatDateTime(toDateTime(block_timestamp), '%Y-%m', 'UTC')
)
ENGINE = ReplacingMergeTree()
PARTITION BY month
ORDER BY (block_number, transaction_index)
SETTINGS allow_nullable_key = 1;