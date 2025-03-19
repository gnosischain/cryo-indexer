CREATE TABLE IF NOT EXISTS {{database}}.blocks
(
    `block_number` Nullable(UInt32),
    `block_hash` Nullable(String),
    `parent_hash` Nullable(String),
    `uncles_hash` Nullable(String),
    `author` Nullable(String),
    `state_root` Nullable(String),
    `transactions_root` Nullable(String),
    `receipts_root` Nullable(String),
    `gas_used` Nullable(UInt64),
    `gas_limit` Nullable(UInt64),
    `extra_data` Nullable(String),
    `logs_bloom` Nullable(String),
    `timestamp` Nullable(UInt32),
    `difficulty` Nullable(UInt64),
    `total_difficulty` Nullable(String),
    `size` Nullable(UInt64),
    `mix_hash` Nullable(String),
    `nonce` Nullable(String),
    `base_fee_per_gas` Nullable(UInt64),
    `withdrawals_root` Nullable(String),
    `chain_id` Nullable(UInt64),
    `block_timestamp` DateTime64(0, 'UTC') MATERIALIZED toDateTime64(addSeconds(
        toDateTime((SELECT genesis_timestamp FROM {{database}}.chain_metadata WHERE network_name = 'gnosis' LIMIT 1)),
        coalesce(block_number, 0) * (SELECT seconds_per_block FROM {{database}}.chain_metadata WHERE network_name = 'gnosis' LIMIT 1)
    ), 0, 'UTC'),
    `month` String MATERIALIZED formatDateTime(block_timestamp, '%Y-%m', 'UTC')
)
ENGINE = ReplacingMergeTree()
PARTITION BY month
ORDER BY block_number
SETTINGS allow_nullable_key = 1;