-- Native Transfers table
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
    `block_timestamp` DateTime64(0, 'UTC') MATERIALIZED toDateTime64(addSeconds(
        toDateTime((SELECT genesis_timestamp FROM {{database}}.chain_metadata WHERE network_name = 'gnosis' LIMIT 1)),
        coalesce(block_number, 0) * (SELECT seconds_per_block FROM {{database}}.chain_metadata WHERE network_name = 'gnosis' LIMIT 1)
    ), 0, 'UTC'),
    `month` String MATERIALIZED formatDateTime(block_timestamp, '%Y-%m', 'UTC')
)
ENGINE = ReplacingMergeTree()
PARTITION BY month
ORDER BY (block_number, transfer_index)
SETTINGS allow_nullable_key = 1;