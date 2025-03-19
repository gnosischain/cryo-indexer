-- Code Diffs table
CREATE TABLE IF NOT EXISTS {{database}}.code_diffs
(
    `block_number` Nullable(UInt32),
    `transaction_index` Nullable(UInt32),
    `transaction_hash` Nullable(String),
    `address` Nullable(String),
    `from_value` Nullable(String),
    `to_value` Nullable(String),
    `chain_id` Nullable(UInt64),
    `block_timestamp` DateTime64(0, 'UTC') MATERIALIZED toDateTime64(addSeconds(
        toDateTime((SELECT genesis_timestamp FROM {{database}}.chain_metadata WHERE network_name = 'gnosis' LIMIT 1)),
        coalesce(block_number, 0) * (SELECT seconds_per_block FROM {{database}}.chain_metadata WHERE network_name = 'gnosis' LIMIT 1)
    ), 0, 'UTC'),
    `month` String MATERIALIZED formatDateTime(block_timestamp, '%Y-%m', 'UTC')
)
ENGINE = ReplacingMergeTree()
PARTITION BY month
ORDER BY (block_number, transaction_index)
SETTINGS allow_nullable_key = 1;