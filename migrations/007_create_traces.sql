CREATE TABLE IF NOT EXISTS {{database}}.traces
(
    `action_from` Nullable(String),
    `action_to` Nullable(String),
    `action_value` Nullable(String),
    `action_gas` Nullable(UInt32),
    `action_input` Nullable(String),
    `action_call_type` Nullable(String),
    `action_init` Nullable(String),
    `action_reward_type` Nullable(String),
    `action_type` Nullable(String),
    `result_gas_used` Nullable(UInt32),
    `result_output` Nullable(String),
    `result_code` Nullable(String),
    `result_address` Nullable(String),
    `trace_address` Nullable(String),
    `subtraces` Nullable(UInt32),
    `transaction_index` Nullable(UInt32),
    `transaction_hash` Nullable(String),
    `block_number` Nullable(UInt32),
    `block_hash` Nullable(String),
    `error` Nullable(String),
    `chain_id` Nullable(UInt64),
    `block_timestamp` DateTime64(0, 'UTC'),
    `insert_version` UInt64 MATERIALIZED toUnixTimestamp64Nano(now64(9))
)
ENGINE = ReplacingMergeTree(insert_version)
PARTITION BY toStartOfMonth(block_timestamp)
ORDER BY (block_number, transaction_index, trace_address)
SETTINGS allow_nullable_key = 1;


INSERT INTO {{database}}.migrations (name) VALUES ('007_create_traces');