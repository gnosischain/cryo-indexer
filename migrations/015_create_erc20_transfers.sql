-- ERC20 Transfers table
CREATE TABLE IF NOT EXISTS {{database}}.erc20_transfers
(
    `block_number` Nullable(UInt32),
    `block_hash` Nullable(String),
    `transaction_index` Nullable(UInt32),
    `log_index` Nullable(UInt32),
    `transaction_hash` Nullable(String),
    `erc20` Nullable(String),
    `from_address` Nullable(String),
    `to_address` Nullable(String),
    `value` Nullable(String),
    `chain_id` Nullable(UInt64)
)
ENGINE = MergeTree()
ORDER BY (block_number, transaction_index, log_index)
SETTINGS allow_nullable_key = 1;