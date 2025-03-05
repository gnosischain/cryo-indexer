-- ERC20 Metadata table
CREATE TABLE IF NOT EXISTS {{database}}.erc20_metadata
(
    `block_number` Nullable(UInt32),
    `erc20` Nullable(String),
    `name` Nullable(String),
    `symbol` Nullable(String),
    `decimals` Nullable(UInt32),
    `chain_id` Nullable(UInt64)
)
ENGINE = MergeTree()
ORDER BY (erc20, block_number)
SETTINGS allow_nullable_key = 1;