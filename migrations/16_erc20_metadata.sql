-- ERC20 Metadata table
CREATE TABLE IF NOT EXISTS {{database}}.erc20_metadata
(
    `block_number` UInt32,
    `erc20` String,
    `name` Nullable(String),
    `symbol` Nullable(String),
    `decimals` Nullable(UInt32),
    `chain_id` UInt64
)
ENGINE = MergeTree()
ORDER BY (erc20, block_number);

-- Insert migration record
INSERT INTO {{database}}.migrations (name) VALUES ('16_erc20_metadata');