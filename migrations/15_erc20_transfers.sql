-- ERC20 Transfers table
CREATE TABLE IF NOT EXISTS {{database}}.erc20_transfers
(
    `block_number` UInt32,
    `block_hash` Nullable(String),
    `transaction_index` UInt32,
    `log_index` UInt32,
    `transaction_hash` String,
    `erc20` String,
    `from_address` String,
    `to_address` String,
    `value` String,
    `chain_id` UInt64
)
ENGINE = MergeTree()
ORDER BY (block_number, transaction_index, log_index);

-- Insert migration record
INSERT INTO {{database}}.migrations (name) VALUES ('15_erc20_transfers');