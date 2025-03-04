-- ERC721 Transfers table
CREATE TABLE IF NOT EXISTS {{database}}.erc721_transfers
(
    `block_number` UInt32,
    `block_hash` Nullable(String),
    `transaction_index` UInt32,
    `log_index` UInt32,
    `transaction_hash` String,
    `erc20` String,
    `from_address` String,
    `to_address` String,
    `token_id` String,
    `chain_id` UInt64
)
ENGINE = MergeTree()
ORDER BY (block_number, transaction_index, log_index);

-- Insert migration record
INSERT INTO {{database}}.migrations (name) VALUES ('17_erc721_transfers');