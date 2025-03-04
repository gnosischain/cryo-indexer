-- Nonce Diffs table
CREATE TABLE IF NOT EXISTS {{database}}.nonce_diffs
(
    `block_number` Nullable(UInt32),
    `transaction_index` Nullable(UInt32),
    `transaction_hash` Nullable(String),
    `address` String,
    `from_value` UInt64,
    `to_value` UInt64,
    `chain_id` UInt64
)
ENGINE = MergeTree()
ORDER BY (block_number, transaction_index);

-- Insert migration record
INSERT INTO {{database}}.migrations (name) VALUES ('10_nonce_diffs');