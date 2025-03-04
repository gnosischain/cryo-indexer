-- Storage Diffs table
CREATE TABLE IF NOT EXISTS {{database}}.storage_diffs
(
    `block_number` Nullable(UInt32),
    `transaction_index` Nullable(UInt32),
    `transaction_hash` Nullable(String),
    `address` String,
    `slot` String,
    `from_value` String,
    `to_value` String,
    `chain_id` UInt64
)
ENGINE = MergeTree()
ORDER BY (block_number, transaction_index);

-- Insert migration record
INSERT INTO {{database}}.migrations (name) VALUES ('11_storage_diffs');