-- Storage Reads table
CREATE TABLE IF NOT EXISTS {{database}}.storage_reads
(
    `block_number` Nullable(UInt32),
    `transaction_index` Nullable(UInt32),
    `transaction_hash` Nullable(String),
    `contract_address` String,
    `slot` String,
    `value` String,
    `chain_id` UInt64
)
ENGINE = MergeTree()
ORDER BY (block_number, transaction_index);

-- Insert migration record
INSERT INTO {{database}}.migrations (name) VALUES ('14_storage_reads');