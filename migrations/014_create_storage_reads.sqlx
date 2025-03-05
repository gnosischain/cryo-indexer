-- Storage Reads table
CREATE TABLE IF NOT EXISTS {{database}}.storage_reads
(
    `block_number` Nullable(UInt32),
    `transaction_index` Nullable(UInt32),
    `transaction_hash` Nullable(String),
    `contract_address` Nullable(String),
    `slot` Nullable(String),
    `value` Nullable(String),
    `chain_id` Nullable(UInt64)
)
ENGINE = MergeTree()
ORDER BY (block_number, transaction_index)
SETTINGS allow_nullable_key = 1;