-- Code Reads table
CREATE TABLE IF NOT EXISTS {{database}}.code_reads
(
    `block_number` Nullable(UInt32),
    `transaction_index` Nullable(UInt32),
    `transaction_hash` Nullable(String),
    `contract_address` Nullable(String),
    `code` Nullable(String),
    `chain_id` Nullable(UInt64)
)
ENGINE = MergeTree()
ORDER BY (block_number, transaction_index)
SETTINGS allow_nullable_key = 1;