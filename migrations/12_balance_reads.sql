-- Balance Reads table
CREATE TABLE IF NOT EXISTS {{database}}.balance_reads
(
    `block_number` Nullable(UInt32),
    `transaction_index` Nullable(UInt32),
    `transaction_hash` Nullable(String),
    `address` String,
    `balance` String,
    `chain_id` UInt64
)
ENGINE = MergeTree()
ORDER BY (block_number, transaction_index);

-- Insert migration record
INSERT INTO {{database}}.migrations (name) VALUES ('12_balance_reads');