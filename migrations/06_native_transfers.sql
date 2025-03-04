-- Native Transfers table
CREATE TABLE IF NOT EXISTS {{database}}.native_transfers
(
    `block_number` UInt32,
    `block_hash` String,
    `transaction_index` Nullable(UInt32),
    `transfer_index` UInt32,
    `transaction_hash` Nullable(String),
    `from_address` String,
    `to_address` String,
    `value` String,
    `chain_id` UInt64
)
ENGINE = MergeTree()
ORDER BY (block_number, transfer_index);

-- Insert migration record
INSERT INTO {{database}}.migrations (name) VALUES ('06_native_transfers');