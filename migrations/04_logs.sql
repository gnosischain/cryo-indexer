-- Logs table
CREATE TABLE IF NOT EXISTS {{database}}.logs
(
    `block_number` UInt32,
    `block_hash` Nullable(String),
    `transaction_index` UInt32,
    `log_index` UInt32,
    `transaction_hash` String,
    `address` String,
    `topic0` Nullable(String),
    `topic1` Nullable(String),
    `topic2` Nullable(String),
    `topic3` Nullable(String),
    `data` String,
    `n_data_bytes` UInt32,
    `chain_id` UInt64
)
ENGINE = MergeTree()
ORDER BY (block_number, transaction_index, log_index);

-- Insert migration record
INSERT INTO {{database}}.migrations (name) VALUES ('04_logs');