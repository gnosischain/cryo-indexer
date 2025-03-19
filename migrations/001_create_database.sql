-- Create a table to track migrations
CREATE TABLE IF NOT EXISTS {{database}}.migrations
(
    name String,
    executed_at DateTime DEFAULT now(),
    success UInt8 DEFAULT 1
) 
ENGINE = MergeTree()
ORDER BY name;

-- Create the chain metadata table for timestamp calculations
CREATE TABLE IF NOT EXISTS {{database}}.chain_metadata
(
    `network_name` String,
    `genesis_timestamp` UInt64,
    `seconds_per_block` UInt16,
    `chain_id` UInt64
)
ENGINE = ReplacingMergeTree()
ORDER BY network_name;

-- Insert the chain metadata for Gnosis Chain (default values)
INSERT INTO {{database}}.chain_metadata 
(network_name, genesis_timestamp, seconds_per_block, chain_id)
VALUES ('gnosis', 1539024180, 5, 100);

-- Insert migration record
INSERT INTO {{database}}.migrations (name) VALUES ('001_create_database');