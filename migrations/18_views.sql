-- Create materialized view for ERC20 transfers (from logs)
CREATE MATERIALIZED VIEW IF NOT EXISTS {{database}}.erc20_transfers_view
ENGINE = MergeTree()
ORDER BY (block_number, transaction_index, log_index)
AS SELECT
    block_number,
    transaction_index,
    log_index,
    transaction_hash,
    address AS token_address,
    unhexOrNull(substring(topic1, 27)) AS from_address,
    unhexOrNull(substring(topic2, 27)) AS to_address,
    data AS value,
    chain_id
FROM {{database}}.logs
WHERE topic0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'  -- keccak256('Transfer(address,address,uint256)')
    AND length(topic1) >= 27
    AND length(topic2) >= 27
    AND length(topics) = 3;

-- Create materialized view for ERC721 transfers (from logs)
CREATE MATERIALIZED VIEW IF NOT EXISTS {{database}}.erc721_transfers_view
ENGINE = MergeTree()
ORDER BY (block_number, transaction_index, log_index)
AS SELECT
    block_number,
    transaction_index,
    log_index,
    transaction_hash,
    address AS token_address,
    unhexOrNull(substring(topic1, 27)) AS from_address,
    unhexOrNull(substring(topic2, 27)) AS to_address,
    topic3 AS token_id,
    chain_id
FROM {{database}}.logs
WHERE topic0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'  -- keccak256('Transfer(address,address,uint256)')
    AND length(topic1) >= 27
    AND length(topic2) >= 27
    AND length(topics) = 4
    AND length(data) = 0;

-- User activity view - aggregates all addresses that have either sent or received transactions
CREATE MATERIALIZED VIEW IF NOT EXISTS {{database}}.user_activity
ENGINE = MergeTree()
ORDER BY (address, block_number)
AS SELECT
    address,
    min(block_number) AS first_activity_block,
    max(block_number) AS last_activity_block,
    count(*) AS tx_count,
    chain_id
FROM (
    SELECT from_address AS address, block_number, chain_id FROM {{database}}.transactions
    UNION ALL
    SELECT to_address AS address, block_number, chain_id FROM {{database}}.transactions WHERE to_address IS NOT NULL
)
GROUP BY address, chain_id;

-- Block statistics view
CREATE MATERIALIZED VIEW IF NOT EXISTS {{database}}.block_statistics
ENGINE = MergeTree()
ORDER BY (block_number)
AS SELECT
    blocks.block_number,
    blocks.timestamp,
    blocks.gas_used,
    blocks.gas_limit,
    blocks.base_fee_per_gas,
    count(transactions.transaction_hash) AS transaction_count,
    sum(transactions.gas_used) AS total_gas_used,
    avg(transactions.gas_price) AS avg_gas_price,
    blocks.chain_id
FROM {{database}}.blocks
LEFT JOIN {{database}}.transactions ON blocks.block_number = transactions.block_number
GROUP BY
    blocks.block_number,
    blocks.timestamp,
    blocks.gas_used,
    blocks.gas_limit,
    blocks.base_fee_per_gas,
    blocks.chain_id;

-- Insert migration record
INSERT INTO {{database}}.migrations (name) VALUES ('18_views');