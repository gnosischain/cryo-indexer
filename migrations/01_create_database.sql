-- Create the database if it doesn't exist
CREATE DATABASE IF NOT EXISTS {{database}};

-- Create a table to track migrations
CREATE TABLE IF NOT EXISTS {{database}}.migrations
(
    `name` String,
    `executed_at` DateTime DEFAULT now(),
    `success` UInt8 DEFAULT 1
)
ENGINE = MergeTree()
ORDER BY (name);

-- Insert migration record
INSERT INTO {{database}}.migrations (name) VALUES ('01_create_database');