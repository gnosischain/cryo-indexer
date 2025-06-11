
CREATE DATABASE IF NOT EXISTS {{database}};


CREATE TABLE IF NOT EXISTS {{database}}.migrations
(
    name String,
    executed_at DateTime DEFAULT now(),
    success UInt8 DEFAULT 1
) 
ENGINE = MergeTree()
ORDER BY name;


INSERT INTO {{database}}.migrations (name) VALUES ('001_create_database');