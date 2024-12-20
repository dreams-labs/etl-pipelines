-- Creates copies of seven core schema tables in the dev_core schema. These tables
-- are filtered to only include the coin_ids in reference.dev_coins.

-- Create dev_core.coins
CREATE OR REPLACE TABLE dev_core.coins
AS
SELECT *
FROM core.coins
WHERE coin_id IN (SELECT coin_id FROM reference.dev_coins);

-- Create dev_core.chains
CREATE OR REPLACE TABLE dev_core.chains
AS
SELECT *
FROM core.chains;


-- Create dev_core.coin_facts_coingecko
CREATE OR REPLACE TABLE dev_core.coin_facts_coingecko
AS
SELECT *
FROM core.coin_facts_coingecko
WHERE coin_id IN (SELECT coin_id FROM reference.dev_coins);

-- Create dev_core.coin_facts_metadata
CREATE OR REPLACE TABLE dev_core.coin_facts_metadata
AS
SELECT *
FROM core.coin_facts_metadata
WHERE coin_id IN (SELECT coin_id FROM reference.dev_coins);


-- Create dev_core.coin_market_data
CREATE OR REPLACE TABLE dev_core.coin_market_data
PARTITION BY DATE(date)
CLUSTER BY coin_id
AS
SELECT *
FROM core.coin_market_data
WHERE coin_id IN (SELECT coin_id FROM reference.dev_coins);

-- Create dev_core.coin_wallet_transfers
CREATE OR REPLACE TABLE dev_core.coin_wallet_transfers
PARTITION BY DATE(date)
CLUSTER BY coin_id, token_address
AS
SELECT *
FROM core.coin_wallet_transfers
WHERE coin_id IN (SELECT coin_id FROM reference.dev_coins);

-- Create dev_core.coin_wallet_profits
CREATE OR REPLACE TABLE dev_core.coin_wallet_profits
PARTITION BY DATE(date)
CLUSTER BY coin_id, wallet_address
AS
SELECT *
FROM core.coin_wallet_profits
WHERE coin_id IN (SELECT coin_id FROM reference.dev_coins);





-- Query to retrieve counts of the dev_core tables
SELECT 'dev_core.coins' AS table_name, COUNT(*) AS row_count
FROM dev_core.coins

UNION ALL

SELECT 'dev_core.chains' AS table_name, COUNT(*) AS row_count
FROM dev_core.chains

UNION ALL

SELECT 'dev_core.coin_facts_coingecko' AS table_name, COUNT(*) AS row_count
FROM dev_core.coin_facts_coingecko

UNION ALL

SELECT 'dev_core.coin_facts_metadata' AS table_name, COUNT(*) AS row_count
FROM dev_core.coin_facts_metadata

UNION ALL

SELECT 'dev_core.coin_market_data' AS table_name, COUNT(*) AS row_count
FROM dev_core.coin_market_data

UNION ALL

SELECT 'dev_core.coin_wallet_transfers' AS table_name, COUNT(*) AS row_count
FROM dev_core.coin_wallet_transfers

UNION ALL

SELECT 'dev_core.coin_wallet_profits' AS table_name, COUNT(*) AS row_count
FROM dev_core.coin_wallet_profits;