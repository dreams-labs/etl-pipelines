CREATE OR REPLACE TABLE ethereum_transfers.transfers_cohort_1_2024_0925  -- needs to be changed
PARTITION BY date
CLUSTER BY token_address AS (

WITH cohort_coins_list AS (
    SELECT LOWER(address) AS address
    FROM etl_pipelines.ethereum_transfers_cohorts
    GROUP BY 1
),
transfers_filtered AS (
    SELECT t.*
    FROM `bigquery-public-data.crypto_ethereum.token_transfers` t
    JOIN cohort_coins_list cl ON cl.address = LOWER(t.token_address)
    WHERE CAST(block_timestamp AS DATE) BETWEEN '2024-01-01' AND '2024-09-25'  -- needs to be changed
),
transfers AS (
    -- receipts (positive)
    SELECT block_timestamp,
           to_address AS address,
           CAST(value AS FLOAT64) AS value,
           token_address
    FROM transfers_filtered

    UNION ALL

    -- sends (negative)
    SELECT block_timestamp,
           from_address AS address,
           -CAST(value AS FLOAT64) AS value,
           token_address
    FROM transfers_filtered
),
daily_net_transfers AS (
    SELECT CAST(block_timestamp AS DATE) AS date,
           address AS wallet_address,
           token_address,
           SUM(value) AS amount
    FROM transfers
    GROUP BY 1, 2, 3
    HAVING SUM(value) <> 0
)

SELECT *
FROM daily_net_transfers
)
