CREATE OR REPLACE VIEW etl_pipelines.ethereum_net_transfers AS (

WITH all_transfers as (
    SELECT * FROM ethereum_transfers.transfers_cohort_1_2020_and_earlier
    UNION ALL
    SELECT * FROM ethereum_transfers.transfers_cohort_1_2021
    UNION ALL
    SELECT * FROM ethereum_transfers.transfers_cohort_1_2022
    UNION ALL
    SELECT * FROM ethereum_transfers.transfers_cohort_1_2023
    UNION ALL
    SELECT * FROM ethereum_transfers.transfers_cohort_1_2024_0925
    UNION ALL
    SELECT * FROM ethereum_transfers.transfers_cohort_1_2024_1123

    UNION ALL

    SELECT * FROM ethereum_transfers.transfers_cohort_2_2020_and_earlier
    UNION ALL
    SELECT * FROM ethereum_transfers.transfers_cohort_2_2021
    UNION ALL
    SELECT * FROM ethereum_transfers.transfers_cohort_2_2022
    UNION ALL
    SELECT * FROM ethereum_transfers.transfers_cohort_2_2023
    UNION ALL
    SELECT * FROM ethereum_transfers.transfers_cohort_2_2024_1007
    UNION ALL
    SELECT * FROM ethereum_transfers.transfers_cohort_2_2024_1123

    UNION ALL

    SELECT * FROM ethereum_transfers.transfers_cohort_3_2020_and_earlier
    UNION ALL
    SELECT * FROM ethereum_transfers.transfers_cohort_3_2021
    UNION ALL
    SELECT * FROM ethereum_transfers.transfers_cohort_3_2022
    UNION ALL
    SELECT * FROM ethereum_transfers.transfers_cohort_3_2023
    UNION ALL
    SELECT * FROM ethereum_transfers.transfers_cohort_3_2024_1123
)

SELECT t.date
,t.token_address
,t.wallet_address
,t.amount/pow(10,c.decimals) AS amount
FROM all_transfers t
JOIN `etl_pipelines.ethereum_transfers_cohorts` c ON c.address=t.token_address

);
