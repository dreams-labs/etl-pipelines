CREATE OR REPLACE VIEW etl_pipelines.ethereum_net_transfers AS (

    SELECT * FROM ethereum_transfers.transfers_cohort_1_2022

);