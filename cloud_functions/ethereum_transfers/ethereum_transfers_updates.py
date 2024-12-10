"""
Retrieves new Ethereum blockchain transfer data from the BigQuery public tables, loads
it to the ethereum_tranfers schema, and updates etl_pipelines.ethereum_net_transfers to
add the newest tables.
"""
from datetime import date
# import functions_framework
from dreams_core import core as dc
from google.cloud import bigquery
from datetime import datetime, date, timedelta
from typing import List,Dict
import pytz

# set up logger at the module level
logger = dc.setup_logger()


def get_cohort_latest_dates(client: bigquery.Client) -> List[Dict]:
    """
    Get the latest data date for each cohort based on table names.

    Args:
        client: Authenticated BigQuery client

    Returns:
        List of dictionaries containing latest dates for each cohort with structure:
        {
            'cohort_number': int,
            'latest_data_date': date,
            'last_table_id': str
        }
    """
    query = """
    WITH latest_tables AS (
        SELECT
            table_id,
            CAST(SPLIT(table_id, '_')[OFFSET(2)] AS INT64) as cohort_number,
            PARSE_DATE('%Y%m%d', SPLIT(table_id, '_')[OFFSET(4)]) as end_date,
            ROW_NUMBER() OVER (
                PARTITION BY CAST(SPLIT(table_id, '_')[OFFSET(2)] AS INT64)
                ORDER BY SPLIT(table_id, '_')[OFFSET(4)] DESC
            ) as rn
        FROM `ethereum_transfers.__TABLES__`
        WHERE table_id LIKE 'transfers_cohort_%'
    ),
    all_cohorts AS (
        SELECT DISTINCT
            cohort_number
        FROM etl_pipelines.ethereum_transfers_cohorts
    )

    SELECT
        ac.cohort_number,
        lt.table_id as last_table_id,
        lt.end_date as latest_data_date
    FROM all_cohorts ac
    LEFT JOIN latest_tables lt ON ac.cohort_number = lt.cohort_number AND lt.rn = 1
    ORDER BY ac.cohort_number
    """

    results = []
    query_job = client.query(query)

    for row in query_job:
        cohort_data = {
            'cohort_number': row.cohort_number,
            'latest_data_date': row.latest_data_date or date(2000, 1, 1),
            'last_table_id': row.last_table_id or "None"
        }
        results.append(cohort_data)

    return results


def generate_transfer_table_query(cohort_number: int, start_date: date, end_date: date) -> str:
    """
    Generate the SQL query for creating a transfer table for a specific cohort and date range.

    Args:
        cohort_number: The cohort number
        start_date: Start date for the data
        end_date: End date for the data

    Returns:
        SQL query string
    """
    table_name = (f"ethereum_transfers.transfers_cohort_{cohort_number}_"
                 f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}")

    # Format dates as 'YYYY-MM-DD' for BigQuery
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    query = f"""
    CREATE OR REPLACE TABLE {table_name}
    PARTITION BY date
    CLUSTER BY token_address AS (

    WITH cohort_coins_list AS (
        SELECT LOWER(address) AS address
        FROM etl_pipelines.ethereum_transfers_cohorts
        WHERE cohort_number = {cohort_number}
        GROUP BY 1
    ),
    transfers_filtered AS (
        SELECT t.*
        FROM `bigquery-public-data.crypto_ethereum.token_transfers` t
        JOIN cohort_coins_list cl ON cl.address = LOWER(t.token_address)
        WHERE CAST(block_timestamp AS DATE) BETWEEN '{start_date_str}' AND '{end_date_str}'
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
    """

    return query


def create_new_transfer_tables(client: bigquery.Client, cohort_statuses: List[Dict],target_sync_date: date
                               ) -> List[Dict]:
    """
    Create new transfer tables for cohorts that need updates.

    Args:
        client: BigQuery client
        cohort_statuses: List of dictionaries containing cohort status info
        target_sync_date: Date to sync data up to

    Returns:
        List of dictionaries containing information about created tables
    """
    logger.info(f"Starting transfer table creation process. Target sync date: {target_sync_date}")

    results = []

    for cohort in cohort_statuses:
        if cohort['latest_data_date'] >= target_sync_date:
            continue

        # Define date range for new table
        start_date = cohort['latest_data_date'] + timedelta(days=1)
        end_date = target_sync_date

        # Generate and execute query
        try:
            query = generate_transfer_table_query(
                cohort_number=cohort['cohort_number'],
                start_date=start_date,
                end_date=end_date
            )

            # Execute query
            query_job = client.query(query)
            query_job.result()  # Wait for query to complete

            # Record result
            result = {
                'cohort_number': cohort['cohort_number'],
                'start_date': start_date,
                'end_date': end_date,
                'status': 'success',
                'error': None
            }

        except Exception as e:
            result = {
                'cohort_number': cohort['cohort_number'],
                'start_date': start_date,
                'end_date': end_date,
                'status': 'failed',
                'error': str(e)
            }

        results.append(result)

    return results
