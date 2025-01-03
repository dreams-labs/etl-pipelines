"""
Retrieves new Ethereum blockchain transfer data from the BigQuery public tables, loads
it to the ethereum_tranfers schema, and updates etl_pipelines.ethereum_net_transfers to
add the newest tables.
"""
from datetime import datetime, date, timedelta
from typing import List, Dict
import functions_framework
from dreams_core import core as dc
from google.cloud import bigquery
from google.api_core import exceptions
import pytz

# set up logger at the module level
logger = dc.setup_logger()


# -----------------------------------
#           Core Interface
# -----------------------------------

@functions_framework.http
def update_ethereum_transfers(request):  # pylint:disable=unused-argument
    """
    Main orchestration function for ethereum transfers ETL.
    Handles the complete process of:
    1. Checking which cohorts need updates
    2. Creating new transfer tables
    3. Updating the ethereum_net_transfers view
    """
    client = bigquery.Client()

    try:
        logger.info("Starting ethereum transfers ETL process...")

        # Step 1: Get current status of cohorts
        logger.debug("Checking cohort statuses...")
        cohort_statuses = get_cohort_latest_dates(client)

        # Calculate target sync date (2 days ago in UTC)
        target_sync_date = (datetime.now(pytz.UTC) - timedelta(days=2)).date()
        logger.info(f"Target sync date set to {target_sync_date}.")

        # Log current status
        for cohort in cohort_statuses:
            logger.debug(
                f"Cohort {cohort['cohort_number']}: "
                f"Latest data date = {cohort['latest_data_date']}, "
                f"Last table = {cohort['last_table_id']}"
            )

        # Step 2: Create new tables where needed
        logger.info("Creating new transfer tables if necessary...")
        table_results = create_new_transfer_tables(client, cohort_statuses, target_sync_date)

        # Log table creation results
        for result in table_results:
            if result['status'] == 'success':
                logger.info(
                    f"Successfully created table for cohort {result['cohort_number']} "
                    f"({result['start_date']} to {result['end_date']})"
                )
            else:
                logger.error(
                    f"Failed to create table for cohort {result['cohort_number']}: "
                    f"{result['error']}"
                )

        # Step 3: Update the view if any tables were created
        if any(result['status'] == 'success' for result in table_results):
            logger.info("Updating ethereum_net_transfers view...")
            view_result = update_ethereum_transfers_view(client)

            if view_result['status'] == 'success':
                logger.info(
                    f"Successfully updated view with {view_result['tables_included']} tables"
                )
            else:
                logger.error(f"Failed to update view: {view_result['error']}")
        else:
            logger.info("No new tables created, skipping view update")
            view_result = {'status': 'skipped', 'message': 'No new tables created'}

        logger.info("ETL process completed")

        # Add return statement here with execution details
        return ({
            'status': 'success',
            'message': 'ETL process completed successfully',
            'details': {
                'tables_created': table_results,
                'view_update': view_result
            }
        }, 200)

    except exceptions.GoogleAPIError as e:
        logger.error(f"BigQuery API error: {str(e)}")
        return {
            'status': 'error',
            'message': 'Database operation failed',
            'details': {'error': str(e)}
        }, 500

    except ValueError as e:
        logger.error(f"Value error in ETL process: {str(e)}")
        return {
            'status': 'error',
            'message': 'Invalid data encountered',
            'details': {'error': str(e)}
        }, 400

    except Exception as e:  # pylint: disable=broad-except
        # Top-level handler for unexpected errors to prevent function crashes
        logger.critical(f"Unexpected error in ETL process: {str(e)}")
        return {
            'status': 'error',
            'message': 'Internal server error',
            'details': {'error': str(e)}
        }, 500

    finally:
        client.close()
        logger.info("Closed BigQuery client")



# -----------------------------------
#          Helper Functions
# -----------------------------------

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
        logger.debug(f"Processing cohort {cohort['cohort_number']}...")

        if cohort['latest_data_date'] >= target_sync_date:
            logger.info(f"Cohort {cohort['cohort_number']} is up to date. Skipping.")
            continue

        # Define date range for new table
        start_date = cohort['latest_data_date'] + timedelta(days=1)
        end_date = target_sync_date

        logger.info(f"Creating new table for cohort {cohort['cohort_number']} "
                   f"from {start_date} to {end_date}")

        # Generate and execute query
        try:
            query = generate_transfer_table_query(
                cohort_number=cohort['cohort_number'],
                start_date=start_date,
                end_date=end_date
            )

            logger.debug(f"Executing query for cohort {cohort['cohort_number']}")
            # Execute query
            query_job = client.query(query)
            query_job.result()  # Wait for query to complete

            logger.info(f"Successfully created table for cohort {cohort['cohort_number']}")

            # Record result
            result = {
                'cohort_number': cohort['cohort_number'],
                'start_date': start_date,
                'end_date': end_date,
                'status': 'success',
                'error': None
            }

        except Exception as e:  # pylint:disable=broad-exception-caught
            logger.error(f"Error creating table for cohort {cohort['cohort_number']}: {str(e)}")
            result = {
                'cohort_number': cohort['cohort_number'],
                'start_date': start_date,
                'end_date': end_date,
                'status': 'failed',
                'error': str(e)
            }

        results.append(result)

    logger.info("Completed transfer table creation process")
    return results


def update_ethereum_transfers_view(client: bigquery.Client) -> Dict:
    """
    Updates the ethereum_net_transfers view to include all existing transfer tables.

    Args:
        client: BigQuery client

    Returns:
        Dict with status of the operation
    """
    try:
        # Get all existing transfer tables
        logger.info("Querying for existing transfer tables")
        tables_query = """
        SELECT table_id
        FROM `ethereum_transfers.__TABLES__`
        WHERE table_id LIKE 'transfers_cohort_%'
        ORDER BY table_id
        """

        tables = client.query(tables_query).result()
        table_ids = [row.table_id for row in tables]

        if not table_ids:
            raise ValueError("No transfer tables found")

        logger.info(f"Found {len(table_ids)} transfer tables")

        # Generate the view SQL with line breaks after UNION ALL
        union_queries = [f"    SELECT * FROM ethereum_transfers.{table_id}" for table_id in table_ids]
        formatted_unions = " UNION ALL\n".join(union_queries)

        view_query = f"""
        CREATE OR REPLACE VIEW etl_pipelines.ethereum_net_transfers AS (

        WITH all_transfers as (
        {formatted_unions}
        )

        SELECT t.date
        ,t.token_address
        ,t.wallet_address
        ,t.amount/pow(10,c.decimals) AS amount
        FROM all_transfers t
        JOIN `etl_pipelines.ethereum_transfers_cohorts` c ON c.address=t.token_address

        )
        """

        # Execute the view creation
        logger.info("Executing view update query")
        query_job = client.query(view_query)
        query_job.result()

        return {
            'status': 'success',
            'tables_included': len(table_ids),
            'table_list': table_ids
        }

    except Exception as e:  # pylint:disable=broad-exception-caught
        logger.error(f"Error updating transfers view: {str(e)}")
        return {
            'status': 'failed',
            'error': str(e)
        }
