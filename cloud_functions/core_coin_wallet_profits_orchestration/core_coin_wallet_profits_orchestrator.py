"""
Cloud function that runs a query to refresh the data in bigquery table core.coin_wallet_profits
through batch calculations that are stored as temp tables.
"""
import os
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import requests
import functions_framework
import google.auth
import google.oauth2.id_token
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession
from dreams_core.googlecloud import GoogleCloud as dgc
from dreams_core import core as dc

# pylint: disable=W1203  # no f strings in logs

# set up logger at the module level
logger = dc.setup_logger()

@functions_framework.http
def orchestrate_core_coin_wallet_profits_rebuild(request):  # pylint: disable=W0613
    """
    runs all functions in sequence to refresh core.coin_wallet_profits

    Params:
    - request (flask.request): optionally should include:
        - batch_size: number of coins to be calculated in each batch
        - max_workers: maximum number of concurrent threads (default: 4)
    """
    logger.info("Beginning rebuild sequence for core.coin_wallet_profits...")

    # Drop any temp tables that still exist
    drop_temp_tables()

    # 1. Assign coins to batches, with each batch including {batch_size} coins
    batch_size = int(request.args.get('batch_size', 100))
    max_workers = int(request.args.get('max_workers', 4))
    batch_count = set_coin_batches(batch_size)


    # 2. Calculate coin_wallet_profits data for each batch using multiple threads
    worker_url = "https://core-coin-wallet-profits-954736581165.us-west1.run.app"
    session = get_auth_session(worker_url)
    failed_batches = []

    def process_single_batch(batch, session):
        logger.info("Initiating profits calculations for batch %s...", batch)
        response = session.post(worker_url, json={"batch_number": batch})
        if response.status_code != 200:
            raise Exception(f"Batch {batch} failed: {response.json().get('error')}")
        logger.info("Completed profits calculations for batch %s.", batch)

        return response.json()

    # Sequence to initiate multithreaded function calls for multiple batches at once
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_batch = {
            executor.submit(process_single_batch, batch, session): batch
            for batch in range(batch_count)
        }

        for future in concurrent.futures.as_completed(future_to_batch):
            batch = future_to_batch[future]
            try:
                result = future.result()
                logger.info(f"Completed batch {result['batch']}")
            except Exception as e:
                logger.error(str(e))
                failed_batches.append(batch)

    if failed_batches:
        raise RuntimeError(f"Failed batches: {failed_batches}")

    # 3. Rebuild core.coin_wallet_profits, optionally drop temp tables
    rebuild_core_table()
    # drop_temp_tables() # disabled for now because they're useful for auditing data

    return '{{"rebuild of core.coin_wallet_profits complete."}}'



def get_auth_session(target_url):
    """
    Creates an authenticated session that works both locally and in Cloud Run.

    Args:
        target_url (str): The URL of the target Cloud Run service

    Returns:
        requests.Session: An authenticated session
    """
    # Check if running locally (with service account key file)
    if os.getenv('GOOGLE_APPLICATION_CREDENTIALS'):
        # Local development with service account key
        credentials = service_account.IDTokenCredentials.from_service_account_file(
            os.getenv('GOOGLE_APPLICATION_CREDENTIALS'),
            target_audience=target_url
        )
        return AuthorizedSession(credentials)

    # Running in Cloud Run or GCP environment
    else:
        # Create a regular session
        session = requests.Session()

        # Get ID token for authentication
        auth_req = google.auth.transport.requests.Request()
        id_token = google.oauth2.id_token.fetch_id_token(auth_req, target_url)

        # Add the ID token to the session's headers
        session.headers.update({
            'Authorization': f'Bearer {id_token}',
            'Content-Type': 'application/json'
        })

        return session



def set_coin_batches(batch_size):
    """
    Retrieves market data from the core.coin_wallet_transfers table and converts columns to
    memory-efficient formats.

    Paramas:
    - batch_size (int): the number of coins to put in each batch

    Returns:
    - batch_count (int): total number of coin batches generated based on the batch_size
    """
    logger.debug('Retrieving coin_id list...')

    # SQL query to retrieve transfers data
    query_sql = f"""
        -- Create a temporary table with coin_ids and batch numbers
        CREATE OR REPLACE TABLE `temp.temp_coin_batches` AS
        WITH numbered_coins AS (
        SELECT
            cwt.coin_id,
            ROW_NUMBER() OVER (ORDER BY cwt.coin_id) AS row_num
        FROM
            `core.coin_wallet_transfers` cwt
        -- inner join to filter onto only coins with price data
        JOIN (
            select coin_id
            from `core.coin_market_data`
            group by 1
        ) cmd on cmd.coin_id = cwt.coin_id
        GROUP BY
            cwt.coin_id
        )
        SELECT
        coin_id,
        CAST(FLOOR((row_num - 1) / {batch_size}) AS INT64) AS batch_number,
        current_datetime() as batching_date,
        CAST(NULL AS STRING) as batch_table
        FROM
        numbered_coins
        ORDER BY
        batch_number, coin_id
        limit 5
        ;

        -- Query to get the number of batches (useful for our orchestrator)
        SELECT MAX(batch_number) + 1 AS total_batches FROM `temp.temp_coin_batches`;
        """

    # Run the SQL query using dgc's run_sql method
    batch_count_df = dgc().run_sql(query_sql)
    batch_count = batch_count_df['total_batches'][0]

    logger.info("Assigned coins to %s batches of %s in temp.temp_coin_batches.",
                batch_count, batch_size)

    return batch_count


def rebuild_core_table():
    """
    Rebuilds the core.coin_wallet_profits, replacing the existing table

    1. Confirms that all of the temp batch tables are completed
    2. Rebuilds core.coin_wallet_profits by unioning them all together

    Params: None
    Returns: None
    """
    logger.debug("Rebuilding core.coin_wallet_profits table...")

    # 1. Confirm that all batches have been completed
    completeness_check_sql = """
        select count(distinct batch_number) as missing_batches
        from temp.temp_coin_batches
        where batch_table is null
        """

    completeness_df = dgc().run_sql(completeness_check_sql)

    if completeness_df['missing_batches'][0] > 0:
        raise RuntimeError(
            f"Batch generation incomplete: %s {completeness_df['missing_batches'][0]} batches "
            "are missing. Aborting core.coin_wallet_profits update sequence.")


    # 2. Rebuild core.coin_wallet_profits
    rebuild_cwp_sql = '''
        DECLARE union_query STRING;

        -- Generate the UNION ALL query dynamically
        SET union_query = (
        SELECT STRING_AGG(FORMAT("SELECT * FROM `%s`", batch_table), " UNION ALL ")
        FROM (
            SELECT DISTINCT batch_table
            FROM temp.temp_coin_batches
        )
        );

        -- Execute the UNION ALL and save the results into a new table
        EXECUTE IMMEDIATE FORMAT("""
            CREATE OR REPLACE TABLE `core.coin_wallet_profits`
            PARTITION BY DATE(date)
            CLUSTER BY coin_id, wallet_address
            AS
            %s
        """, union_query);
        '''

    _ = dgc().run_sql(rebuild_cwp_sql)
    logger.info("Successfully rebuilt core.coin_wallet_profits.")


def drop_temp_tables():
    """
    Safely drops temp tables from core.coin_wallet_profits rebuild pipeline.
    Handles cases where tables don't exist.
    """
    logger.debug("Dropping temp tables from core.coin_wallet_profits pipeline...")

    try:
        # Check if temp_coin_batches exists
        check_sql = """
            SELECT table_name
            FROM `temp.INFORMATION_SCHEMA.TABLES`
            WHERE table_name = 'temp_coin_batches'
        """
        exists_df = dgc().run_sql(check_sql)

        if not exists_df.empty:
            # Get and drop batch tables
            temp_tables_sql = """
                select distinct batch_table
                from temp.temp_coin_batches
                where batch_table is not null
            """
            temp_tables = dgc().run_sql(temp_tables_sql)['batch_table']

            for table in temp_tables:
                drop_temp_sql = f"drop table if exists `{table}`"
                _ = dgc().run_sql(drop_temp_sql)

            # Drop the batch assignment table
            _ = dgc().run_sql("drop table if exists `temp.temp_coin_batches`")

        logger.info("Successfully dropped temp tables.")

    except Exception as e:
        logger.warning(f"Error dropping temp tables: {str(e)}")
