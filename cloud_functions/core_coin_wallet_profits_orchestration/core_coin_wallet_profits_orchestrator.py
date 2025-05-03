"""
Cloud function that runs a query to refresh the data in bigquery table core.coin_wallet_profits
through batch calculations that are stored as temp tables.
"""
import os
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import requests
from requests.adapters import HTTPAdapter
import functions_framework
import google.auth
import google.oauth2.id_token
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession
from google.api_core import exceptions as google_exceptions
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

    # Sequence to initiate multithreaded function calls for multiple batches with retries
    max_retries = 3
    batches_to_process = list(range(batch_count))
    all_failed_batches = []

    for attempt in range(1, max_retries + 1):
        failed_batches = []
        logger.info(f"Starting batch processing attempt {attempt} for batches: {batches_to_process}")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_batch = {
                executor.submit(process_single_batch, batch, session, worker_url): batch
                for batch in batches_to_process
            }

            for future in concurrent.futures.as_completed(future_to_batch):
                batch = future_to_batch[future]
                try:
                    result = future.result()
                    logger.info(f"Completed batch {result['batch']}")
                except BatchProcessingError as e:
                    logger.error("Batch processing error on batch %s: %s", batch, str(e))
                    failed_batches.append(batch)
                except Exception as e:
                    logger.error("Unexpected error in batch %d: %s", batch, str(e))
                    failed_batches.append(batch)

        if not failed_batches:
            # All batches succeeded
            batches_to_process = []
            break

        logger.info("Batches failed in attempt %s: %s", attempt, failed_batches)
        if attempt < max_retries:
            logger.info("Retrying failed batches in next attempt.")
            batches_to_process = failed_batches
        else:
            all_failed_batches = failed_batches

    if all_failed_batches:
        raise RuntimeError(f"Failed batches after {max_retries} attempts: {all_failed_batches}")

    # 3. Rebuild core.coin_wallet_profits, optionally drop temp tables
    rebuild_core_table()
    drop_temp_tables()

    return (
        {
            "status": "complete",
            "batch_size": int(batch_size),
            "max_workers": int(max_workers),
            "total_batches": int(batch_count),
            "failed_batches": failed_batches
        },
        200
    )



class BatchProcessingError(Exception):
    """Custom exception for batch processing failures"""
    pass  # pylint:disable=W0107  # unecessary pass

def process_single_batch(batch, session, worker_url):
    """Process a single batch of coin wallet profits calculations"""
    logger.info("Initiating profits calculations for batch %s...", batch)
    try:
        response = session.post(worker_url, json={"batch_number": batch})
        if response.status_code != 200:
            error_msg = response.json().get('error', 'Unknown error')
            raise BatchProcessingError(f"Batch {batch} failed with status {response.status_code}: {error_msg}")

        logger.info("Completed profits calculations for batch %s.", batch)
        return response.json()
    except requests.RequestException as e:
        # Handle network/connection errors
        raise BatchProcessingError(f"Network error processing batch {batch}: {str(e)}") from e
    except ValueError as e:
        # Handle JSON decode errors
        raise BatchProcessingError(f"Invalid JSON response for batch {batch}: {str(e)}") from e



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

        # Increase urllib3 connection‐pool to avoid “pool is full” warnings
        adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

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
       while excluding specific wallet_address-coin_id combinations that
       have wallet values higher than coin market caps, the vast majority
       of which are deployers, bridges, or other outliers.

    Params: None
    Returns: None
    """
    logger.info("Rebuilding core.coin_wallet_profits table...")

    # 1. Confirm that all batches have been completed
    completeness_check_sql = """
        select count(distinct batch_number) as missing_batches
        from temp.temp_coin_batches
        where batch_table is null
        """

    completeness_df = dgc().run_sql(completeness_check_sql)

    if completeness_df['missing_batches'][0] > 0:
        raise RuntimeError(
            f"Batch generation incomplete: {completeness_df['missing_batches'][0]} batches "
            "are missing. Aborting core.coin_wallet_profits update sequence."
        )

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
            AS (
                WITH draft_table AS (
                    SELECT *
                    FROM (%s)
                ),

                overage_wallets as (
                    SELECT coin_id, wallet_address
                    FROM (
                        -- Subquery to identify overage wallet combinations
                        WITH overage_wallets AS (
                            SELECT cwp.coin_id, cwp.wallet_address
                            FROM draft_table cwp
                            JOIN core.coin_market_data cmd
                            ON cmd.coin_id = cwp.coin_id AND cmd.date = cwp.date
                            WHERE cwp.usd_balance > cmd.market_cap
                            AND cmd.market_cap > 0
                            GROUP BY 1, 2
                        ),
                        overage_coins AS (
                            SELECT coin_id, COUNT(wallet_address) AS total_wallets
                            FROM overage_wallets
                            GROUP BY 1
                        )
                        SELECT ow.coin_id, ow.wallet_address
                        FROM overage_coins oc
                        JOIN overage_wallets ow ON ow.coin_id = oc.coin_id
                        -- more than 20 overage wallets usually indicates bad market cap data
                        WHERE oc.total_wallets <= 20
                    )
                )

                SELECT t.*
                FROM draft_table t
                LEFT JOIN overage_wallets ow
                    ON ow.coin_id = t.coin_id
                    AND ow.wallet_address = t.wallet_address
                WHERE ow.wallet_address is null
            )
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

    except google_exceptions.NotFound:
        logger.warning("One or more tables not found during cleanup")
    except google_exceptions.PermissionDenied as e:
        logger.warning(f"Permission denied while dropping tables: {str(e)}")
    except Exception as e: # pylint:disable=W0718  # general exception catch
        logger.warning(f"Error dropping temp tables: {str(e)}")
