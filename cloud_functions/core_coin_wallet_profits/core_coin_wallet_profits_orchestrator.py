"""
Cloud function that runs a query to refresh the data in bigquery table core.coin_wallet_profits
through batch calculations that are stored as temp tables.
"""
import functions_framework
from dreams_core.googlecloud import GoogleCloud as dgc
from dreams_core import core as dc

# import local package
import core_coin_wallet_profits as cwp

# set up logger at the module level
logger = dc.setup_logger()


@functions_framework.http
def orchestrate_core_coin_wallet_profits_rebuild(request):  # pylint: disable=W0613
    """
    runs all functions in sequence to refresh core.coin_wallet_profits

    Params:
    - request (flask.request): optionally should include the number of coins to be calculated
        in each batch through the 'batch_size' param
    """
    logger.info("Beginning rebuild sequence for core.coin_wallet_profits...")

    # 1. Assign coins to batches, with each batch including {batch_size} coins
    batch_size = request.args.get('batch_size', 100)
    batch_count = set_coin_batches(batch_size)

    # 2. Calculate coin_wallet_profits data for each batch
    for batch in range(batch_count):
        cwp.update_core_coin_wallet_profits(batch)

    # 3. Rebuild core.coin_wallet_profits and drop the temp tables
    rebuild_core_table()
    drop_temp_tables()

    return '{{"rebuild of core.coin_wallet_profits complete."}}'


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
        batch_number, coin_id;

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
            f"Batch generation incomplete: {completeness_df['missing_batches'][0]} batches "
            "are missing. Aborting sequence.")


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
    Drops all temp tables from the core.coin_wallet_profits rebuild pipeline.

    Params: None
    Returns: None
    """
    logger.debug("Dropping temp tables from core.coin_wallet_profits pipeline...")

    # Retrieve list of temp tables
    temp_tables_sql = """
        select batch_table
        from temp.temp_coin_batches
        group by 1
        """
    temp_tables = dgc().run_sql(temp_tables_sql)['batch_table']

    # Drop the batch tables
    for table in temp_tables:
        drop_temp_sql = f"drop table `{table}`;"
        _ = dgc().run_sql(drop_temp_sql)

    # Drop the batch assignment table
    drop_temp_sql = "drop table `temp.temp_coin_batches`"
    _ = dgc().run_sql(drop_temp_sql)

    logger.info("Successfully dropped temp tables from core.coin_wallet_profits pipeline.")
