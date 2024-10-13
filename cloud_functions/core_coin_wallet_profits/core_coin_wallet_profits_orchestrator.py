"""
Cloud function that runs a query to refresh the data in bigquery table core.coin_wallet_profits
This function is heavily optimized to reduce memory usage by taking advantage of categorical
columns, int32, and dropping columns as soon as they are no longer needed.
"""
import time
import gc
import logging
import pandas as pd
import numpy as np
import functions_framework
import pandas_gbq
from dreams_core.googlecloud import GoogleCloud as dgc
from dreams_core import core as dc

# set up logger at the module level
logger = dc.setup_logger()
logger.setLevel(logging.DEBUG)


# @functions_framework.http
# def update_core_coin_wallet_profits(request):  # pylint: disable=W0613
#     """
#     runs all functions in sequence to refresh core.coin_wallet_profits
#     """

#     coin_prefix = request.args.get('coin_prefix', '')
#     print(coin_prefix)

#     return '{{"rebuild of core.coin_wallet_transfers complete."}}'


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
        CAST(FLOOR((row_num - 1) / {batch_size}) AS INT64) AS batch_number
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

    return batch_count
