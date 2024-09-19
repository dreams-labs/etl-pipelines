"""
update geckoterminal market data
"""
import time
import datetime
from pytz import utc
import requests
import pandas as pd
import functions_framework
import dreams_core.core as dc
from dreams_core.googlecloud import GoogleCloud as dgc

# set up logger at the module level
logger = dc.setup_logger()

# Main code sequence
@functions_framework.http
def update_geckoterminal_market_data(request):  # pylint: disable=unused-argument  # noqa: F841
    """
    update the market data
    """

    # 1. Retrieve and structure records for coins to update
    # -----------------------------------------------------
    # Retrieve the list of all coins to be updated
    updates_df = retrieve_updates_df()

    # Transform updates_df to make pairs of blockchain-addresses to be fed into the API
    all_blockchain_address_pairs = list_all_pairs(updates_df)


    # 2. Retrieve market data from Geckoterminal
    # ------------------------------------------
    # Retrieve all pool addresses for the blockchain-address pairs
    all_pairs_with_pools = retrieve_all_pool_addresses(all_blockchain_address_pairs)


    # Add OHLCV data from pools and current time
    updated_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for pair in all_pairs_with_pools:

        # if no pool_address was returned from the earlier call, continue
        if pair[2]is None:
            continue

        ohlcv_data = retrieve_market_data_for_pool(pair)
        pair.extend([ohlcv_data[0], ohlcv_data[1], ohlcv_data[2], updated_at])

    # Create a DataFrame from the updated list
    info_df = pd.DataFrame(
        all_pairs_with_pools,
        columns=['chain', 'address', 'pool_address', 'date', 'price', 'volume', 'updated_at']
    )

    # Define the data types for the DataFrame
    dtypes = {
        'chain': 'object',
        'address': 'object',
        'pool_address': 'object',
        'date': 'object',
        'price': 'object',
        'volume': 'object',
        'updated_at': 'object'
    }

    # Apply the data types to the DataFrame
    info_df = info_df.astype(dtypes)

    # Merge the new data with the original updates DataFrame
    updates_df = pd.merge(updates_df, info_df, on='address', how='left')

    # Drop the 'chain' column as it is no longer needed
    updates_df = updates_df.drop('chain', axis=1)

    return 'all done'


def retrieve_updates_df():
    '''
    pulls a list of tokens in need of a geckoterminal market data update. this is defined as \
        coins that don't have coingecko_ids and that either have no market data or whose market \
        data is 2+ days out of date.

    returns:
        updates_df (dataframe): list of tokens that need market data updates
    '''

    query_sql = '''
        with geckoterminal_data_status as (
            select cfm.geckoterminal_id
            ,ch.chain_text_geckoterminal
            ,c.address
            ,max(cmd.date) as most_recent_record
            from core.coins c
            join core.chains ch on ch.chain_id = c.chain_id
            join core.coin_facts_metadata cfm on cfm.coin_id = c.coin_id
            left join `etl_pipelines.coin_market_data_geckoterminal` cmd on cmd.geckoterminal_id = cfm.geckoterminal_id
            left join `core.coin_facts_coingecko` cfg on cfg.coin_id = c.coin_id

            -- remove coins that don't have metadata from geckoterminal
            where cfm.geckoterminal_id is not null

            -- remove coins that have coingecko data available
            and cfg.coin_id is null
            group by 1,2,3
        )

        select ds.geckoterminal_id
        ,ds.chain_text_geckoterminal
        ,ds.address
        ,ds.most_recent_record
        ,array_agg(cmd.date IGNORE NULLS order by cmd.date asc) as all_dates
        from geckoterminal_data_status ds
        left join `etl_pipelines.coin_market_data_geckoterminal` cmd on cmd.geckoterminal_id = ds.geckoterminal_id
        where (
            ds.most_recent_record is null
            or ds.most_recent_record < (current_date('UTC') - 2)
        )
        group by 1,2,3,4
        '''

    updates_df = dgc().run_sql(query_sql)

    return updates_df


# Function to extract blockchain/address pairs from the DataFrame
def list_all_pairs(updates_df):
    """
    Generates a list of blockchain-token address pairs from the updates DataFrame.

    Parameters:
    updates_df (DataFrame): The df containing 'chain_text_geckoterminal' and 'address' columns.

    Returns:
    list: A list of blockchain and address pairs.
    """
    all_blockchain_address_pairs = []

    for _, row in updates_df.iterrows():
        chain_text_geckoterminal = row['chain_text_geckoterminal']
        address = row['address']
        all_blockchain_address_pairs.append([chain_text_geckoterminal, address])

    return all_blockchain_address_pairs


# Function to retrieve all pool addresses for a list of blockchain/address pairs
def retrieve_all_pool_addresses(all_blockchain_address_pairs):
    """
    Retrieves pool addresses for all blockchain/address pairs.

    Parameters:
    all_blockchain_address_pairs (list): A list of blockchain and address pairs.

    Returns:
    list: The updated list with pool addresses appended.
    """
    all_pairs_with_pools = []

    for pair in all_blockchain_address_pairs:
        pool_address = retrieve_pool_address(pair[0], pair[1])
        pair.append(pool_address)
        all_pairs_with_pools.append(pair)

        # rest to accommodate rate limit of 30 calls per minute
        time.sleep(2)

    return all_pairs_with_pools


# Function to retrieve the pool address for a given blockchain and token address
def retrieve_pool_address(blockchain, address):
    """
    Retrieves the pool address associated with a specific token on a given blockchain.

    If the following occurs, the pool_address is returned as a string:
        - 200 response and at least 1 pool is associated with the token

    If any of the following occur, an empty string is returned:
        - 200 response but no pools are associated with the token
        - 429 response after 3 retries
        - all other HTTP errors
        - general exception catch

    Parameters:
    blockchain (str): The blockchain identifier.
    address (str): The token address on the blockchain.

    Returns:
    pool_address (str): The pool address if successful, otherwise an empty string
    """
    retries = 3
    wait_times = [15, 30, 60]  # Wait times in seconds for retries

    for attempt in range(retries):
        try:
            # make request to geckoterminal api
            r = requests.get(
                f"https://api.geckoterminal.com/api/v2/networks/{blockchain}/tokens/{address}"
                ,timeout=30
            )

            # logic for handling rate limit codes and HTTP errors
            if r.status_code == 429:
                if attempt < retries - 1:
                    wait_time = wait_times[attempt]
                    logger.warning("Received 429 API code, retrying in %s seconds...", wait_time)
                    time.sleep(wait_time)
                    continue
                else:
                    logger.warning("Maximum retry attempts reached for 429 error.")
                    return None
            r.raise_for_status()  # Raise an HTTPError if the request is unsuccessful


            # if any pools are included in the top_pools, return the first one
            data = r.json()
            if len(data["data"]["relationships"]["top_pools"]["data"]) > 0:
                pool_pair = data["data"]["relationships"]["top_pools"]["data"][0]["id"]

                split_chain_and_pool_address = pool_pair.split("_")
                pool_address = split_chain_and_pool_address[-1]
                logger.debug("Retrieved pool address for %s:%s", blockchain, address)

                return pool_address

            # if no pools are associated with the address, log it
            else:
                logger.warning("No top pools currently associated with %s:%s", blockchain, address)
                return None

        # if there is an HTTP error, log it
        except requests.exceptions.HTTPError as http_err:
            status_code = (
                http_err.response.status_code if http_err.response else "No response"
            )
            logger.warning("HTTP error occurred (HTTP %s): %s", status_code, http_err)
            return None

        # general exceptions catch
        except Exception as err:  # pylint: disable=W0718
            logger.error("An unexpected error occurred: %s", err)
            return None


# Function to retrieve OHLCV data from the pool address for a given blockchain
def retrieve_market_data_for_pool(pair):
    """
    Retrieves OHLCV (Open, High, Low, Close, Volume) data from a pool for a specific blockchain.

    Parameters:
    blockchain (str): The blockchain identifier.
    pool_address (str): The pool address to query.

    Returns:
    list: A list containing the date, close price, and volume if successful, otherwise a default list.
    """
    BASE_URL = "https://api.geckoterminal.com/api/v2/networks"

    blockchain = pair[0]
    token_address = pair[0]
    pool_address = pair[2]

    try:
        # make the request
        r = requests.get(f"{BASE_URL}/{blockchain}/pools/{pool_address}/ohlcv/day",
                         timeout=30)
        data = r.json()

        # upload the raw response to cloud storage
        if r.status_code == 200:
            filename = f"{blockchain}_{token_address}_{datetime.datetime.now(utc).strftime('%Y%m%d_%H%M')}.json"
            dgc().gcs_upload_file(data, gcs_folder='data_lake/geckoterminal_market_data', filename=filename)

        # process the data
        ohlcv_list = data["data"]["attributes"]["ohlcv_list"][0]
        unix_timestamp = unix_to_datetime(ohlcv_list[0])
        daily_close_price = ohlcv_list[4]
        daily_volume = ohlcv_list[5]
        ohlcv_data = [unix_timestamp, daily_close_price, daily_volume]

        return ohlcv_data

    except requests.RequestException:
        return [datetime.datetime(2000, 1, 1, 0, 0, 0), 0.0, 0.0]


# Function to convert a Unix timestamp to a human-readable datetime format
def unix_to_datetime(unix_timestamp):
    """
    Converts a Unix timestamp to a human-readable format (YYYY-MM-DD HH:MM:SS).

    Parameters:
    unix_timestamp (int): The Unix timestamp to convert.

    Returns:
    str: The formatted date as a string.
    """
    datetime_obj = datetime.datetime.fromtimestamp(unix_timestamp)
    datetime_obj = datetime_obj.replace(hour=0, minute=0, second=0, microsecond=0)
    formatted_date = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
    return formatted_date

