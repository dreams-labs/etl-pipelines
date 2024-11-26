"""
This script retrieves and updates market data from GeckoTerminal for a list of coins
that do not have Coingecko data. The data is fetched via multiple API calls to obtain
market data such as OHLCV (Open, High, Low, Close, Volume) from token pools. The data
is then uploaded to a BigQuery table for further analysis. The script includes retry
logic to handle rate limits and HTTP errors gracefully.
"""
# pylint: disable=C0301

import time
import datetime
from pytz import utc
import requests
import pandas as pd
import functions_framework
import pandas_gbq
import dreams_core.core as dc
from dreams_core.googlecloud import GoogleCloud as dgc

# set up logger at the module level
logger = dc.setup_logger()

# Main code sequence
@functions_framework.http
def update_geckoterminal_market_data(request):  # pylint: disable=unused-argument  # noqa: F841
    """
    Fetches updated market data for tokens without Coingecko data and uploads it to BigQuery.

    1. Retrieves a list of tokens that need GeckoTerminal data updates.
    2. For each token, retrieves associated pool information using the GeckoTerminal API.
    3. Fetches OHLCV data (Open, High, Low, Close, Volume) for each pool address.
    4. Aggregates the data into a DataFrame and uploads it to BigQuery.

    Parameters:
        request (flask.Request): A Flask request object, though it is unused in this function.

    Returns:
        str: A success message when the update process completes.
    """

    # 1. Retrieve and structure records for coins to update
    # -----------------------------------------------------
    # Retrieve the list of all coins to be updated
    updates_df = retrieve_updates_df()

    # Generate a list of arrays containing each token's id, chain, and address
    all_blockchain_address_pairs = [
        [row['geckoterminal_id'], row['chain_text_geckoterminal'], row['address']]
        for _, row in updates_df.iterrows()
    ]

    # 2. Ping Geckoterminal API to get pool address
    # ---------------------------------------------
    # Retrieve all pool addresses for the blockchain-address pairs
    all_pairs_with_pools = retrieve_all_pool_addresses(all_blockchain_address_pairs)


    # 3. Ping Geckoterminal API to get pool market data and aggregate it
    # ------------------------------------------------------------------
    # Retrieve OHLCV data for pool addresses
    all_market_data = []
    for pair in all_pairs_with_pools:
        # if no pool_address was returned from the earlier call, skip the coin
        if pair[3]is None:
            continue

        # if market data is successfully retrieved, append it
        market_df = retrieve_market_data_for_pool(pair)
        if not market_df.empty:
            all_market_data.append(market_df)

        # pause to accomodate 30 calls/minute rate limit
        time.sleep(2)

    if not all_market_data:
        return "no new market data found"

    combined_market_df = pd.concat(all_market_data, ignore_index=True)

    # 4. Filter existing records and upload the remaining market data to BigQuery
    # ---------------------------------------------------------------------------
    new_market_data_df = filter_new_market_data(updates_df, combined_market_df)

    # Filter out records for the current UTC date
    current_utc_date = pd.Timestamp.now(tz='UTC').normalize()
    new_market_data_df = new_market_data_df[new_market_data_df['date'] < current_utc_date]

    if not new_market_data_df.empty:
        upload_combined_market_df(new_market_data_df)

    return "finished updating geckoterminal market data"



def retrieve_updates_df():
    """
    pulls a list of tokens in need of a geckoterminal market data update. this is defined as \
        coins that don't have coingecko_ids and that either have no market data or whose market \
        data is 2+ days out of date.

    returns:
        updates_df (dataframe): list of tokens that need market data updates
    """

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
        order by 1
        '''

    updates_df = dgc().run_sql(query_sql)

    return updates_df


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
        pool_address = retrieve_pool_address(pair[1], pair[2])
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
    pair (series): A series of the blockchain, token address, and pool address for one coin

    Returns:
    market_df (dataframe): Either a dataframe with valid OHLCV data or an empty dataframe
    """
    BASE_URL = "https://api.geckoterminal.com/api/v2/networks"  # pylint: disable=C0103

    geckoterminal_id = pair[0]
    blockchain = pair[1]
    token_address = pair[2]
    pool_address = pair[3]

    try:
        # Make the request
        r = requests.get(f"{BASE_URL}/{blockchain}/pools/{pool_address}/ohlcv/day",
                         timeout=30)

        # Raise an HTTPError if the HTTP request returned an unsuccessful status code
        r.raise_for_status()

        # Check if the expected data structure is present
        data = r.json()
        if ("data" not in data or
            "attributes" not in data["data"] or
            "ohlcv_list" not in data["data"]["attributes"]):
            logger.warning("Malformed data structure received.")
            return pd.DataFrame()

        # Check if the OHLCV list is empty
        if len(data["data"]["attributes"]["ohlcv_list"])==0:
            logger.warning("Received empty ohlcv_list from API.")
            return pd.DataFrame()

        # Store the raw JSON response in cloud storage
        filename_datetime = datetime.datetime.now(utc).strftime('%Y%m%d_%H%M')
        filename = f"{blockchain}_{token_address}_{filename_datetime}.json"
        dgc().gcs_upload_file(
            data, gcs_folder='data_lake/geckoterminal_market_data', filename=filename)
        logger.debug("Stored valid API response data for %s:%s.", blockchain, token_address)

        # Process the valid response into a dataframe
        if len(data["data"]["attributes"]["ohlcv_list"]) > 0:
            market_df = pd.DataFrame(data["data"]["attributes"]["ohlcv_list"])
            market_df.columns = ['date','open','high','low','close','volume']
            market_df['date'] = market_df['date'].apply(unix_to_datetime)

            # append metadata
            market_df['geckoterminal_id'] = geckoterminal_id
            market_df['pool_address'] = pool_address
            market_df['blockchain'] = blockchain

            return market_df

    except requests.exceptions.HTTPError as http_err:
        logger.warning("HTTP error occurred: %s", http_err)
        return pd.DataFrame()
    except requests.exceptions.RequestException as req_err:
        logger.warning("Request exception occurred: %s", req_err)
        return pd.DataFrame()
    except Exception as e:  # pylint: disable=W0718
        logger.warning("An unexpected error occurred: %s", e)
        return pd.DataFrame()



# Function to convert a Unix timestamp to a human-readable datetime format
def unix_to_datetime(unix_timestamp):
    """
    Converts a Unix timestamp to a human-readable format (YYYY-MM-DD HH:MM:SS).

    Parameters:
    unix_timestamp (int): The Unix timestamp to convert.

    Returns:
    str: The formatted date as a string.
    """
    datetime_obj = datetime.datetime.fromtimestamp(unix_timestamp, tz=datetime.timezone.utc)
    datetime_obj = datetime_obj.replace(hour=0, minute=0, second=0, microsecond=0)
    formatted_date = datetime_obj.strftime("%Y-%m-%d %H:%M:%S")

    return formatted_date

def filter_new_market_data(updates_df, combined_market_df):
    """
    Filters combined_market_df to keep only the rows with data that isn't already in BigQuery.

    Args:
        updates_df (DataFrame): DataFrame containing current state of the BigQuery table
        combined_market_df (DataFrame): DataFrame containing new market data

    Returns:
        DataFrame: A filtered DataFrame with only new market data
    """

    # Create a df showing all existing bigquery records by exploding the 'all_dates' column
    existing_dates_df = updates_df.explode('all_dates')
    existing_dates_df['all_dates'] = pd.to_datetime(existing_dates_df['all_dates']).dt.tz_localize('UTC')
    existing_dates_df = existing_dates_df[['geckoterminal_id', 'all_dates']].rename(columns={'all_dates': 'date'})

    # Set market data to be a UTC datetime
    combined_market_df['date'] = pd.to_datetime(combined_market_df['date']).dt.tz_localize('UTC')

    # Merge combined_market_df with existing_dates_df on 'geckoterminal_id' and 'date'
    merged_df = pd.merge(
        combined_market_df,
        existing_dates_df,
        on=['geckoterminal_id', 'date'],
        how='left',
        indicator=True
    )

    # Filter to keep only rows that aren't already in BigQuery
    filtered_market_df = merged_df[merged_df['_merge'] == 'left_only'].drop(columns=['_merge'])

    return filtered_market_df


def upload_combined_market_df(combined_market_df):
    """
    Uploads the combined_market_df DataFrame to a BigQuery table by appending the data.

    Parameters:
    combined_market_df (DataFrame): The DataFrame containing the market data to upload.

    Returns:
    None
    """

    # Define the data types for the DataFrame
    dtypes = {
        'geckoterminal_id': 'object',
        'blockchain': 'object',
        'pool_address': 'object',
        'date': 'datetime64[ns, UTC]',
        'open': 'float',
        'high': 'float',
        'low': 'float',
        'close': 'float',
        'volume': 'float',
        'updated_at': 'datetime64[ns, UTC]'
    }

    # Set updated_at time
    updated_at = datetime.datetime.now(utc).strftime('%Y-%m-%d %H:%M:%S')
    combined_market_df.loc[:, 'updated_at'] = updated_at

    # Apply the data types to the DataFrame
    combined_market_df = combined_market_df.astype(dtypes)

    # Upload the DataFrame to BigQuery
    destination_table = 'etl_pipelines.coin_market_data_geckoterminal'
    project_id = 'western-verve-411004'
    pandas_gbq.to_gbq(
        combined_market_df,
        destination_table=destination_table,
        project_id=project_id,
        if_exists='append'
    )
    logger.info("Uploaded %s rows to etl_pipelines.coin_market_data_geckoterminal.",
                combined_market_df.shape[0])
