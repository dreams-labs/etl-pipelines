"""
update geckoterminal market data
"""
import datetime
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
    # Read the updates_df CSV file
    updates_df = retrieve_updates_df()

    # Retrieve all blockchain/address pairs
    all_blockchain_address_pairs = retrieve_all_pairs(updates_df)

    # Retrieve all pool addresses for the blockchain/address pairs
    all_pairs_with_pools = retrieve_all_pool_addresses(all_blockchain_address_pairs)

    # Add OHLCV data from pools and current time
    for pair in all_pairs_with_pools:
        ohlcv_data = retrieve_data_from_pool(pair[0], pair[2])
        pair.extend([ohlcv_data[0], ohlcv_data[1], ohlcv_data[2], get_current_time()])

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


from dreams_core.googlecloud import GoogleCloud as dgc


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
            select c.coin_id
            ,ch.chain_text_geckoterminal
            ,c.address
            ,max(cmd.date) as most_recent_record
            from core.coins c
            join core.chains ch on ch.chain_id = c.chain_id
            left join core.coin_market_data cmd on cmd.coin_id = c.coin_id

            -- remove coins that have coingecko data available
            left join `core.coin_facts_coingecko` cfg on cfg.coin_id = c.coin_id
            where cfg.coin_id is null
            group by 1,2,3
        )

        select coin_id
        ,chain_text_geckoterminal
        ,address
        ,most_recent_record
        from geckoterminal_data_status ds
        where (
            ds.most_recent_record is null
            or ds.most_recent_record < (current_date('UTC') - 1)
        )
        '''

    updates_df = dgc().run_sql(query_sql)

    return updates_df


updates_df = retrieve_updates_df()



# Function to retrieve the pool address for a given blockchain and token address
def retrieve_pool_address(blockchain, address):
    """
    Retrieves the pool address associated with a specific token on a given blockchain.

    Parameters:
    blockchain (str): The blockchain identifier.
    address (str): The token address on the blockchain.

    Returns:
    str: The pool address if successful, otherwise an error message.
    """
    BASE_URL = "https://api.geckoterminal.com/api/v2/networks"

    try:
        r = requests.get(f"{BASE_URL}/{blockchain}/tokens/{address}")
        data = r.json()
        data = data["data"]["relationships"]["top_pools"]["data"][0]["id"]

        split_chain_and_pool_address = data.split("_")
        pool_address = split_chain_and_pool_address[-1]

        return pool_address

    except requests.RequestException:
        return "couldn't retrieve the pool address"

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

    return all_pairs_with_pools

# Function to extract blockchain/address pairs from the DataFrame
def retrieve_all_pairs(updates_df):
    """
    Extracts blockchain and token address pairs from the updates DataFrame.

    Parameters:
    updates_df (DataFrame): The DataFrame containing 'chain_text_geckoterminal' and 'address' columns.

    Returns:
    list: A list of blockchain and address pairs.
    """
    all_blockchain_address_pairs = []

    for index, row in updates_df.iterrows():
        chain_text_geckoterminal = row['chain_text_geckoterminal']
        address = row['address']
        all_blockchain_address_pairs.append([chain_text_geckoterminal, address])

    return all_blockchain_address_pairs

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

# Function to get the current time in a specific format
def get_current_time():
    """
    Returns the current time in the format 'YYYY-MM-DD HH:MM:SS'.

    Returns:
    str: The current date and time as a string.
    """
    now = datetime.datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S")

# Function to retrieve OHLCV data from the pool address for a given blockchain
def retrieve_data_from_pool(blockchain, pool_address):
    """
    Retrieves OHLCV (Open, High, Low, Close, Volume) data from a pool for a specific blockchain.

    Parameters:
    blockchain (str): The blockchain identifier.
    pool_address (str): The pool address to query.

    Returns:
    list: A list containing the date, close price, and volume if successful, otherwise a default list.
    """
    BASE_URL = f"https://api.geckoterminal.com/api/v2/networks/{blockchain}/pools/{pool_address}/ohlcv/day"

    try:
        r = requests.get(f"{BASE_URL}")
        data = r.json()
        ohlcv_list = data["data"]["attributes"]["ohlcv_list"][0]
        unix_timestamp = unix_to_datetime(ohlcv_list[0])
        daily_close_price = ohlcv_list[4]
        daily_volume = ohlcv_list[5]
        ohlcv_data = [unix_timestamp, daily_close_price, daily_volume]

        return ohlcv_data

    except requests.RequestException:
        return [datetime.datetime(2000, 1, 1, 0, 0, 0), 0.0, 0.0]
