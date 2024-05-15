import datetime
import logging
import pandas as pd
import json
import requests
import pandas_gbq
import functions_framework
from dreams_core.googlecloud import GoogleCloud as dgc


# The miliseconds are removed
def strip_and_format_unixtime(unix_time):
    # Convert the number to a string
    number_str = str(unix_time)

    # Strip the last two digits by slicing the string
    stripped_number_str = number_str[:-3]

    # Convert the stripped string back to an integer
    stripped_number = int(stripped_number_str)

    # Convert Unix timestamp to datetime object
    datetime_obj = datetime.datetime.fromtimestamp(stripped_number)

    # Format the datetime object to a human-readable date and time string
    formatted_datetime = datetime_obj.strftime('%Y-%m-%d %H:%M:%S')

    # Print the formatted date and time
    return formatted_datetime


# Function to divide the first item in the list by 2
def replace_unix_timestamp(lst):
    lst[0] = strip_and_format_unixtime(lst[0])
    return lst

def format_and_add_columns(df, coingecko_id):
    '''
    converts json data from the coingecko api into a table-formatted dataframe by converting columns of \
    tuples into standardized columns that match the bigquery table format

    params:
        df (pandas.DataFrame): df of coingecko market data
        coingecko_id (str): coingecko id of coin

    returns:
        df (pandas.DataFrame): formatted df of market data
    '''
    # L`oop through each row in the DataFrame and apply the function
    for index, row in df.iterrows():
        # Formatting unix timestamp of prices column
        df.at[index, 'prices'] = cmd.replace_unix_timestamp(row['prices'])

        # Removing the unix timestamps from the columns
        df.at[index, 'market_caps'] = row['market_caps'][1]
        df.at[index, 'total_volumes'] = row['total_volumes'][1]

    df = df.assign(date=[i[0] for i in df['prices']],
                    prices=[i[1] for i in df['prices']],
                    coingecko_id = coingecko_id)

    # Rearranging columns
    df = df[['coingecko_id', 'date', 'prices', 'market_caps', 'total_volumes']]

    # Renaming columns
    df.columns = ['coingecko_id', 'date', 'price', 'market_cap', 'volume']

    return df


def retrieve_coin_data(coingecko_id):
    '''
    retrieves market data for a given coingecko_id. 

    note that no api key is used, as inputting a free plan api key in the headers causes the
    call to error out. 

    params:
        coingecko_id (str): coingecko id of coin

    returns:
        df (dataframe): formatted df of market data
    '''
    logger = logging.getLogger(__name__)

    logger.info('retreiving coingecko data for %s...', coingecko_id)
    url = f'https://api.coingecko.com/api/v3/coins/{coingecko_id}/market_chart?vs_currency=usd&days=365&interval=daily'
    r = requests.get(url, timeout=30)

    logger.info('formatting coingecko data for %s...', coingecko_id)
    data = r.json()
    df = pd.DataFrame(data)
    df = format_and_add_columns(df, coingecko_id)

    return df


def upload_market_data(market_df):
    '''
    appends the market_df to the bigquery table etl_pipelines.coin_market_data_coingecko. 

    steps:
        1. explicitly map datatypes onto new dataframe upload_df
        2. declare schema datatypes
        3. upload using pandas_gbq

    params:
        freshness_df (pandas.DataFrame): df of fresh dune data
        transfers_df (pandas.DataFrame): df of token transfers
    returns:
        none
    '''
    logger = logging.getLogger(__name__)

    # add metadata to upload_df
    upload_df = pd.DataFrame()
    upload_df['date'] = market_df['date']
    upload_df['chain_text_coingecko'] = market_df['chain']
    upload_df['token_address'] = market_df['address']
    upload_df['price'] = market_df['price']
    upload_df['market_cap'] = market_df['market_cap']
    upload_df['volume'] = market_df['volume']
    upload_df['data_source'] = 'coingecko'
    upload_df['data_updated_at'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # set df datatypes of upload df
    dtype_mapping = {
        'date': 'datetime64[ns, UTC]',
        'chain_text_coingecko': str,
        'token_address': str,
        'price': float,
        'market_cap': float,
        'volume': float,
        'data_source': str,
        'data_updated_at': 'datetime64[ns, UTC]'
    }
    upload_df = upload_df.astype(dtype_mapping)
    logger.info('prepared upload df with %s rows.',len(upload_df))

    # upload df to bigquery
    project_id = 'western-verve-411004'
    table_name = 'etl_pipelines.coin_market_data_coingecko'
    schema = [
        {'name':'date', 'type': 'datetime'},
        {'name':'chain_text_coingecko', 'type': 'string'},
        {'name':'token_address', 'type': 'string'},

        # note the special datatype for bignumeric which ensures precision for very small price values
        {'name':'price', 'type': 'bignumeric'},

        {'name':'market_cap', 'type': 'float'},
        {'name':'volume', 'type': 'float'},
        {'name':'data_source', 'type': 'string'},
        {'name':'data_updated_at', 'type': 'datetime'}
    ]
    pandas_gbq.to_gbq(
        upload_df
        ,table_name
        ,project_id=project_id
        ,if_exists='append'
        ,table_schema=schema
        ,progress_bar=False

        # this setting slows the upload but  is required for the bignumeric column to upload without errors
        ,api_method='load_csv'
    )
    logger.info('appended upload df to %s.', table_name)
