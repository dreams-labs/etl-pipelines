import datetime
import logging
from pytz import utc
import pandas as pd
import json
import requests
import pandas_gbq
import functions_framework
from dreams_core.googlecloud import GoogleCloud as dgc


# The miliseconds are removed
def strip_and_format_unixtime(unix_time):
    '''
    converts a unix timestamp into a single date datetime. this includes logic that assigns \
    mid-day records to a single date

    params:
        unix_time (int): unix timestamp

    returns:
        formatted_datetime (datetime): datetime derived from the unix timestamp
    '''
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
    '''
    utility function to replace coingecko tuple responses with unix timestamps into datetimes

    params:
        lst (tuple): tuple response from coingecko witha unix timestamp as the first value

    returns:
        lst (datetime): datetime derived from the unix timestamp
    '''
    lst[0] = strip_and_format_unixtime(lst[0])
    return lst

def format_and_add_columns(df, coingecko_id, coin_id, most_recent_record):
    '''
    converts json data from the coingecko api into a table-formatted dataframe by converting columns of \
    tuples into standardized columns that match the bigquery table format

    params:
        df (pandas.DataFrame): df of coingecko market data
        coingecko_id (str): coingecko id of coin
        coin_id (str): matches core.coins.coin_id
        most_recent_record (datetime): most recent timestamp in etl_pipelines.coin_market_data_coingecko

    returns:
        df (pandas.DataFrame): formatted df of market data
    '''
    # loop through each row in the DataFrame and apply the function
    for index, row in df.iterrows():
        # formatting unix timestamp of prices column
        df.at[index, 'prices'] = replace_unix_timestamp(row['prices'])

        # removing the unix timestamps from the columns
        df.at[index, 'market_caps'] = row['market_caps'][1]
        df.at[index, 'total_volumes'] = row['total_volumes'][1]

    df = df.assign(date=[i[0] for i in df['prices']],
                    prices=[i[1] for i in df['prices']],
                    coingecko_id = coingecko_id)

    # rearranging and renaming columns
    df['coin_id'] = coin_id
    df = df[['coingecko_id', 'coin_id', 'date', 'prices', 'market_caps', 'total_volumes']]
    df.columns = ['coingecko_id', 'coin_id', 'date', 'price', 'market_cap', 'volume']

    # convert market_cap and volumes to integers
    df['market_cap'] = df['market_cap'].astype(int)
    df['volume'] = df['volume'].astype(int)

    # convert date column to utc datetime
    df['date'] = pd.to_datetime(df['date'])
    df['date'] = df['date'].dt.tz_localize('UTC')

    # find and drop the index of the row with the most recent date to avoid partial date data
    max_date_index = df['date'].idxmax()
    df = df.drop(max_date_index)

    # if records exist in the database, remove them from the df
    if not pd.isna(most_recent_record):



    # round date to nearest day
    df['date'] = pd.to_datetime(df['date']).dt.floor('D')

    # drop duplicate dates if exists. this only occurs if a coin is removed from coingecko, e.g.:
    # https://www.coingecko.com/en/coins/serum-ser
    # https://www.coingecko.com/en/coins/chart-roulette
    df = df.drop_duplicates(subset='date', keep='last')

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
        status_code (int): status code of coingecko api call
    '''
    url = f'https://api.coingecko.com/api/v3/coins/{coingecko_id}/market_chart?vs_currency=usd&days=365&interval=daily'
    r = requests.get(url, timeout=30)

    data = r.json()
    if r.status_code == 200:
        df = pd.DataFrame(data)
    else:
        df = None
    return df,r.status_code


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
    upload_df['coin_id'] = market_df['coin_id']
    upload_df['coingecko_id'] = market_df['coingecko_id']
    upload_df['price'] = market_df['price']
    upload_df['market_cap'] = market_df['market_cap']
    upload_df['volume'] = market_df['volume']
    upload_df['updated_at'] = datetime.datetime.now(utc).strftime('%Y-%m-%d %H:%M:%S')

    # set df datatypes of upload df
    dtype_mapping = {
        'date': 'datetime64[ns, UTC]',
        'coin_id': str,
        'coingecko_id': str,
        'price': float,
        'market_cap': int,
        'volume': int,
        'updated_at': 'datetime64[ns, UTC]'
    }
    upload_df = upload_df.astype(dtype_mapping)
    logger.info('prepared upload df with %s rows.',len(upload_df))

    # upload df to bigquery
    project_id = 'western-verve-411004'
    table_name = 'etl_pipelines.coin_market_data_coingecko'
    schema = [
        {'name':'date', 'type': 'datetime'},
        {'name':'coin_id', 'type': 'string'},
        {'name':'chain_text_coingecko', 'type': 'string'},

        # note the special datatype for bignumeric which ensures precision for very small price values
        {'name':'price', 'type': 'bignumeric'},

        {'name':'market_cap', 'type': 'int'},
        {'name':'volume', 'type': 'int'},
        {'name':'updated_at', 'type': 'datetime'}
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
