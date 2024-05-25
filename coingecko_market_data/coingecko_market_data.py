'''
cloud function that updates the bigquery table `etl_pipelines.coin_market_data_coingecko` by making api \
calls to coingecko to get market data and uploading it
'''
import datetime
import time
import logging
from pytz import utc
import pandas as pd
import requests
import pandas_gbq
import functions_framework
from dreams_core.googlecloud import GoogleCloud as dgc


# retrieve list of coins with a coingecko_id that have no market data
def retrieve_updates_df():
    '''
    pulls a list of tokens with coingecko ids from bigquery, limiting to coins that either \
        have no market data or that have data at least 2 days old. 

    returns:
        updates_df (dataframe): list of tokens that need price updates from coingecko
    '''

    query_sql = '''
        with coingecko_data_status as (
            select cgi.coin_id
            ,cgi.coingecko_id
            ,max(md.date) as most_recent_record
            from `etl_pipelines.coin_coingecko_ids` cgi
            left join `etl_pipelines.coin_market_data_coingecko` md on md.coingecko_id = cgi.coingecko_id
            where cgi.coingecko_id is not null
            group by 1,2
        )

        select cds.coin_id
        ,cds.coingecko_id
        ,cds.most_recent_record
        from coingecko_data_status cds
        where (
            cds.most_recent_record is null
            or cds.most_recent_record < (current_date('UTC') - 1)
            )
        '''

    updates_df = dgc().run_sql(query_sql)

    return updates_df



def strip_and_format_unixtime(unix_time):
    '''
    converts a coingecko unix timestamp into a single date datetime. this includes logic that \
        assigns mid-day records to a single date

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
    converts json data from the coingecko api into a table-formatted dataframe by converting \
        columns of tuples into standardized columns that match the bigquery table format

    params:
        df (pandas.DataFrame): df of coingecko market data
        coingecko_id (str): coingecko id of coin
        coin_id (str): matches core.coins.coin_id
        most_recent_record (datetime): most recent record in coin_market_data_coingecko

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

    # round date to nearest day
    df['date'] = pd.to_datetime(df['date']).dt.floor('D')

    # if records already exist in the database, remove them from the df
    if not pd.isna(most_recent_record):
        most_recent_record = most_recent_record.to_pydatetime().replace(tzinfo=utc)
        df = df[df['date'] > most_recent_record]

    # drop duplicate dates if exists. this only occurs if a coin is removed from coingecko, e.g.:
    # https://www.coingecko.com/en/coins/serum-ser
    # https://www.coingecko.com/en/coins/chart-roulette
    df = df.drop_duplicates(subset='date', keep='last')

    return df



def ping_coingecko_api(coingecko_id):
    '''
    requests market data for a given coingecko_id. 

    note that no api key is used, as inputting a free plan api key in the headers causes the \
        call to error out. also stores the json response in cloud storage in case there any \
        improper adjustments are later detected. 

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
        # upload the raw response to cloud storage
        filename = f"{coingecko_id}_{datetime.datetime.now(utc).strftime('%Y%m%d_%H%M')}.json"
        dgc().gcs_upload_file(data, gcs_folder='data_lake/coingecko_market_data', filename=filename)

        # convert json blob to a dataframe
        df = pd.DataFrame(data)
    else:
        df = None

    return df,r.status_code


def retrieve_coingecko_market_data(coingecko_id):
    '''
    attempts to retrieve data from the coingecko api including error handling for various \
        coingecko api status codes

    params:
        coingecko_id (string): coingecko id of coin

    returns:
        market_df (dataframe): raw api response of market data
        api_status_code (int): status code of coingecko api call
    '''
    logger = logging.getLogger(__name__)
    logger.addHandler(logging.NullHandler())
    retry_attempts = 3

    for attempt in range(retry_attempts):
        market_df,api_status_code = ping_coingecko_api(coingecko_id)
        if api_status_code == 200:
            break
        elif api_status_code == 404:
            break
        elif api_status_code == 429:
            logger.info('coingecko api call timed out, retrying after a 60 second pause...')
            time.sleep(60)
            retry_attempts -= 1
            continue
        else:
            logger.error('unexpected coingecko api status code %s for %s, continuing to next coin.',
                str(api_status_code), coingecko_id)
            break
    logger.info('coingecko api call for %s completed with status code %s.',
        coingecko_id, str(api_status_code))

    return market_df, api_status_code



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

        # note the bignumeric datatype which ensures precision for very small price values
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

        # this setting is required for the bignumeric column to upload without errors
        ,api_method='load_csv'
    )
    logger.info('appended upload df to %s.', table_name)



def push_updates_to_bigquery():
    '''
    runs a sql query that inserts newly added rows in etl_pipelines.coin_market_data_coingecko \
        into core.coin_market_data
    '''
    query_sql = '''
    insert into core.coin_market_data (

        select md.date
        ,co.coin_id
        ,co.chain_id
        ,co.address
        ,md.price

        -- use fdv if market cap data isn't available
        ,case 
            when md.market_cap > 0 then md.market_cap
            else cast(md.price*cgf.total_supply as int64)
            end as market_cap

        -- calculate fdv using total supply
        ,cast(md.price*cgf.total_supply as int64) as fdv

        -- calculate circulating supply using market cap
        ,case
            when md.market_cap > 0 then cast(md.market_cap/md.price as int64)
            else cast(cgf.total_supply as int64)
            end as circulating_supply

        -- total supply retrieved from coingecko metadata tables
        ,cast(cgf.total_supply as int64) as total_supply

        ,md.volume
        ,'coingecko' as data_source
        ,md.updated_at
        from core.coins co
        join core.coingecko_facts cgf on cgf.coin_id = co.coin_id
        join etl_pipelines.coin_market_data_coingecko md on md.coin_id = co.coin_id

        -- don't insert rows that already have data
        left join core.coin_market_data cmd on cmd.coin_id = md.coin_id and cmd.date = md.date
        where cmd.date is null

    )
    '''

    dgc().run_sql(query_sql)



@functions_framework.cloud_event
def update_coingecko_market_data():
    '''
    runs all functions in sequence to update and upload coingecko market data
    '''
    # configure logger
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s',
        datefmt='%d/%b/%Y %H:%M:%S'
        )
    logger = logging.getLogger(__name__)

    # retrieve list of coins with a coingecko_id that need market data updates
    updates_df = retrieve_updates_df()

    # retrieve market data for each coin in need of updates
    for i in range(updates_df.shape[0]):
        try:
            # store iteration-specific variables
            coingecko_id = updates_df['coingecko_id'][i]
            coin_id = updates_df['coin_id'][i]
            most_recent_record = updates_df['most_recent_record'][i]

            # retrieve coingecko market data
            logger.info('retreiving coingecko data for %s...', coingecko_id)
            market_df,api_status_code = retrieve_coingecko_market_data(coingecko_id)

            if api_status_code == 200:
                # format and filter market data to prepare for upload
                logger.info('formatting market data for %s...', coingecko_id)
                market_df = format_and_add_columns(market_df, coingecko_id, coin_id, most_recent_record)

                # skip to next coin if there's no new records available
                if market_df.empty:
                    logger.info('no new records found for %s, continuing to next coin.', coingecko_id)
                    continue

                # upload market data to bigquery
                logger.info('uploading market data for %s...', coingecko_id)
                upload_market_data(market_df)
                continue

        except Exception as e:
            logger.error('an error occurred for coingecko_id %s: %s. continuing to next coin.', coingecko_id, e)
            continue

    # add the new records to core.coin_market_data
    logger.info('pushing new coingecko market data records to core.coin_market_data...')
    push_updates_to_bigquery()

    # and we're done
    logger.info('update_coingecko_market_data() has completed successfully!')