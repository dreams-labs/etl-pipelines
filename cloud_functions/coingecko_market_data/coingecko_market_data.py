'''
cloud function that updates the bigquery table `etl_pipelines.coin_market_data_coingecko` by \
    making api calls to coingecko to get market data and uploading it
'''
import datetime
import time
import os
from pytz import utc
import pandas as pd
import requests
import pandas_gbq
import functions_framework
from google.cloud import bigquery
from dreams_core.googlecloud import GoogleCloud as dgc
from dreams_core import core as dc

# set up logger at the module level
logger = dc.setup_logger()


@functions_framework.http
def update_coin_market_data_coingecko(request):  # pylint: disable=unused-argument  # noqa: F841
    '''
    runs all functions in sequence to update and upload the coingecko market data to
    etl_pipelines.update_coin_market_data_coingecko.

    key steps:
        1. retrieve a df showing the coins that are stale enough to need updates
        2. for each coin in need of updates, ping the coingecko api and format any
            new records, then upload them to bigquery.
    '''
    # 1. retrieve list of coins with a coingecko_id that need market data updates
    updates_df = retrieve_updates_df()
    logger.info('retrieved bigquery for %s records with stale coingecko market data...',
                str(updates_df.shape[0]))

    # 2. retrieve market data for each coin in need of updates

    all_market_data = []
    for i in range(updates_df.shape[0]):
        try:
            # coingecko free api limit is 30 calls per minute so pause 2 seconds.
            # also prevents the cloud function from creating too many instances.
            if i > 0:
                time.sleep(2)

            # store iteration-specific variables
            coingecko_id = updates_df['coingecko_id'][i]

            # prepare pandas date series of existing records to filter to only new rows
            dates_with_records = updates_df[updates_df['coingecko_id']==coingecko_id]['all_dates']
            dates_with_records = pd.Series([date for date in dates_with_records.iloc[0]])
            dates_with_records = pd.to_datetime(dates_with_records).dt.tz_localize('UTC')

            # retrieve coingecko market data
            logger.info('retreiving coingecko data for %s...', coingecko_id)
            market_df,api_status_code = retrieve_coingecko_market_data(coingecko_id)

            if api_status_code == 200:
                if not market_df.empty:
                    logger.info('successfully retrieved %s records for %s.'
                                , str(market_df.shape[0]), coingecko_id)

                    # format and filter market data to prepare for upload
                    market_df = format_and_add_columns(market_df, coingecko_id, dates_with_records)
                    all_market_data.append(market_df)

                else:
                    logger.info('no new records found for %s.', coingecko_id)

            elif api_status_code == 404:
                # blacklist ids that return 404 responses so we don't keep retrying them
                blacklist_coingecko_id(coingecko_id)

        except requests.RequestException as req_err:
            logger.error('Network error while retrieving data for %s: %s', coingecko_id, req_err)
        except ValueError as val_err:
            logger.error('Data processing error for %s: %s', coingecko_id, val_err)
        except Exception as e: # pylint: disable=W0718
            logger.error('Unexpected error occurred for coingecko_id %s: %s', coingecko_id, str(e))
            logger.debug('Stack trace: ', exc_info=True)

            continue

    # Combine all data into a single DataFrame and upload it at once
    if all_market_data:
        combined_market_df = pd.concat(all_market_data, ignore_index=True)
        logger.info('uploading combined market data...')
        upload_market_data(combined_market_df)

    logger.info('update_coin_market_data_coingecko() completed successfully.')

    return '{{"status":"200"}}'



def retrieve_updates_df():
    '''
    pulls a list of tokens with coingecko ids from bigquery, limiting to coins that either \
        have no market data or that have data at least 2 days old.

    returns:
        updates_df (dataframe): list of tokens that need price updates from coingecko
    '''

    query_sql = '''
        with coingecko_data_status as (
            select cgi.coingecko_id
            ,max(md.updated_at) as most_recent_record
            from `etl_pipelines.coin_coingecko_ids` cgi
            left join `etl_pipelines.coin_market_data_coingecko` md on md.coingecko_id = cgi.coingecko_id

            -- filter to remove ids that result in 404 responses
            left join `etl_pipelines.coingecko_ids_blacklist` bl on bl.coingecko_id = cgi.coingecko_id
            where cgi.coingecko_id is not null
            and bl.coingecko_id is null
            group by 1
        )

        select cds.coingecko_id
        ,cds.most_recent_record
        ,array_agg(cmd.date IGNORE NULLS order by cmd.date asc) as all_dates
        from coingecko_data_status cds
        join `etl_pipelines.coin_market_data_coingecko` cmd on cmd.coingecko_id = cds.coingecko_id
        where (
            cds.most_recent_record is null
            or cds.most_recent_record < (current_date('UTC') - 2)
        )
        group by 1,2
        '''

    updates_df = dgc().run_sql(query_sql)

    return updates_df



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
    retry_attempts = 3

    for _ in range(retry_attempts):
        market_df,api_status_code = ping_coingecko_api(coingecko_id)
        if api_status_code == 200:
            break
        elif api_status_code == 404:
            break
        elif api_status_code == 429:
            logger.info('coingecko api call timed out, retrying after a 60 second pause...')
            time.sleep(60)
            continue
        else:
            logger.error('unexpected coingecko api status code %s for %s, continuing to next coin.',
                str(api_status_code), coingecko_id)
            break
    logger.info('coingecko api call for %s completed with status code %s.',
        coingecko_id, str(api_status_code))

    return market_df, api_status_code



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



def format_and_add_columns(df, coingecko_id, dates_with_records):
    '''
    Converts json data from the coingecko api into a table-formatted dataframe by converting
    columns of tuples into standardized columns that match the bigquery table format.

    params:
        df (pandas.DataFrame): df of coingecko market data
        coingecko_id (str): coingecko id of coin
        dates_with_records (pandas.Series): series of all dates with records for the coingecko_id
            in etl_pipelines.coin_market_data_coingecko

    returns:
        df (pandas.DataFrame): formatted df of market data
    '''
    # Loop through each row in the DataFrame and apply the function
    for index, row in df.iterrows():
        # Formatting unix timestamp of prices column
        df.at[index, 'prices'] = replace_unix_timestamp(row['prices'])

        # Removing the unix timestamps from the columns
        df.at[index, 'market_caps'] = row['market_caps'][1]
        df.at[index, 'total_volumes'] = row['total_volumes'][1]

    df = df.assign(
        date=[i[0] for i in df['prices']],
        prices=[i[1] for i in df['prices']],
        coingecko_id=coingecko_id
    )

    # Rearranging and renaming columns
    df = df[['coingecko_id', 'date', 'prices', 'market_caps', 'total_volumes']]
    df.columns = ['coingecko_id', 'date', 'price', 'market_cap', 'volume']

    # Convert market_cap and volumes to integers
    df['market_cap'] = df['market_cap'].astype(int)
    df['volume'] = df['volume'].astype(int)

    # Convert date column to UTC datetime
    df['date'] = pd.to_datetime(df['date'])
    df['date'] = df['date'].dt.tz_localize('UTC')

    # Find and drop the row with the most recent date to avoid partial date data
    max_date_index = df['date'].idxmax()
    df = df.drop(max_date_index)

    # Round date to nearest day
    df['date'] = pd.to_datetime(df['date']).dt.floor('D')

    # Filter out records that already exist in the database based on dates_with_records
    df = df[~df['date'].isin(dates_with_records)]

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
    api_key = os.getenv('COINGECKO_API_KEY')

    url = f'https://api.coingecko.com/api/v3/coins/{coingecko_id}/market_chart?vs_currency=usd&days=365&interval=daily&x_cg_demo_api_key={api_key}' # pylint: disable=C0301
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

    # add metadata to upload_df
    upload_df = pd.DataFrame()
    upload_df['date'] = market_df['date']
    upload_df['coingecko_id'] = market_df['coingecko_id']
    upload_df['price'] = market_df['price']
    upload_df['market_cap'] = market_df['market_cap']
    upload_df['volume'] = market_df['volume']
    upload_df['updated_at'] = datetime.datetime.now(utc).strftime('%Y-%m-%d %H:%M:%S')

    # set df datatypes of upload df
    dtype_mapping = {
        'date': 'datetime64[ns, UTC]',
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


def blacklist_coingecko_id(coingecko_id: str):
    """
    Blacklist a Coingecko ID by inserting it into the specified BigQuery table
    when a 404 error is encountered.

    Args:
        coingecko_id (str): The Coingecko ID to be blacklisted.
    Returns:
        None
    """
    # Initialize BigQuery client
    client = bigquery.Client()

    # Prepare the full table name
    table_full_name = "western-verve-411004.etl_pipelines.coingecko_ids_blacklist"

    # Prepare the SQL query to insert the blacklisted coingecko_id
    insert_blacklist_query = f"""
        INSERT INTO `{table_full_name}` (coingecko_id, blacklisted_at, reason)
        VALUES ('{coingecko_id}', CURRENT_TIMESTAMP(), '404 Not Found')
    """

    # Run the query using the BigQuery client
    client.query(insert_blacklist_query)

    logger.info('Coingecko ID %s has been blacklisted due to 404 response.', coingecko_id)
