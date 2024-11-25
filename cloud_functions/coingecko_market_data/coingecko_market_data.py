'''
cloud function that updates the bigquery table `etl_pipelines.coin_market_data_coingecko` by \
    making api calls to coingecko to get market data and uploading it
'''
import datetime
import os
import concurrent.futures
from pytz import utc
import pandas as pd
import requests
import pandas_gbq
import functions_framework
from google.cloud import bigquery
from dreams_core.googlecloud import GoogleCloud as dgc
from dreams_core import core as dc

# pylint: disable=W1203  # f strings in logs

# set up logger at the module level
logger = dc.setup_logger()


@functions_framework.http
def update_coin_market_data_coingecko(request):
    '''
    Cloud function that updates the BigQuery table `etl_pipelines.coin_market_data_coingecko` by
    making concurrent API calls to CoinGecko to get market data and uploading it.

    The function processes coins in parallel batches using a thread pool executor. Each batch
    retrieves market data for multiple coins and uploads the combined results to BigQuery.

    Args:
        request (flask.Request): The request object containing optional parameters:
            - batch_size (int): Number of coins to process in each batch (default: 100)
            - max_workers (int): Number of concurrent worker threads (default: 5)

    Request Parameters:
        batch_size (optional): Number of coins per batch
            - Type: integer
            - Default: 100
            - Example: ?batch_size=50
        max_workers (optional): Number of concurrent threads
            - Type: integer
            - Default: 5
            - Example: ?max_workers=3

    Returns:
        str: JSON string with status code indicating completion

    Key Steps:
        1. Retrieve list of coins needing market data updates
        2. Split coins into batches for concurrent processing
        3. Process batches in parallel using thread pool
        4. Combine results and upload to BigQuery

    Example Usage:
        # Default parameters
        POST /update_coin_market_data_coingecko

        # Custom batch size and workers
        POST /update_coin_market_data_coingecko?batch_size=50&max_workers=3
    '''
    # Get parameters from request with defaults
    batch_size = int(request.args.get('batch_size', 100))
    max_workers = int(request.args.get('max_workers', 5))

    logger.info(f'Starting update with batch_size={batch_size}, max_workers={max_workers}')

    # Retrieve coins needing updates
    updates_df = retrieve_updates_df()
    logger.info('retrieved bigquery for %s records with stale coingecko market data...',
                len(updates_df))

    # Initialize a single BigQuery client
    client = bigquery.Client()

    # Split updates_df into batches
    batches = [updates_df[i:i + batch_size] for i in range(0, len(updates_df), batch_size)]

    all_market_data = []

    # Process batches concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_batch = {executor.submit(process_coin_batch, batch, client): i
                          for i, batch in enumerate(batches)}

        for future in concurrent.futures.as_completed(future_to_batch):
            batch_num = future_to_batch[future]
            try:
                batch_results = future.result()
                if batch_results:
                    all_market_data.extend(batch_results)
                    logger.info(f'Completed batch {batch_num + 1}/{len(batches)}')
            except Exception as e:
                logger.error(f'Batch {batch_num} generated an exception: {str(e)}')

    # Upload all collected data
    if all_market_data:
        combined_market_df = pd.concat(all_market_data, ignore_index=True)
        logger.info('Uploading combined market data...')
        upload_market_data(combined_market_df)

    logger.info('update_coin_market_data_coingecko() completed successfully.')
    return '{"status":"200"}'



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
            ,max(sl.created_at) as most_recent_search
            ,max(cmd.date) as most_recent_market_data
            ,max(case when api_code = 404 then 1 else 0 end) as has_404_code
            from `core.coin_facts_coingecko` cgi
            left join `etl_pipelines.coin_market_data_coingecko` cmd on cmd.coingecko_id = cgi.coingecko_id
            left join `etl_pipelines.coin_market_data_coingecko_search_logs` sl on sl.coingecko_id = cgi.coingecko_id
            group by 1
        )

        select cds.coingecko_id
        ,cds.most_recent_search
        ,array_agg(cmd.date IGNORE NULLS order by cmd.date asc) as all_dates
        from coingecko_data_status cds
        left join `etl_pipelines.coin_market_data_coingecko` cmd on cmd.coingecko_id = cds.coingecko_id
        where (
            -- hasn't been searched in the last 2 days
            (cds.most_recent_search is null
            or cds.most_recent_search < (current_date('UTC') - 2))
            AND
            -- doesn't have market data from the last 2 days
            ((cds.most_recent_market_data) < (current_date('UTC') - 2))
        )
        -- has never had 404 code
        and cds.has_404_code = 0
        group by 1,2
        limit 25
        '''

    updates_df = dgc().run_sql(query_sql)

    return updates_df



def process_coin_batch(coin_batch_df, client):
    """
    Process a batch of coins and return their market data

    Args:
        coin_batch_df (pd.DataFrame): DataFrame containing a batch of coins to process
        client (bigquery.Client): Shared BigQuery client instance

    Returns:
        list: List of processed market data DataFrames
    """
    batch_market_data = []

    for _, row in coin_batch_df.iterrows():
        try:
            coingecko_id = row['coingecko_id']
            dates_with_records = row['all_dates']

            if dates_with_records is not None and len(dates_with_records) > 0:
                dates_with_records = pd.Series([date for date in dates_with_records])
                dates_with_records = pd.to_datetime(dates_with_records).dt.tz_localize('UTC')

            api_market_df, api_status_code = retrieve_coingecko_market_data(coingecko_id)
            log_market_data_search(client, coingecko_id, api_status_code, api_market_df)

            if api_status_code == 200 and not api_market_df.empty:
                market_df = format_and_add_columns(api_market_df, coingecko_id, dates_with_records)
                if not market_df.empty:
                    batch_market_data.append(market_df)

        except Exception as e:
            logger.error(f"Error processing {coingecko_id}: {str(e)}")
            continue

    return batch_market_data



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
    full_row_count = len(df)
    df = df[~df['date'].isin(dates_with_records)]
    logger.info(' %s/%s records for %s were new.',
                len(df), full_row_count, coingecko_id)

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
    coingecko_api_key = os.getenv('COINGECKO_API_KEY')

    url = f'https://pro-api.coingecko.com/api/v3/coins/{coingecko_id}/market_chart?vs_currency=usd&days=365&interval=daily' # pylint: disable=C0301

    headers = {
        "accept": "application/json",
        "x-cg-pro-api-key": coingecko_api_key
    }
    r = requests.get(url, headers=headers, timeout=30)

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

    # Filter out records for the current UTC date
    current_utc_date = pd.Timestamp.now(tz='UTC').normalize()
    market_df = market_df[market_df['date'] < current_utc_date]

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



def log_market_data_search(client, coingecko_id, api_status_code, market_df):
    """
    Logs search outcome to BigQuery using streaming inserts.
    This is used to avoid making duplicate requests to:
        - inactive coins that return 404 responses
        - coins with partial date coverage that appear to always need updates
            based on their coin_market_data coverage

    Args:
        coingecko_id (str): The Coingecko ID to be blacklisted.
        api_status_code (int): The API code from the request
        market_df (pd.DataFrame): The df returned by the API
    Returns:
        None
    """
    # Prepare the full table name
    table_full_name = "western-verve-411004.etl_pipelines.coin_market_data_coingecko_search_logs"

    # Prepare the row data
    row_to_insert = [
        {
            "coingecko_id": coingecko_id,
            "api_code": api_status_code,
            "records_returned": len(market_df),
            "created_at": datetime.datetime.now(utc).strftime('%Y-%m-%d %H:%M:%S')
        }
    ]

    # Use streaming insert to add the data
    errors = client.insert_rows_json(table_full_name, row_to_insert)
    if errors:
        logger.error("Failed to log market data search outcome: %s", errors)
    else:
        logger.info("Logged market data search outcome for %s.", coingecko_id)
