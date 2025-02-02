"""
cloud function that updates the bigquery table `etl_pipelines.coin_market_data_coingecko` by \
    making api calls to coingecko to get market data and uploading it
"""
import time
import datetime
import os
import uuid
import logging
import json
import concurrent.futures
from pytz import utc
import pandas as pd
import requests
import pandas_gbq
import functions_framework
from google.cloud import bigquery, storage, exceptions
from dreams_core.googlecloud import GoogleCloud as dgc
from dreams_core import core as dc

# pylint: disable=W1203  # f strings in logs

# set up logger at the module level
logger = dc.setup_logger()

# suppress dreams_core.googlecloud info logs
logging.getLogger('dreams_core.googlecloud').setLevel(logging.WARNING)

@functions_framework.http
def update_coin_market_data_coingecko(request):
    """
    Cloud function that updates the BigQuery table `etl_pipelines.coin_market_data_coingecko` by
    making concurrent API calls to CoinGecko to get market data and uploading it.

    The function processes coins in parallel batches using a thread pool executor. Each batch
    retrieves market data for multiple coins and uploads its results to BigQuery.

    Args:
        request (flask.Request): The request object containing optional parameters:
            - batch_size (int): Number of coins to process in each batch (default: 100)
            - max_workers (int): Number of concurrent worker threads (default: 5)
            - retry_recent_searches (bool): Whether to reattempt retrieval of market data for
                coins that have had attempts in the last 2 days

    Returns:
        str: JSON string with status and batch processing results
    """
    # Get parameters from request with defaults
    batch_size = int(request.args.get('batch_size', 100))
    max_workers = int(request.args.get('max_workers', 5))
    retry_recent_searches = request.args.get('retry_recent_searches', False)

    logger.info(f'Starting update with batch_size={batch_size}, max_workers={max_workers}')

    # Retrieve coins needing updates
    updates_df = retrieve_updates_df(retry_recent_searches)
    logger.info('retrieved freshness state for %s coins with stale coingecko market data...',
                len(updates_df))

    # Initialize a single BigQuery client
    client = bigquery.Client()

    # Split updates_df into batches
    batches = [updates_df[i:i + batch_size] for i in range(0, len(updates_df), batch_size)]

    successful_batches = 0
    failed_batches = 0

    # Process batches concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_batch = {executor.submit(process_coin_batch, batch, client): i
                          for i, batch in enumerate(batches)}

        for future in concurrent.futures.as_completed(future_to_batch):
            batch_num = future_to_batch[future]
            try:
                batch_success = future.result()
                if batch_success:
                    successful_batches += 1
                    logger.info(f'Successfully completed batch {batch_num + 1}/{len(batches)}')
                else:
                    failed_batches += 1
                    logger.warning(f'Batch {batch_num + 1}/{len(batches)} completed with no data')
            except Exception as e:  # pylint:disable=broad-exception-caught
                failed_batches += 1
                logger.error(f'Batch {batch_num + 1} generated an exception: {str(e)}')

    logger.info(f'update_coin_market_data_coingecko() completed. '
                f'Successful batches: {successful_batches}, Failed batches: {failed_batches}')

    return json.dumps({
        "status": "200",
        "successful_batches": successful_batches,
        "failed_batches": failed_batches,
        "total_batches": len(batches)
    })



def retrieve_updates_df(retry_recent_searches=False):
    """
    pulls a list of tokens with coingecko ids from bigquery, limiting to coins that either \
        have no market data or that have data at least 2 days old.

    params:
        retry_recent_searches (bool): whether to retry coins that have been attempted in
            the last 2 days

    returns:
        updates_df (dataframe): list of tokens that need price updates from coingecko
    """
    # If configured to retry recent searches, disable the where clause that filters them out
    if retry_recent_searches:
        recent_searches_sql = ""
    else:
        recent_searches_sql = """
            and (
                -- include if there aren't any past searches
                cds.most_recent_search is null
                -- include if the last search was over 2 days ago
                OR cds.most_recent_search < (current_date('UTC') - 2)
            )
            """

    query_sql = f"""
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
        left join `etl_pipelines.coin_market_data_coingecko_search_logs` sl on sl.coingecko_id = cds.coingecko_id
            and sl.created_at = cds.most_recent_search
        left join `etl_pipelines.coin_market_data_coingecko` cmd on cmd.coingecko_id = cds.coingecko_id

        -- criteria 1: only search if there isn't market data from the last 2 days
        where (
            cds.most_recent_market_data is null
            or (cds.most_recent_market_data) < (current_date('UTC') - 2)
        )

        -- criteria 2: only search if there's no prior/recent searches
        {recent_searches_sql}

        -- criteria 3: exclude if it has ever returned a 404 code
        and cds.has_404_code = 0
        group by 1,2
        """

    updates_df = dgc().run_sql(query_sql)

    return updates_df



def process_coin_batch(coin_batch_df, client):
    """
    Process a batch of coins and upload their combined market data to BigQuery

    Args:
        coin_batch_df (pd.DataFrame): DataFrame containing a batch of coins to process
        client (bigquery.Client): Shared BigQuery client instance

    Returns:
        bool: True if batch was successfully processed and uploaded, False otherwise
    """
    batch_market_data = []

    for _, row in coin_batch_df.iterrows():
        try:
            coingecko_id = row['coingecko_id']
            dates_with_records = row['all_dates']
            new_row_count = None

            if dates_with_records is not None and len(dates_with_records) > 0:
                dates_with_records = pd.Series([date for date in dates_with_records])
                dates_with_records = pd.to_datetime(dates_with_records).dt.tz_localize('UTC')

            api_market_df, api_status_code = retrieve_coingecko_market_data(coingecko_id)

            if api_status_code == 200 and not api_market_df.empty:
                market_df, new_row_count = format_and_add_columns(api_market_df, coingecko_id, dates_with_records)
                if not market_df.empty:
                    batch_market_data.append(market_df)

            log_market_data_search(client, coingecko_id, api_status_code, api_market_df, new_row_count)


        except requests.RequestException as e:
            logger.error(f"API request failed for {coingecko_id}: {str(e)}")
            continue
        except ValueError as e:
            logger.error(f"Data formatting error for {coingecko_id}: {str(e)}")
            continue
        except pd.errors.DataError as e:
            logger.error(f"DataFrame operation error for {coingecko_id}: {str(e)}")
            continue
        except KeyError as e:
            logger.error(f"Missing key in API response for {coingecko_id}: {str(e)}")
            continue

    # After processing all coins in the batch, combine and upload the results
    if batch_market_data:
        try:
            combined_market_df = pd.concat(batch_market_data, ignore_index=True)
            upload_market_data(combined_market_df)
            logger.debug(f"Successfully uploaded batch data with {len(batch_market_data)} coins")
            return True
        except Exception as upload_error:  # pylint: disable=broad-except
            logger.error(f"Failed to upload batch data: {str(upload_error)}")

            # Store the failed batch in GCS and BigQuery
            log_failed_upload(combined_market_df)
            return False

    # If no new records were returned, the batch sequence still succeeded
    logger.warning("No market data collected for this batch")
    return True



def retrieve_coingecko_market_data(coingecko_id):
    """
    Attempts to retrieve data from coingecko API with exponential backoff.

    Params:
        coingecko_id (string): coingecko id of coin

    Returns:
        market_df (dataframe): raw api response of market data
        api_status_code (int): status code of coingecko api call
    """
    base_delay = 1  # Start with 1 second delay
    max_delay = 32  # Max delay of 32 seconds
    retry_attempts = 5

    for attempt in range(retry_attempts):
        market_df, api_status_code = ping_coingecko_api(coingecko_id)

        if api_status_code == 200:
            break
        elif api_status_code == 404:
            break
        elif api_status_code == 429 or api_status_code == 0:  # 0 for empty responses
            delay = min(base_delay * (2 ** attempt), max_delay)
            logger.info('Rate limited, waiting %s seconds before retry...', delay)
            time.sleep(delay)
            continue
        else:
            logger.error('Unexpected coingecko api status code %s for %s',
                str(api_status_code), coingecko_id)
            break

    logger.debug('coingecko api call for %s completed with status code %s',
        coingecko_id, str(api_status_code))

    return market_df, api_status_code



def strip_and_format_unixtime(unix_time):
    """
    converts a coingecko unix timestamp into a single date datetime. this includes logic that \
        assigns mid-day records to a single date

    params:
        unix_time (int): unix timestamp

    returns:
        formatted_datetime (datetime): datetime derived from the unix timestamp
    """
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
    """
    utility function to replace coingecko tuple responses with unix timestamps into datetimes

    params:
        lst (tuple): tuple response from coingecko witha unix timestamp as the first value

    returns:
        lst (datetime): datetime derived from the unix timestamp
    """
    lst[0] = strip_and_format_unixtime(lst[0])
    return lst



def format_and_add_columns(df, coingecko_id, dates_with_records):
    """
    Converts json data from the coingecko api into a table-formatted dataframe by converting
    columns of tuples into standardized columns that match the bigquery table format.

    params:
        df (pandas.DataFrame): df of coingecko market data
        coingecko_id (str): coingecko id of coin
        dates_with_records (pandas.Series): series of all dates with records for the coingecko_id
            in etl_pipelines.coin_market_data_coingecko

    returns:
        df (pandas.DataFrame): formatted df of market data
        new_row_count (int): how many rows were new data (used for logging)
    """
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
    df['market_cap'] = df['market_cap'].apply(lambda x: int(round(x)) if pd.notna(x) else x)
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
    new_row_count = len(df)
    logger.info(' %s/%s records for %s were new.',
                new_row_count, full_row_count, coingecko_id)

    # drop duplicate dates if exists. this only occurs if a coin is removed from coingecko, e.g.:
    # https://www.coingecko.com/en/coins/serum-ser
    # https://www.coingecko.com/en/coins/chart-roulette
    df = df.drop_duplicates(subset='date', keep='last')

    return df, new_row_count



def ping_coingecko_api(coingecko_id):
    """
    requests market data for a given coingecko_id.

    note that no api key is used, as inputting a free plan api key in the headers causes the \
        call to error out. also stores the json response in cloud storage in case there any \
        improper adjustments are later detected.

    params:
        coingecko_id (str): coingecko id of coin

    returns:
        df (dataframe): formatted df of market data
        status_code (int): status code of coingecko api call
    """
    coingecko_api_key = os.getenv('COINGECKO_API_KEY')

    url = f'https://pro-api.coingecko.com/api/v3/coins/{coingecko_id}/market_chart?vs_currency=usd&days=3650&interval=daily' # pylint: disable=C0301

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


def format_upload_df(market_df):
    """
    Formats the market data for bigquery upload, including special handling for the
    price field's NUMERIC datatype.

    Params:
    - market_df (df): dataframe with the api response market data

    Returns:
    - upload_df (df): the param df formatted for bugquery upload
    """
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


    # Update price column to fit in bigquery's NUMERIC datatype
    def adjust_for_bigquery_numeric(value):
        # Parse scale from scientific notation
        str_value = f"{value:.15e}"
        _, exponent = str_value.split('e')
        scale = abs(int(exponent))  # Get the absolute scale

        # if the precision is too small then return 0
        if int(exponent) < -37:
            return 0

        # Adjust scale for small numbers (negative exponent)
        if int(exponent) < 0:
            remaining_precision = max(38 - scale, 0)
            return format(value, f'.{remaining_precision}e')

        else:
            # For large numbers, no scaling needed, just ensure precision fits
            return value

    upload_df['price'] = upload_df['price'].apply(adjust_for_bigquery_numeric)

    return upload_df


def upload_market_data(market_df, max_retries=3, base_delay=3):
    """
    Appends market data to BigQuery with exponential backoff retries.

    Params:
        market_df (DataFrame): Market data to upload
        max_retries (int): Maximum retry attempts
        base_delay (int): Base delay in seconds between retries

    Returns:
        bool: Success status
    """
    # Format the data for upload
    upload_df = format_upload_df(market_df)

    # Attempt upload with retries
    client = bigquery.Client()
    table_id = 'western-verve-411004.etl_pipelines.coin_market_data_coingecko'

    for attempt in range(max_retries):
        try:
            errors = client.insert_rows_from_dataframe(
                client.get_table(table_id),
                upload_df
            )

            # Check if errors exist and aren't empty
            if errors and any(errors):
                # pylint:disable=broad-exception-raised
                raise Exception(f'Failed to stream records: {errors}')
            else:
                logger.info(f'Streamed {len(upload_df)} records to {table_id}')
                return True

        # pylint:disable=broad-exception-caught
        except Exception as e:
            delay = base_delay * (2 ** attempt)
            if attempt < max_retries - 1:
                logger.warning(f'Upload attempt {attempt + 1} failed, retrying in {delay}s: {str(e)}')
                time.sleep(delay)
                continue
            else:
                logger.error(f'Upload failed after {max_retries} attempts: {str(e)}')

    return False


def log_market_data_search(client, coingecko_id, api_status_code, market_df, new_row_count):
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
        new_row_count (int): how many rows were new data
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
            "new_records": new_row_count,
            "created_at": datetime.datetime.now(utc).strftime('%Y-%m-%d %H:%M:%S')
        }
    ]

    # Use streaming insert to add the data
    errors = client.insert_rows_json(table_full_name, row_to_insert)
    if errors:
        logger.error("Failed to log market data search outcome: %s", errors)
    else:
        logger.debug("Logged market data search outcome for %s.", coingecko_id)


def log_failed_upload(combined_market_df):
    """
    Logs failed uploads to GCS and bigquery. The full df is stored in GCS folder
    'etl_objects/coingecko_market_data_failed_batches' and the coingecko_ids are stored
    in bigquery table 'etl_pipelines.coin_market_data_coingecko_upload_failures'.

    Params:
    - combined_market_df (pd.DataFrame): the data that failed to upload to the
        table etl_pipelines.coin_market_data_coingecko
    """

    # Define filename and upload failed batch for review
    current_datetime = datetime.datetime.now(utc).strftime('%Y%m%d_%H%M')
    random_uuid = uuid.uuid4()
    filename = f"failed_batch_{len(combined_market_df)}_{current_datetime}_{random_uuid}.csv"
    gcs_folder='etl_objects/coingecko_market_data_failed_batches'

    # Upload directly to GCS without temp files
    try:
        # Convert DataFrame to CSV string in memory
        csv_data = combined_market_df.to_csv(index=False).encode('utf-8')

        # Upload bytes directly
        storage_client = storage.Client()
        bucket = storage_client.bucket('dreams-labs-storage')
        blob = bucket.blob(f'{gcs_folder}/{filename}')
        blob.upload_from_string(csv_data, content_type='text/csv')

        logger.info("Uploaded failed batch to GCS as %s", filename)

    except exceptions.GoogleCloudError as gcs_error:
        logger.error("Failed to upload failed batch to GCS due to cloud error: %s", gcs_error)
    except IOError as io_error:
        logger.error("Failed to upload failed batch to GCS due to file handling error: %s", io_error)
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Failed to upload failed batch to GCS: %s", e)

    # Define df to be uploaded to BigQuery
    upload_failures_df = pd.DataFrame()
    upload_failures_df['coingecko_id'] = combined_market_df['coingecko_id']
    upload_failures_df['failed_at'] = datetime.datetime.now(utc).strftime('%Y-%m-%d %H:%M:%S')

    # set df datatypes of upload df
    dtype_mapping = {
        'coingecko_id': str,
        'failed_at': 'datetime64[ns, UTC]'
    }
    upload_failures_df = upload_failures_df.astype(dtype_mapping)

    # upload df to bigquery
    project_id = 'western-verve-411004'
    table_name = 'etl_pipelines.coin_market_data_coingecko_upload_failures'
    schema = [
        {'name':'coingecko_id', 'type': 'string'},
        {'name':'failed_at', 'type': 'datetime'}
    ]

    try:
        pandas_gbq.to_gbq(
            upload_failures_df
            ,table_name
            ,project_id=project_id
            ,if_exists='append'
            ,table_schema=schema
            ,progress_bar=False
        )
        logger.info('Appended coingecko_ids in failed batch to %s.', table_name)
    except exceptions.GoogleCloudError as bq_error:
        logger.error("Failed to log to BigQuery due to cloud error: %s", bq_error)
    except pandas_gbq.gbq.GenericGBQException as gbq_error:
        logger.error("Failed to log to BigQuery due to GBQ error: %s", gbq_error)
    except IOError as io_error:
        logger.error("Failed to log to BigQuery due to file handling error: %s", io_error)
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Failed to log to BigQuery: %s", e)
