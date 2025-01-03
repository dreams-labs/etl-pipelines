"""
This module processes Geckoterminal JSON data stored in Cloud Storage and uploads it to BigQuery.
It identifies coins needing processing, retrieves JSON data, and uploads the aggregated metadata.
Functions include fetching data, combining metadata from multiple sources, and logging outcomes.
Parallel processing is used to handle multiple coins efficiently, with error handling at each step.
"""
import datetime
from concurrent.futures import ThreadPoolExecutor
import json
import functions_framework
from google.cloud import bigquery
from google.cloud import storage
import dreams_core.core as dc
from dreams_core.googlecloud import GoogleCloud as dgc

# Set up logger at the module level
logger = dc.setup_logger()

# Centralized config for bucket paths and table names
CONFIG = {
    'bucket_name': 'dreams-labs-storage',
    'main_metadata_path': 'data_lake/geckoterminal_coin_metadata_main/',
    'info_metadata_path': 'data_lake/geckoterminal_coin_metadata_info/',
    'metadata_table': 'western-verve-411004.etl_pipelines.coin_geckoterminal_metadata',
}

@functions_framework.http
def parse_geckoterminal_json(request):  # pylint: disable=unused-argument  # noqa: F841
    """
    Main function to parse and process the Geckoterminal JSON data.

    This function serves as an HTTP endpoint to trigger the parsing and processing
    of Geckoterminal data stored in Google Cloud Storage. It identifies unprocessed
    JSON files, fetches the data for each coin, and uploads various components
    (metadata, and contracts) to the corresponding BigQuery table.

    Workflow:
    ---------
    1. Identify Coins to Process:
       - Calls `identify_coins_to_process()` to compare existing JSON files in Google Cloud Storage
         with records in the BigQuery table `etl_pipelines.coin_geckoterminal_metadata`.
       - Identifies coins whose data has not yet been uploaded to BigQuery.

    2. Fetch and Process JSON Data:
       - For each identified coin, the function retrieves the corresponding JSON data
         using `fetch_coin_json(coin)`.

    3. Upload Data to BigQuery:
       - The function processes and uploads the metadata using `upload_metadata(json_data)`.
    """
    # Initialize clients
    storage_client = storage.Client()
    bigquery_client = bigquery.Client()

    # Identify coins that haven't yet been processed
    coins_to_process = identify_coins_to_process(storage_client)

    # Process each coin in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(process_coin, coin, storage_client, bigquery_client)
            for coin in coins_to_process
        ]
        for future in futures:
            future.result()  # Ensure exceptions are raised

    return f"Geckoterminal JSON parsing complete. Processed {len(coins_to_process)} coins."


def identify_coins_to_process(storage_client):
    """
    Identifies the JSON files that need to be processed by comparing a list of all JSON files
    in Cloud Storage with the records in the `etl_pipelines.coin_geckoterminal_metadata`
    BigQuery table.

    Returns: A list of coins with valid JSON files that have not been added to BigQuery.
    """
    # Retrieve the list of JSON files from Cloud Storage
    bucket = storage_client.bucket(CONFIG['bucket_name'])
    blobs = bucket.list_blobs(prefix=CONFIG['main_metadata_path'])

    # Only include blobs that are actual JSON files and not directories or empty entries
    coins_with_json = [
        blob.name.replace(CONFIG['main_metadata_path'], '').replace('_main.json', '')
        for blob in blobs if blob.name.endswith('_main.json')
    ]

    # Retrieve the list of coins already uploaded to BigQuery
    query_sql = f'''
        SELECT geckoterminal_id
        FROM {CONFIG['metadata_table']}
        GROUP BY 1
    '''
    query_df = dgc().run_sql(query_sql)
    coins_in_table = list(query_df['geckoterminal_id'])

    # Identify coins that need processing
    coins_to_process = [coin for coin in coins_with_json if coin not in coins_in_table]
    logger.info('Geckoterminal JSON blobs to process: %s', str(len(coins_to_process)))

    return coins_to_process



def process_coin(coin, storage_client, bigquery_client):
    """
    Fetches and processes a coin's metadata from Geckoterminal JSON data.
    """
    try:
        # Fetch both JSON objects
        main_json_data = fetch_coin_json(coin,storage_client,CONFIG['main_metadata_path'],'_main')
        info_json_data = fetch_coin_json(coin,storage_client,CONFIG['info_metadata_path'],'_info')

        # Upload metadata to BigQuery
        upload_metadata(main_json_data, info_json_data, bigquery_client)

    except FileNotFoundError as fnf_error:
        logger.error('File not found for coin %s: %s', coin, str(fnf_error))
    except KeyError as key_error:
        logger.error('Key error in coin metadata for %s: %s', coin, str(key_error))
    except Exception as e: # pylint: disable=W0718
        # Generic fallback for any other unhandled exceptions
        logger.error('Unexpected error while processing coin %s: %s', coin, str(e))



def fetch_coin_json(coin, storage_client, metadata_path, endpoint_suffix):
    """
    Retrieves the JSON blob for a given coin from Google Cloud Storage.

    Args:
        coin (str): The unique identifier for the coin (without file extensions or suffixes).
        storage_client (google.cloud.storage.Client): An initialized Google Cloud Storage client.
        metadata_path (str): The path within the bucket where the coin JSON files are stored.
        endpoint_suffix (str): The API endpoint indicator, e.g. "_main" or "_info"

    Returns:
        dict: The parsed JSON content from the blob as a Python dictionary.

    Raises:
        google.cloud.exceptions.NotFound: If the JSON blob for the specified coin is not
        found in the bucket.
        json.JSONDecodeError: If the blob content cannot be decoded as valid JSON.
    """
    bucket = storage_client.bucket(CONFIG['bucket_name'])
    file_name = f'{metadata_path}{coin}{endpoint_suffix}.json'
    blob = bucket.blob(file_name)
    blob_contents = blob.download_as_string()

    return json.loads(blob_contents)


def upload_metadata(main_json_data, info_json_data, bigquery_client):
    """
    Combines main and info JSON data and uploads it to BigQuery.

    Args:
        main_json_data (dict): JSON response from the main Geckoterminal API endpoint.
        info_json_data (dict): JSON response from the info Geckoterminal API endpoint.
        bigquery_client (google.cloud.bigquery.Client): Initialized BigQuery client.

    Returns:
        None
    """
    table_id = CONFIG['metadata_table']
    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Determine the status of each JSON file
    main_json_status = 'success' if main_json_data else 'missing'
    info_json_status = 'success' if info_json_data else 'missing'

    # Check if there were any parsing or structure errors
    error_message = None
    if main_json_data and not isinstance(main_json_data.get('data'), dict):
        main_json_status = 'error'
        error_message = 'Main JSON structure invalid.'

    if info_json_data and not isinstance(info_json_data.get('data'), dict):
        info_json_status = 'error'
        error_message = 'Info JSON structure invalid.'

    # Set the overall status
    if main_json_status == 'success' and info_json_status == 'success':
        overall_status = 'complete'
    elif main_json_status == 'success' and info_json_status == 'missing':
        overall_status = 'main_only'
    elif main_json_status == 'missing' and info_json_status == 'success':
        overall_status = 'info_only'
    else:
        overall_status = 'error' if error_message else 'missing'

    # Handle missing or invalid relationships and top_pools data
    top_pools = []
    if main_json_data and 'relationships' in main_json_data['data']:
        top_pools = main_json_data['data']['relationships'].get('top_pools', {}).get('data', [])

        # 1. Retrieve total_supply and decimals from the respective JSON data
        if main_json_data:
            raw_total_supply = main_json_data['data']['attributes'].get('total_supply', None)
        else:
            raw_total_supply = None

        if info_json_data:
            decimals = info_json_data['data']['attributes'].get('decimals', None)
        else:
            decimals = None

        # 2. Calculate total_supply_adjusted by dividing raw_total_supply by 10^decimals
        if raw_total_supply is not None and decimals is not None:
            try:
                total_supply_adjusted = float(raw_total_supply) / (10 ** int(decimals))

                # 3. If total_supply_adjusted exceeds 10^20, set it to None
                # numbers above this scale cause type errors and are likely bad geckoterminal data
                if total_supply_adjusted > 10**20:
                    total_supply_adjusted = None

            except (ValueError, TypeError):
                total_supply_adjusted = None  # Handle cases where casting fails
        else:
            total_supply_adjusted = None  # Set to None if total_supply or decimals are missing

    # Prepare the aggregated data for upload
    # pylint: disable=C0301  # Disables line-length warnings for the following lines
    rows_to_insert = [{
        'geckoterminal_id': main_json_data['data']['id'] if main_json_data else None,
        'name': info_json_data['data']['attributes'].get('name', None) if info_json_data else None,
        'symbol': info_json_data['data']['attributes'].get('symbol', None) if info_json_data else None,
        'address': info_json_data['data']['attributes'].get('address', None) if info_json_data else None,
        'decimals': info_json_data['data']['attributes'].get('decimals', None) if info_json_data else None,
        'image_url': info_json_data['data']['attributes'].get('image_url', None) if info_json_data else None,
        'coingecko_coin_id': info_json_data['data']['attributes'].get('coingecko_coin_id', None) if info_json_data else None,
        'websites': info_json_data['data']['attributes'].get('websites', []) if info_json_data else [],
        'description': info_json_data['data']['attributes'].get('description', None) if info_json_data else None,
        'gt_score': info_json_data['data']['attributes'].get('gt_score', None) if info_json_data else None,
        'discord_url': info_json_data['data']['attributes'].get('discord_url', None) if info_json_data else None,
        'telegram_handle': info_json_data['data']['attributes'].get('telegram_handle', None) if info_json_data else None,
        'twitter_handle': info_json_data['data']['attributes'].get('twitter_handle', None) if info_json_data else None,
        'total_supply_raw': main_json_data['data']['attributes'].get('total_supply', None) if main_json_data else None,
        'total_supply': total_supply_adjusted,
        'top_pools': top_pools,
        'main_json_status': main_json_status,
        'info_json_status': info_json_status,
        'overall_status': overall_status,
        'error_message': error_message,
        'retrieval_date': current_time
    }]

    # Upload to BigQuery
    errors = bigquery_client.insert_rows_json(table_id, rows_to_insert)
    if not errors:
        logger.info("Successfully inserted metadata for coin %s",
                    main_json_data['data']['id'])
    else:
        logger.error("Error inserting metadata for coin %s: %s",
                     main_json_data['data']['id'], errors)

def insert_rows(bigquery_client, table_id, rows_to_insert):
    """
    Inserts rows into BigQuery and logs the outcome.
    """
    errors = bigquery_client.insert_rows_json(table_id, rows_to_insert)
    success_count = len(rows_to_insert) - len(errors)
    failure_count = len(errors)

    logger.info("%s rows inserted into %s", success_count, table_id)
    if failure_count > 0:
        logger.error("%s rows failed to insert into %s: %s", failure_count, table_id, errors)
