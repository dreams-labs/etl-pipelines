"""
This module retrieves metadata for coins from Geckoterminal and stores the results in Google Cloud
Storage (GCS) and BigQuery. It performs the following sequence of operations:

1. retrieve_coingecko_metadata(request):
   - This is the entry point function, triggered via an HTTP request.
   - It queries BigQuery to retrieve a list of coins that need metadata from Geckoterminal.
   - For each coin in the list, it calls geckoterminal_metadata_search() to retrieve metadata
     and handle storage.

2. geckoterminal_metadata_search(blockchain, address, coin_id, storage_client, bigquery_client):
   - This function performs the actual metadata retrieval for a given blockchain and coin address.
   - It calls the fetch_geckoterminal_data() function to make an API request to Geckoterminal.
   - Based on the API response, it logs success or failure and stores the metadata in GCS if the
     request is successful.
   - It then logs the search result (including success, failure, or rate limit) in BigQuery.

3. fetch_geckoterminal_data(blockchain, address, max_retries=3, retry_delay=30):
   - This function makes the API call to Geckoterminal to retrieve the metadata for a specific coin.
   - It handles API rate-limiting by retrying up to max_retries if a 429 error is encountered.
   - Returns both the API response data and the status code for further processing in
     geckoterminal_metadata_search().

### Flow Summary:
The retrieve_coingecko_metadata() function starts by querying BigQuery for a list of coins that
require metadata. For each coin, it calls geckoterminal_metadata_search(), which in turn calls
fetch_geckoterminal_data() to retrieve metadata from Geckoterminal. If successful, the metadata is
stored in Google Cloud Storage, and the result (whether success, failure, or a rate-limiting event)
is logged in BigQuery.
"""
import time
import datetime
import logging
import json
import requests
import functions_framework
from google.cloud import bigquery
from google.cloud import storage
from dreams_core.googlecloud import GoogleCloud as dgc
import dreams_core.core as dc

# set up logger at the module level
logger = dc.setup_logger()


@functions_framework.http
def retrieve_coingecko_metadata(request):  # pylint: disable=unused-argument  # noqa: F841
    '''
    Queries BigQuery to obtain a list of coins that need metadata and attempts to match them and
    store metadata by calling geckoterminal_metadata_search() for each.
    '''

    # Initialize clients once and pass them down
    storage_client = storage.Client(project='dreams-labs-data')
    bigquery_client = bigquery.Client(project='dreams-labs-data')

    # Query to pull list of coins to update
    query_sql = '''
        select cc.coin_id, ch.chain_text_geckoterminal, cc.address
        from core.coins cc
        join core.chains ch on ch.chain_id = cc.chain_id
        left join `etl_pipelines.coin_geckoterminal_ids` cgi on cgi.coin_id = cc.coin_id
            and cgi.api_status_code <> 429 -- don't apply any filters based on api rate limit errors
        where cc.address is not null -- removes coins without addresses
        and ch.chain_text_geckoterminal is not null -- removes coins without geckoterminal chain aliases
        and cgi.coin_id is null -- removes coins that have already been searched for
        group by 1, 2, 3
    '''

    update_queue_df = dgc().run_sql(query_sql)
    logger.info('coins to update: %s', str(update_queue_df.shape[0]))

    # Iterate over the DataFrame directly
    for _, row in update_queue_df.iterrows():
        blockchain = row['chain_text_geckoterminal']
        address = row['address']
        coin_id = row['coin_id']

        logger.info('Initiating geckoterminal metadata search for <%s:%s>', blockchain, address)

        # Pass the storage and bigquery clients to the search function
        geckoterminal_metadata_search(blockchain, address, coin_id, storage_client, bigquery_client)

        logger.info('Pausing to avoid geckoterminal API rate limit issues...')
        time.sleep(5)

    return "Geckoterminal metadata update completed."



def geckoterminal_metadata_search(blockchain, address, coin_id, storage_client, bigquery_client):
    '''
    For a given blockchain and address, attempts to look up the coin on Geckoterminal by calling
    ping_geckoterminal_api() for both the main and info endpoints. If the search is successful,
    stores the metadata in GCS.

    param: blockchain <string> this must match chain_text_geckoterminal from core.chains
    param: address <string> token contract address
    param: coin_id <dataframe> core.coins.coin_id which is added to BigQuery records
    param: storage_client <google.cloud.storage.Client> GCS client object for storage
    param: bigquery_client <google.cloud.bigquery.Client> BigQuery client object
    '''
    # Step 1: API Call for main and info endpoints
    response_data, status_code = ping_geckoterminal_api(blockchain, address)
    info_response_data, info_status_code = ping_geckoterminal_api(blockchain, address, info=True)

    # Initialize the data for BigQuery and logging
    search_data = {
        'coin_id': coin_id,
        'geckoterminal_id': None,
        'search_successful': False,
        'api_status_code': status_code,
        'info_api_status_code': info_status_code,
        'search_log': '',
        'search_date': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

    # Step 2: Handle API Responses
    if status_code == 429 or info_status_code == 429:
        search_data['search_log'] = 'Rate limit exceeded (429), retrying later'
        logger.warning('Rate limit (429) encountered for <%s:%s>. Will retry later.',
                       blockchain, address)
    else:
        try:
            search_data['geckoterminal_id'] = response_data['data']['id']
            search_data['search_successful'] = True
            search_data['search_log'] = 'search successful'
            logger.info('Geckoterminal search successful for <%s:%s>', blockchain, address)
        except KeyError:
            search_data['search_log'] = 'KeyError: ID not found in response data'
            logger.info('FAILURE: KeyError - search failed for <%s:%s>', blockchain, address)
        except (TypeError, AttributeError):
            search_data['search_log'] = 'TypeError or AttributeError: Invalid response data'
            logger.info('FAILURE: TypeError or AttributeError - search failed for <%s:%s>',
                        blockchain, address)

    # Step 3: Store metadata in Google Cloud Storage (GCS) if the search is successful
    if search_data['search_successful']:
        bucket = storage_client.get_bucket('dreams-labs-storage')

        # Define paths for main and info endpoint data
        paths = {
            'main': 'data_lake/geckoterminal_coin_metadata_main/',
            'info': 'data_lake/geckoterminal_coin_metadata_info/'
        }

        # File naming convention
        filename_main = f"{response_data['data']['id']}_main.json"
        filename_info = f"{response_data['data']['id']}_info.json"

        # Function to upload data to GCS
        def upload_to_gcs(data, filename, path):
            blob = bucket.blob(path + filename)
            blob.upload_from_string(json.dumps(data), content_type='application/json')

        # Upload both main and info data
        upload_to_gcs(response_data, filename_main, paths['main'])
        upload_to_gcs(info_response_data, filename_info, paths['info'])

    # Step 4: Log search results in BigQuery
    table_id = 'western-verve-411004.etl_pipelines.coin_geckoterminal_ids'
    rows_to_insert = [search_data]
    errors = bigquery_client.insert_rows_json(table_id, rows_to_insert)

    if not errors:
        logger.debug("New row added to etl_pipelines.coin_geckoterminal_ids")
    else:
        logger.debug("Encountered errors while inserting rows: %s", errors)


def ping_geckoterminal_api(blockchain, address, max_retries=3, retry_delay=30, info=False):
    '''
    Makes an API call to Geckoterminal and returns the response data and status code.
    Supports the main token endpoint and the info endpoint based on the info parameter.
    Retries the call if a rate limit error (429) is encountered.

    param: blockchain <string> this must match chain_text_geckoterminal from core.chains
    param: address <string> token contract address
    param: max_retries <int> number of times to retry on 429 error
    param: retry_delay <int> delay in seconds between retries
    param: info <bool> whether to call the /info endpoint or the main token endpoint
    returns: tuple(response_data, status_code) JSON response data and API status code
    '''
    # Use the appropriate URL based on the info parameter
    base_url = f'https://api.geckoterminal.com/api/v2/networks/{blockchain}/tokens/{address}'
    url = f'{base_url}/info' if info else base_url

    for attempt in range(max_retries):
        response = requests.get(url, timeout=30)
        response_data = json.loads(response.text)
        status_code = response.status_code

        # Retry if rate limit is exceeded
        if status_code == 429:
            logging.info("Rate limit exceeded, retrying in %d seconds... (Attempt %d of %d)",
                         retry_delay, attempt + 1, max_retries)
            time.sleep(retry_delay)
        else:
            return response_data, status_code

    logging.error("Max retries reached. Returning the last response data.")
    return response_data, status_code
