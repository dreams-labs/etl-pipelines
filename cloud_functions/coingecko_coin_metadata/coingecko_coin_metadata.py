"""
This module is designed to retrieve and store metadata for cryptocurrency tokens from Coingecko.
It consists of three interconnected functions:

1. `retrieve_coingecko_metadata`: The main entry point that queries a list of tokens, attempts to
    fetch their metadata from Coingecko, and stores the results. It processes each token
    individually and manages rate-limiting between API calls.

2. `coingecko_metadata_search`: Called by `retrieve_coingecko_metadata`, this function attempts to
    look up a token's metadata on Coingecko, stores the metadata in Google Cloud Storage (GCS), and
    logs the results in BigQuery for future reference.

3. `fetch_coingecko_data`: A helper function that handles the actual API call to Coingecko,
    including retrying in the event of rate limits (HTTP 429). This function is invoked by
    `coingecko_metadata_search` to retrieve the metadata for each token.

The three functions work together to ensure that token metadata is fetched, stored, and logged
efficiently while respecting Coingecko's API rate limits and handling errors gracefully.
"""
import time
import datetime
import os
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
def retrieve_coingecko_metadata(request): # pylint: disable=unused-argument  # noqa: F841
    """
    Queries BigQuery to obtain a list of coins that need metadata and attempts to match them and
    store metadata by calling coingecko_metadata_search() for each.
    """

    # get GCP credentials
    credentials = dgc().credentials
    bigquery_client = bigquery.Client(credentials=credentials, project='dreams-labs-data')
    storage_client = storage.Client(credentials=credentials, project='dreams-labs-data')


    # pull list of coins to attempt
    query_sql = """
        select cc.coin_id
        ,ch.chain_text_coingecko
        ,cc.address
        from core.coins cc
        join core.chains ch on ch.chain_id = cc.chain_id
        left join etl_pipelines.coin_coingecko_ids search_exclusions on search_exclusions.coin_id = cc.coin_id
            and search_exclusions.search_log in (
                'search successful'
                ,"{'error': 'coin not found'}"
                ,"KeyError: ID not found in response data"
            )
        left join etl_pipelines.coin_coingecko_metadata cgm on cgm.coingecko_id = cc.coingecko_id
        -- don't include coins without addresses
        where cc.address is not null

        -- don't reattempt addresses that couldn't be found
        and search_exclusions.coin_id is null

        -- don't attempt coins that already have metadata
        and cgm.coingecko_id is null
        group by 1,2,3
        """

    update_queue_df = dgc().run_sql(query_sql)
    logger.info('coins to update: %s', str(update_queue_df.shape[0]))

    # ping api for each coin
    for i in range(len(update_queue_df)):
        blockchain = update_queue_df.iloc[i]['chain_text_coingecko']
        address = update_queue_df.iloc[i]['address']
        coin_id = update_queue_df.iloc[i]['coin_id']

        logger.info('initiating coingecko metadata search for <%s:%s>', blockchain, address)
        coingecko_metadata_search(
                blockchain
                ,address
                ,coin_id
                ,bigquery_client
                ,storage_client
            )

        # # rate limit pause
        # logger.info('pausing 15 seconds to avoid coingecko api rate limit issues...')
        # time.sleep(15)

    return "coingecko metadata update completed."



def coingecko_metadata_search(blockchain, address, coin_id, bigquery_client, storage_client):
    """
    For a given blockchain and address, attempts to look up the coin on Coingecko by calling
    fetch_coingecko_data(). If the search is successful, stores the metadata in GCS.

    param: blockchain <string> this must match chain_text_coingecko from core.chains
    param: address <string> token contract address
    param: coin_id <dataframe> core.coins.coin_id which is added to bigquery records
    param: bigquery_client <dataframe> authenticated client for inserting rows to BigQuery
    param: storage_client <dataframe> authenticated client for uploading to GCS
    """

    # making the api call
    response_data = fetch_coingecko_data(blockchain, address)

    try:
        coingecko_id = response_data['id']
        search_successful = True
        search_log = 'search successful'
        logger.info('search successful for <%s:%s>', blockchain, address)
    except KeyError:
        coingecko_id = None
        search_successful = False
        search_log = 'KeyError: ID not found in response data'
        logger.info('FAILURE: KeyError - search failed for <%s:%s>', blockchain, address)
    except (TypeError, AttributeError):
        coingecko_id = None
        search_successful = False
        search_log = 'TypeError or AttributeError: Invalid response data'
        logger.info('FAILURE: TypeError or AttributeError - search failed for <%s:%s>'
                    , blockchain, address)

    # storing json in gcs
    if search_successful:
        filepath = 'data_lake/coingecko_coin_metadata/'
        filename = str(response_data['id'] + '.json')

        bucket = storage_client.get_bucket('dreams-labs-storage')

        blob = bucket.blob(filepath + filename)
        blob.upload_from_string(json.dumps(response_data), content_type='json')

        logger.info('%s uploaded successfully', filename)

    # store search result in etl_pipelines.coin_coingecko_ids
    table_id = 'western-verve-411004.etl_pipelines.coin_coingecko_ids'

    rows_to_insert = [{
        'coin_id': coin_id,
        'coingecko_id': coingecko_id,
        'search_successful': search_successful,
        'search_date': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'search_log': search_log
    }]

    errors = bigquery_client.insert_rows_json(table_id, rows_to_insert)  # Make an API request.
    if not errors:
        logger.info("new row added to etl_pipelines.coin_coingecko_ids")
    else:
        logger.info("Encountered errors while inserting rows: %s", errors)



def fetch_coingecko_data(blockchain, address, max_retries=3, retry_delay=30):
    """
    Makes an API call to Coingecko and returns the response data.
    Retries the call if a rate limit error (429) is encountered.

    param: blockchain <string> this must match chain_text_coingecko from core.chains
    param: address <string> token contract address
    param: max_retries <int> number of times to retry on 429 error
    param: retry_delay <int> delay in seconds between retries
    returns: response_data <dict> JSON response data from Coingecko API
    """
    coingecko_api_key = os.getenv('COINGECKO_API_KEY')

    url = "https://pro-api.coingecko.com/api/v3/coins/id/contract/contract_address"

    headers = {
        "accept": "application/json",
        "x-cg-pro-api-key": coingecko_api_key
    }

    url = f'https://pro-api.coingecko.com/api/v3/coins/{blockchain}/contract/{address}'

    for attempt in range(max_retries):
        response = requests.get(url, headers=headers, timeout=30)
        response_data = json.loads(response.text)

        if 'status' in response_data and response_data['status'].get('error_code') == 429:
            logger.info("Rate limit exceeded, retrying in %d seconds... (Attempt %d of %d)",
                         retry_delay, attempt + 1, max_retries)
            time.sleep(retry_delay)
        else:
            return response_data

    logger.error("Max retries reached. Returning the last response data.")

    return response_data
