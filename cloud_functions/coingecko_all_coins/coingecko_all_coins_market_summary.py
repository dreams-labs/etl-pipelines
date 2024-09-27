
"""
Retrieves market summary for all coins on coingecko
"""
import time
import datetime
from typing import List, Dict
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
def retrieve_coingecko_all_coins_market_summary(request): # pylint: disable=unused-argument  # noqa: F841
    '''
    Retrieves and n pages of the all coins market summary endpoint

    Paraams:
        all_coins_pages (int): how many pages of records to return
    '''

    # retrieve the number of pages to upload and store the batch time
    pages = request.args.get('all_coins_pages', default=1, type=int)
    batch_datetime = f"{datetime.datetime.now().strftime('%Y_%m_%d__%H_%M_%S')}"

    # get GCP credentials and initiate clients
    credentials = dgc().credentials
    bigquery_client = bigquery.Client()
    storage_client = storage.Client(credentials=credentials, project='dreams-labs-data')
    bucket = storage_client.get_bucket('dreams-labs-storage')
    filepath = 'data_lake/coingecko_all_coins_market_summary/'

    # ping api for each coin, adjusting the range so it starts at 1
    for page in range(1, pages + 1):
        logger.info('Retrieving page %s of coingecko all coins list...', page)

        # making the api call
        response_data = fetch_coingecko_data(page)

        if not response_data:
            logger.info('No records found after page %s, terminating retrieval process.', page-1)
            break

        # store the data in gcs if the response was correctly formed
        if 'id' in response_data[0]:
            # set filename
            filename = f'all_coins_{batch_datetime}_page_{page}'

            # storing json in gcs
            blob = bucket.blob(filepath + filename)
            blob.upload_from_string(json.dumps(response_data), content_type='json')
            logger.info('%s uploaded successfully to GCS.', filename)

            # upload the data to bigquery
            upload_coingecko_data_to_bigquery(response_data, batch_datetime, bigquery_client)
            logger.info('%s uploaded successfully to BigQuery.', filename)


        # if an empty array is returned, that means we don't have any more pages with data

        # rate limit pause
        logger.debug('pausing 15 seconds to avoid coingecko api rate limit issues...')
        time.sleep(15)

    return "coingecko metadata update completed."



def upload_coingecko_data_to_bigquery(
        json_data: List[Dict], batch_datetime, bigquery_client
    ) -> None:
    """
    Extracts relevant fields from a list of coin data in JSON format and uploads them to BigQuery.

    Args:
        json_data (List[Dict]): List of dictionaries, each representing data for a single coin.
        batch_datetime <string> a string representation of when this batch of ids was retrieved
        bigquery_client <Client> authenticated bigquery client

    Returns:
        None: Prints the status of the upload (success or error).
    """

    # define the created_at var with the correct formatting
    parsed_date = datetime.datetime.strptime(batch_datetime, '%Y_%m_%d__%H_%M_%S')
    created_at = parsed_date.strftime('%Y-%m-%d %H:%M:%S')


    # Prepare rows for BigQuery
    rows_to_insert = []
    for coin in json_data:
        row = {
            "id": coin["id"],
            "symbol": coin["symbol"],
            "name": coin["name"],
            "current_price": coin.get("current_price"),
            "market_cap": coin.get("market_cap"),
            "market_cap_rank": coin.get("market_cap_rank"),
            "fully_diluted_valuation": coin.get("fully_diluted_valuation"),
            "total_volume": coin.get("total_volume"),
            "ath_date": coin.get("ath_date").replace("Z", "") if coin.get("ath_date") else None,
            "atl_date": coin.get("atl_date").replace("Z", "") if coin.get("atl_date") else None,
            "last_updated_coingecko": coin.get("last_updated").replace("Z", "") if coin.get("last_updated") else None,  # pylint: disable=C0301
            "created_at": created_at
        }
        rows_to_insert.append(row)

    # Insert rows into BigQuery table
    table_id = 'western-verve-411004.etl_pipelines.coingecko_all_coins_market_summary'
    errors = bigquery_client.insert_rows_json(table_id, rows_to_insert)

    if not errors:
        logger.debug("Rows successfully inserted.")
    else:
        logger.warning("Encountered errors: %s", errors)



def fetch_coingecko_data(page, max_retries=3, retry_delay=30):
    '''
    Makes an API call to Coingecko and returns the response data.
    Retries the call if a rate limit error (429) is encountered.

    param: page <int> the page number of the all coins list to retrieve

    param: max_retries <int> number of times to retry on 429 error
    param: retry_delay <int> delay in seconds between retries

    returns: response_data <dict> JSON response data from Coingecko API
    '''
    coingecko_api_key = os.getenv('COINGECKO_API_KEY')
    headers = {'x_cg_pro_api_key': coingecko_api_key}
    url = f'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&per_page=250&page={page}'

    for attempt in range(max_retries):
        response = requests.get(url, headers=headers, timeout=30)
        response_data = json.loads(response.text)

        if 'status' in response_data and response_data['status'].get('error_code') == 429:
            logger.info("Rate limit exceeded, retrying in %d seconds... (Attempt %d of %d)",
                            retry_delay, attempt + 1, max_retries)
            time.sleep(retry_delay)
        else:
            return response_data

