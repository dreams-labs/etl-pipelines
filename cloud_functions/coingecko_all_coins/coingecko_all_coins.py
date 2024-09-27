"""
Retrieves all coins from coingecko
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
def retrieve_coingecko_all_coins(request): # pylint: disable=unused-argument  # noqa: F841
    '''
    Retrieves and uploads a page from the all coins endpoint for the given page range
    '''

    # retrieve the number of pages to upload and store the batch time
    pages = request.args.get('all_coins_pages', default=1, type=int)
    batch_datetime = f"{datetime.datetime.now().strftime('%Y_%m_%d__%H_%M_%S')}"

    # ping api for each coin, adjusting the range so it starts at 1
    for page in range(1, pages + 1):
        logger.debug('Retrieving page %s of coingecko all coins list...', page)

        # retrieve the metadata
        coingecko_get_all_coins_page(page,batch_datetime)

        # rate limit pause
        logger.info('pausing 15 seconds to avoid coingecko api rate limit issues...')
        time.sleep(15)

    return "coingecko metadata update completed."



def coingecko_get_all_coins_page(page,batch_datetime):
    '''
    Retrieves a page of 250 coins from the coingecko all coins endpoint

    param: page <int> the page number of the all coins list to retrieve
    param: batch_datetime <string> a string representation of when this batch of ids was retrieved
    '''
    # get GCP credentials
    credentials = dgc().credentials

    # making the api call
    response_data = fetch_coingecko_data(page)

    # store the data in gcs if the response was correctly formed
    if 'id' in response_data[0]:
        # storing json in gcs
        filepath = 'data_lake/coingecko_all_coins/'
        filename = f'all_coins_{batch_datetime}_page_{page}'

        client = storage.Client(credentials=credentials, project='dreams-labs-data')
        bucket = client.get_bucket('dreams-labs-storage')

        blob = bucket.blob(filepath + filename)
        blob.upload_from_string(json.dumps(response_data), content_type='json')

        logger.debug('%s uploaded successfully', filename)

        # upload the data to bigquery
        upload_coingecko_data_to_bigquery(response_data, batch_datetime)




def upload_coingecko_data_to_bigquery(json_data: List[Dict], batch_datetime) -> None:
    """
    Extracts relevant fields from a list of coin data in JSON format and uploads them to BigQuery.

    Args:
        json_data (List[Dict]): List of dictionaries, each representing data for a single coin.
        batch_datetime <string> a string representation of when this batch of ids was retrieved

    Returns:
        None: Prints the status of the upload (success or error).
    """
    # Initialize BigQuery client
    client = bigquery.Client()
    table_id = 'western-verve-411004.etl_pipelines.coingecko_all_coins'

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
    errors = client.insert_rows_json(table_id, rows_to_insert)

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

    logger.error("Max retries reached. Returning the last response data.")

    return response_data
