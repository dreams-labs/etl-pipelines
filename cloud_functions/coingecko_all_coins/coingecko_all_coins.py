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
def retrieve_coingecko_all_coins(request):  # pylint: disable=unused-argument  # noqa: F841
    '''
    Retrieves all coins from the Coingecko API, stores the data in GCS, and uploads it to BigQuery.
    '''
    # Get GCP credentials
    credentials = dgc().credentials

    # Retrieve list of all coins
    response_data = fetch_coingecko_data()

    # Store the data if the response was correctly formed
    if 'id' in response_data[0]:
        # Store JSON in GCS
        batch_datetime = f"{datetime.datetime.now().strftime('%Y_%m_%d__%H_%M_%S')}"
        filepath = 'data_lake/coingecko_all_coins/'
        filename = f'all_coins_{batch_datetime}'

        client = storage.Client(credentials=credentials, project='dreams-labs-data')
        bucket = client.get_bucket('dreams-labs-storage')

        blob = bucket.blob(filepath + filename)
        blob.upload_from_string(json.dumps(response_data), content_type='application/json')
        logger.info('%s uploaded successfully to GCS.', filename)

        # Upload the data to BigQuery
        upload_coingecko_data_to_bigquery(response_data, batch_datetime)
        logger.info('%s uploaded successfully to BigQuery.', filename)

    return "Coingecko all coins update completed."


def upload_coingecko_data_to_bigquery(json_data: List[Dict], batch_datetime: str) -> None:
    """
    Extracts relevant fields from a list of coin data in JSON format and uploads them to BigQuery.

    Args:
        json_data (List[Dict]): List of dictionaries, each representing data for a single coin.
        batch_datetime (str): A string representation of when this batch of coins was retrieved.

    Returns:
        None: Logs the status of the upload (success or error).
    """
    # Initialize BigQuery client
    client = bigquery.Client()
    table_id = 'western-verve-411004.etl_pipelines.coingecko_all_coins'

    # Parse and format the created_at field
    parsed_date = datetime.datetime.strptime(batch_datetime, '%Y_%m_%d__%H_%M_%S')
    created_at = parsed_date.strftime('%Y-%m-%d %H:%M:%S')

    # Prepare rows for BigQuery
    rows_to_insert = []
    for coin in json_data:
        # Extract blockchain-contract address pairs
        contract_addresses = []
        platforms = coin.get('platforms', {})

        for blockchain, contract_address in platforms.items():
            contract_addresses.append({
                'blockchain': blockchain,
                'contract_address': contract_address
            })

        # Build row to upload
        row = {
            "id": coin["id"],
            "symbol": coin["symbol"],
            "name": coin["name"],
            "contract_addresses": contract_addresses,
            "created_at": created_at
        }
        rows_to_insert.append(row)

    # Insert rows into BigQuery table
    errors = client.insert_rows_json(table_id, rows_to_insert)

    if not errors:
        logger.debug("Rows successfully inserted.")
    else:
        logger.warning("Encountered errors: %s", errors)


def fetch_coingecko_data(max_retries=3, retry_delay=30) -> Dict:
    '''
    Makes an API call to Coingecko and returns the response data.
    Retries the call if a rate limit error (429) is encountered.

    Args:
        max_retries (int): Number of times to retry on a 429 error. Default is 3.
        retry_delay (int): Delay in seconds between retries. Default is 30.

    Returns:
        response_data (dict): JSON response data from Coingecko API.
    '''
    coingecko_api_key = os.getenv('COINGECKO_API_KEY')
    headers = {'x_cg_pro_api_key': coingecko_api_key}
    url = "https://api.coingecko.com/api/v3/coins/list?include_platform=true&status=active"

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
    