import functions_framework
import pandas as pd
import time
import datetime
import logging
import os
import requests
import json
from google.cloud import bigquery
from google.auth import default
from google.cloud import storage
from dreams_core.googlecloud import GoogleCloud as dgc


def fetch_coingecko_data(blockchain, address, max_retries=3, retry_delay=30):
    '''
    Makes an API call to Coingecko and returns the response data.
    Retries the call if a rate limit error (429) is encountered.

    param: blockchain <string> this must match chain_text_coingecko from core.chains
    param: address <string> token contract address
    param: max_retries <int> number of times to retry on 429 error
    param: retry_delay <int> delay in seconds between retries
    returns: response_data <dict> JSON response data from Coingecko API
    '''
    coingecko_api_key = os.getenv('COINGECKO_API_KEY')
    headers = {'x_cg_pro_api_key': coingecko_api_key}
    url = f'https://api.coingecko.com/api/v3/coins/{blockchain}/contract/{address}'

    for attempt in range(max_retries):
        response = requests.get(url, headers=headers, timeout=30)
        response_data = json.loads(response.text)

        if 'status' in response_data and response_data['status'].get('error_code') == 429:
            logging.info("Rate limit exceeded, retrying in %d seconds... (Attempt %d of %d)", retry_delay, attempt + 1, max_retries)
            time.sleep(retry_delay)
        else:
            return response_data

    logging.error("Max retries reached. Returning the last response data.")

    return response_data


def coingecko_metadata_search(blockchain, address, coin_id):
    '''
    Attempts to look up a coin on Coingecko and store its metadata in GCS.

    param: blockchain <string> this must match chain_text_coingecko from core.chains
    param: address <string> token contract address
    param: coin_id <dataframe> core.coins.coin_id which is added to bigquery records
    '''
    # get logger
    logger = logging.getLogger(__name__)

    # making the api call
    response_data = fetch_coingecko_data(blockchain, address)

    try:
        coingecko_id = response_data['id']
        search_successful = True
        search_log = 'search successful'
        logger.info('search successful for <%s:%s>', blockchain, address)
    except:
        coingecko_id = None
        search_successful = False
        search_log = str(response_data)
        logger.info('FAILURE: search failed for <%s:%s>', blockchain, address)
        logger.info('%s', str(response_data))

    # storing json in gcs
    if search_successful:
        filepath = 'data_lake/coingecko_coin_metadata/'
        filename = str(response_data['id'] + '.json')

        client = storage.Client(project='dreams-labs-data')
        bucket = client.get_bucket('dreams-labs-storage')

        blob = bucket.blob(filepath + filename)
        blob.upload_from_string(json.dumps(response_data), content_type='json')

        logger.info('%s uploaded successfully', filename)

    # store search result in etl_pipelines.coin_coingecko_ids
    client = bigquery.Client()
    table_id = 'western-verve-411004.etl_pipelines.coin_coingecko_ids'

    rows_to_insert = [{
        'coin_id': coin_id,
        'coingecko_id': coingecko_id,
        'search_successful': search_successful,
        'search_date': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'search_log': search_log
    }]

    errors = client.insert_rows_json(table_id, rows_to_insert)  # Make an API request.
    if errors == []:
        logger.info("new row added to etl_pipelines.coin_coingecko_ids")
    else:
        logger.info("Encountered errors while inserting rows: {}".format(errors))


# clound functions wrapper
def retrieve_coingecko_metadata(request):
    '''
    pulls a list of coins that need metadata and attempts to match them and store metadata
    '''
    # configure logger
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s',
        datefmt='%d/%b/%Y %H:%M:%S'
        )
    logger = logging.getLogger(__name__)

    # pull list of coins to attempt
    query_sql = '''
        select cc.coin_id
        ,ch.chain_text_coingecko
        ,cc.address
        from core.coins cc
        left join core.chains ch on ch.chain_id = cc.chain_id
        left join etl_pipelines.coin_coingecko_ids cgi on cgi.coin_id = cc.coin_id
            and cgi.search_log = "{'error': 'coin not found'}" -- removes coins that could not be found while reattempting api timeouts
        where cc.address is not null -- removes coins without addresses
        limit 5
        '''

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
            )

        # rate limit pause
        logger.info('pausing `15` seconds to avoid coingecko api rate limit issues...')
        time.sleep(15)
