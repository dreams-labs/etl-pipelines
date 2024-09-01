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


def coingecko_metadata_search(
        blockchain
        ,address
        ,coin_id
    ):
    '''
    attempts to look up a coin on coingecko and store its metadata in gcs

    param: coingecko_api_key <string>
    param: blockchain <string> this must match chain_text_coingecko from core.chains
    param: address <string> token contract address
    param: coin_id <dataframe> core.coins.coin_id which is added to bigquery records
    '''
    # get logger
    logger = logging.getLogger(__name__)

    # making the api call
    coingecko_api_key = os.getenv('COINGECKO_API_KEY')

    url = f'https://api.coingecko.com/api/v3/coins/{blockchain}/contract/{address}&x_cg_demo_api_key={coingecko_api_key}'
    response = requests.get(url, timeout=30)
    response_data = json.loads(response.text)

    try:
        coingecko_id = response_data['id']
        search_successful = True
        search_log = 'search successful'
        logger.info('search successful for <%s:%s>', blockchain, address)
    except:
        coingecko_id = None
        search_successful = False
        search_log = str(response_data)
        logger.info('FAILUIRE: search failed for <%s:%s>', blockchain, address)
        logger.info('%s',str(response_data))

    # storing json in gcs
    if search_successful:
        filepath = 'data_lake/coingecko_coin_metadata/'
        filename = str(response_data['id']+'.json')

        client = storage.Client(project='dreams-labs-data')
        bucket = client.get_bucket('dreams-labs-storage')

        blob = bucket.blob(filepath + filename)
        blob.upload_from_string(json.dumps(response_data),content_type = 'json')

        logger.info('%s uploaded successfully', filename)


    # store search result in etl_pipelines.coin_coingecko_ids
    client = bigquery.Client()
    table_id = 'western-verve-411004.etl_pipelines.coin_coingecko_ids'

    rows_to_insert = [{
        'coin_id': coin_id
        ,'coingecko_id': coingecko_id
        ,'search_successful': search_successful
        ,'search_date': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ,'search_log': search_log
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
        ,cc.symbol
        ,ch.chain_text_coingecko
        ,cc.address
        from core.coins cc
        left join core.chains ch on ch.chain_id = cc.chain_id
        left join etl_pipelines.coin_coingecko_ids cgi on cgi.coin_id = cc.coin_id
        where cc.address is not null -- removes coins without addresses
        and cgi.coin_id is null -- removes previously attempted coins (this could be modified to retry some older matches)
        '''

    update_queue_df = dgc().run_sql(query_sql)
    logger.info('coins to update: %s', str(update_queue_df.shape[0]))

    # ping api for each coin
    for i in range(len(update_queue_df)):
        blockchain = update_queue_df.iloc[i]['chain_text_coingecko']
        address = update_queue_df.iloc[i]['address']
        coin_id = update_queue_df.iloc[i]['coin_id']
        symbol = update_queue_df.iloc[i]['symbol']

        logger.info('initiating coingecko metadata search for %s...', symbol)
        coingecko_metadata_search(
                blockchain
                ,address
                ,coin_id
            )

        # rate limit pause
        logger.info('pausing 10 seconds to avoid coingecko api rate limit issues...')
        time.sleep(10)
