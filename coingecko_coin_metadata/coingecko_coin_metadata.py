import base64
import functions_framework
import pandas as pd
import time
import datetime
import math
import os
import requests
import json
from google.cloud import bigquery
from google.auth import default
from google.cloud import storage

# core python begins here
def run_bigquery_sql(
        query_sql
        ,location='US'
        ,project = 'western-verve-411004'
    ):
    '''
    returns the blockchain and contract address of a coin on coingecko

    param: query_sql <string> the query to run
    param: location <string> the location of the bigquery project
    param: project <string> the project ID of the bigquery project
    return: query_df <dataframe> the query result
    '''

    # Create a BigQuery client object.
    client = bigquery.Client(project=project,location=location)

    query_job = client.query(query_sql)
    query_df = query_job.to_dataframe()

    return(query_df)

def coingecko_metadata_search(
        coingecko_api_key
        ,blockchain
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

    # making the api call
    headers = {'x_cg_pro_api_key': coingecko_api_key}
    url = 'https://api.coingecko.com/api/v3/coins/'+blockchain+'/contract/'+address
    response = requests.request("GET", url, headers=headers)
    response_data = json.loads(response.text)

    try:
        coingecko_id = response_data['id']
        search_successful = True
        search_log = 'search successful'
        print('search successful for '+blockchain+address)
    except:
        coingecko_id = None
        search_successful = False
        search_log = str(response_data)
        print('FAILURE: search failed for '+blockchain+address)
        print(response_data)

    # rate limit pause
    print('~~~ rate limit pause :) ~~~')
    time.sleep(15)


    # storing json in gcs
    if search_successful:
        filepath = 'data_lake/coingecko_coin_metadata/'
        filename = str(response_data['id']+'.json')

        client = storage.Client(project='dreams-labs-data')
        bucket = client.get_bucket('dreams-labs-storage')

        blob = bucket.blob(filepath + filename)
        blob.upload_from_string(json.dumps(response_data),content_type = 'json')
        print(filename+' uploaded successfully')


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
        print("new row added to etl_pipelines.coin_coingecko_ids")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))



# clound functions wrapper
def cloud_function_wrapper(data,context):

    print('hello')
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
    ### troubleshooting connection errors 
    # location='US'
    # project = 'western-verve-411004'
    # # Create a BigQuery client object.
    # client = bigquery.Client(project=project,location=location)

    # query_job = client.query(query_sql)
    # query_df = query_job.to_dataframe()

    update_queue_df = run_bigquery_sql(query_sql)
    print('coins to update: '+str(update_queue_df.shape[0]))

    # api key pulled from exposed secret
    coingecko_api_key = os.environ.get('coingecko_api_key')
    print('api key retrieved.')

    # ping api for each coin
    for i in range(len(update_queue_df)):
        blockchain = update_queue_df.iloc[i]['chain_text_coingecko']
        address = update_queue_df.iloc[i]['address']
        coin_id = update_queue_df.iloc[i]['coin_id']
        symbol = update_queue_df.iloc[i]['symbol']

        print('processing '+symbol+'...')
        coingecko_metadata_search(
                coingecko_api_key
                ,blockchain
                ,address
                ,coin_id
            )


# cloud functions execution
# setting these allows both test and production functioning
try:
    data
except: 
    data = 'test_data'
try:
    context
except: 
    context = 'test_context'


cloud_function_wrapper(data, context)