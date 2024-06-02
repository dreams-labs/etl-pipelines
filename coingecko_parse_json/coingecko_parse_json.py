import base64
import functions_framework
import pandas as pd
import time
import datetime
import math
import requests
import json
from google.cloud import bigquery
from google.auth import default
from google.cloud import storage

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


# clound functions wrapper
def cloud_function_wrapper(data,context):

    # pull list of all coins with json objects
    client = storage.Client()
    bucket = client.get_bucket('dreams-labs-storage')
    files = bucket.list_blobs(prefix='data_lake/coingecko_coin_metadata/')
    coins_with_json = []
    for file in files:
      if file.name == 'data_lake/coingecko_coin_metadata/':
          # the folder name shows up as a separate file so skip it
          pass
      else:
        coins_with_json = coins_with_json + [file.name.replace('data_lake/coingecko_coin_metadata/','').replace('.json','')]


    # pull list of coins already uploaded to bigquery
    query_sql = '''
        select coingecko_id
        from etl_pipelines.coin_coingecko_metadata md
        group by 1
        '''
    query_df = run_bigquery_sql(query_sql)
    coins_in_table = list(query_df['coingecko_id'])
    coins_to_process = [coin for coin in coins_with_json if coin not in coins_in_table]
    print('coins to process: '+str(len(coins_to_process)))


    # process each json object
    for coin in coins_to_process:

        # retrieve json blob
        file_name = 'data_lake/coingecko_coin_metadata/'+coin+'.json'
        blob = bucket.blob(file_name)
        blob_contents = blob.download_as_string()
        json_data = json.loads(blob_contents)
        print('processing data for '+json_data['id']+'...')


        # prepare to upload data from the json blob
        client = bigquery.Client()
        updated_at = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')


        # metadata extraction and upload
        coingecko_id                      = json_data['id']
        symbol                            = json_data['symbol']
        name                              = json_data['name']
        homepage                          = json_data['links']['homepage'][0]
        twitter_screen_name               = json_data['links']['twitter_screen_name']
        telegram_channel_identifier       = json_data['links']['telegram_channel_identifier']
        image_urls                        = str(json_data['image'])
        sentiment_votes_up_percentage     = json_data['sentiment_votes_up_percentage']
        sentiment_votes_down_percentage   = json_data['sentiment_votes_down_percentage']
        watchlist_portfolio_users         = json_data['watchlist_portfolio_users']
        total_supply                      = json_data['market_data']['total_supply']
        max_supply                        = json_data['market_data']['max_supply']
        circulating_supply                = json_data['market_data']['circulating_supply']

        table_id = 'western-verve-411004.etl_pipelines.coin_coingecko_metadata'
        rows_to_insert = [{
            'coingecko_id': coingecko_id
            ,'symbol': symbol
            ,'name': name
            ,'homepage': homepage
            ,'twitter_screen_name': twitter_screen_name
            ,'telegram_channel_identifier': telegram_channel_identifier
            ,'image_urls': image_urls
            ,'sentiment_votes_up_percentage': sentiment_votes_up_percentage
            ,'sentiment_votes_down_percentage': sentiment_votes_down_percentage
            ,'watchlist_portfolio_users': watchlist_portfolio_users
            ,'total_supply': total_supply
            ,'max_supply': max_supply
            ,'circulating_supply': circulating_supply
            ,'updated_at': updated_at
        }]

        errors = client.insert_rows_json(table_id, rows_to_insert)  # Make an API request.
        success_count = 0
        failure_count = 0
        if errors == []:
            success_count+=1
        else:
            print("Encountered errors while inserting rows: {}".format(errors))
            failure_count+=1

        print(str(success_count)+' rows inserted into '+table_id)
        if failure_count > 0:
            print(str(success_count)+' rows failed to insert into '+table_id)



        # categories extraction and upload
        categories = json_data['categories']
        table_id = 'western-verve-411004.etl_pipelines.coin_coingecko_categories'
        success_count = 0
        failure_count = 0

        for i in range(len(categories)):

            rows_to_insert = [{
            'coingecko_id': coingecko_id
            ,'category': categories[i]
            ,'coingecko_rank': i+1
            ,'updated_at': updated_at
            }]
            errors = client.insert_rows_json(table_id, rows_to_insert)  # Make an API request.
            if errors == []:
                    success_count+=1
            else:
                print("Encountered errors while inserting rows: {}".format(errors))
                failure_count+=1

        print(str(success_count)+' rows inserted into '+table_id)
        if failure_count > 0:
            print(str(success_count)+' rows failed to insert into '+table_id)



        # contracts extraction and upload
        contracts = json_data['detail_platforms']
        blockchains = list(contracts.keys())
        table_id = 'western-verve-411004.etl_pipelines.coin_coingecko_contracts'
        success_count = 0
        failure_count = 0

        for i in range(len(blockchains)):

            coingecko_id = coingecko_id
            blockchain = blockchains[i]
            address = json_data['detail_platforms'][blockchains[i]]['contract_address']
            decimals = json_data['detail_platforms'][blockchains[i]]['decimal_place']
            coingecko_rank = i+1

            rows_to_insert = [{
                'coingecko_id': coingecko_id
                ,'blockchain': blockchain
                ,'address': address
                ,'decimals': decimals
                ,'coingecko_rank': coingecko_rank
                ,'updated_at': updated_at
            }]
            errors = client.insert_rows_json(table_id, rows_to_insert)  # Make an API request.
            if errors == []:
                    success_count+=1
            else:
                print("Encountered errors while inserting rows: {}".format(errors))
                failure_count+=1

        print(str(success_count)+' rows inserted into '+table_id)
        if failure_count > 0:
            print(str(success_count)+' rows failed to insert into '+table_id)



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