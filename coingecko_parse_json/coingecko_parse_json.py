import base64
import functions_framework
import pandas as pd
import time
import datetime
import logging
import math
import requests
import json
from google.cloud import bigquery
from google.auth import default
from google.cloud import storage
import dreams_core.core as dc
from dreams_core.googlecloud import GoogleCloud as dgc


def identify_coins_to_process():
    """
    identifies the json files that need to be processed by comparing a list of all json files \
         in cloud storage with the records in etl_pipelines.coin_coingecko_metadata. json files \
        that have not been uploaded to the bigquery table need to be processed. 

    return: coins_to_process <array> list of coins with valid json files that have not been \
        added to etl_pipelines.coin_coingecko_metadata
    """
    dc.setup_logger()

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
    query_df = dgc().run_sql(query_sql)
    coins_in_table = list(query_df['coingecko_id'])
    coins_to_process = [coin for coin in coins_with_json if coin not in coins_in_table]
    logger.info('metadata json blobs to process: %s', str(len(coins_to_process)))

    return coins_to_process



def parse_coingecko_json(data, context):
    """
    Main function to parse and process the coingecko json data.
    """
    # Configure logger
    dc.setup_logger()

    # identify jsons that haven't yet been processed
    coins_to_process = identify_coins_to_process()

    # extract the json data and upload it to the corresponding bigquery tables
    for coin in coins_to_process:
        json_data = fetch_coin_json(coin)
        upload_metadata(json_data)
        upload_categories(json_data)
        upload_contracts(json_data)


def fetch_coin_json(coin):
    """
    Retrieves the JSON blob for a coin from Google Cloud Storage.
    """
    client = storage.Client()
    bucket = client.get_bucket('dreams-labs-storage')
    file_name = f'data_lake/coingecko_coin_metadata/{coin}.json'
    blob = bucket.blob(file_name)
    blob_contents = blob.download_as_string()
    return json.loads(blob_contents)

def upload_metadata(json_data):
    """
    Extracts and uploads metadata to BigQuery.
    """
    client = bigquery.Client()
    table_id = 'western-verve-411004.etl_pipelines.coin_coingecko_metadata'
    updated_at = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    rows_to_insert = [{
        'coingecko_id': json_data['id'],
        'symbol': json_data['symbol'],
        'name': json_data['name'],
        'homepage': json_data['links']['homepage'][0],
        'twitter_screen_name': json_data['links']['twitter_screen_name'],
        'telegram_channel_identifier': json_data['links']['telegram_channel_identifier'],
        'image_urls': str(json_data['image']),
        'sentiment_votes_up_percentage': json_data['sentiment_votes_up_percentage'],
        'sentiment_votes_down_percentage': json_data['sentiment_votes_down_percentage'],
        'watchlist_portfolio_users': json_data['watchlist_portfolio_users'],
        'total_supply': json_data['market_data']['total_supply'],
        'max_supply': json_data['market_data']['max_supply'],
        'circulating_supply': json_data['market_data']['circulating_supply'],
        'updated_at': updated_at
    }]

    insert_rows(client, table_id, rows_to_insert)


def upload_categories(json_data):
    """
    Extracts and uploads categories to BigQuery.
    """
    client = bigquery.Client()
    table_id = 'western-verve-411004.etl_pipelines.coin_coingecko_categories'
    updated_at = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    categories = json_data['categories']
    rows_to_insert = [
        {'coingecko_id': json_data['id'], 'category': category, 'coingecko_rank': i+1, 'updated_at': updated_at}
        for i, category in enumerate(categories)
    ]

    insert_rows(client, table_id, rows_to_insert)


def upload_contracts(json_data):
    """
    Extracts and uploads contracts to BigQuery.
    """
    client = bigquery.Client()
    table_id = 'western-verve-411004.etl_pipelines.coin_coingecko_contracts'
    updated_at = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    contracts = json_data['detail_platforms']
    rows_to_insert = [
        {
            'coingecko_id': json_data['id'],
            'blockchain': blockchain,
            'address': contracts[blockchain]['contract_address'],
            'decimals': contracts[blockchain]['decimal_place'],
            'coingecko_rank': i+1,
            'updated_at': updated_at
        }
        for i, blockchain in enumerate(contracts.keys())
    ]

    insert_rows(client, table_id, rows_to_insert)


def insert_rows(client, table_id, rows_to_insert):
    """
    Inserts rows into BigQuery and logs the outcome.
    """
    errors = client.insert_rows_json(table_id, rows_to_insert)
    success_count = len(rows_to_insert) - len(errors)
    failure_count = len(errors)

    logger.info("%s rows inserted into %s", success_count, table_id)
    if failure_count > 0:
        logger.info("%s rows failed to insert into %s: %s", failure_count, table_id, errors)
