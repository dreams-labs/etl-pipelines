"""
This module parses and processes CoinGecko data stored in Google Cloud Storage. It identifies
unprocessed coins, fetches their JSON data, and uploads metadata, categories, and contracts to
BigQuery. Functions interact by retrieving data, processing it, and uploading in sequence.
See individual functions for details on each processing step.
"""
import datetime
import json
import functions_framework
from google.cloud import bigquery
from google.cloud import storage
import dreams_core.core as dc
from dreams_core.googlecloud import GoogleCloud as dgc

# set up logger at the module level
logger = dc.setup_logger()


@functions_framework.http
def parse_coingecko_json(request):  # pylint: disable=unused-argument  # noqa: F841
    """
    Main function to parse and process the CoinGecko JSON data.

    This function serves as an HTTP endpoint to trigger the parsing and processing
    of CoinGecko data stored in Google Cloud Storage. It identifies unprocessed
    JSON files, fetches the data for each coin, and uploads various components
    (metadata, categories, and contracts) to corresponding BigQuery tables.

    Workflow:
    ---------
    1. Identify Coins to Process:
       - Calls the `identify_coins_to_process()` function to compare existing JSON
         files in Google Cloud Storage with records in the BigQuery table
         `etl_pipelines.coin_coingecko_metadata`.
       - Identifies coins whose data has not yet been uploaded to BigQuery.

    2. Fetch and Process JSON Data:
       - For each identified coin, the function retrieves the corresponding JSON
         data using `fetch_coin_json(coin)`.

    3. Upload Data to BigQuery:
       - The function processes and uploads the data in three parts:
         a. Metadata: Using `upload_metadata(json_data)` to extract and upload
            general coin information.
         b. Categories: Using `upload_categories(json_data)` to extract and
            upload category information associated with the coin.
         c. Contracts: Using `upload_contracts(json_data)` to extract and upload
            blockchain contract details.
    """

    # Initialize clients at the top
    storage_client = storage.Client()
    bigquery_client = bigquery.Client()

    # identify jsons that haven't yet been processed
    coins_to_process = identify_coins_to_process(storage_client)

    # extract the json data and upload it to the corresponding bigquery tables
    for coin in coins_to_process:
        json_data = fetch_coin_json(coin, storage_client)
        upload_metadata(json_data, bigquery_client)
        upload_categories(json_data, bigquery_client)
        upload_contracts(json_data, bigquery_client)


    return f"coingecko json parsing complete. processed {len(coins_to_process)} coins."


def identify_coins_to_process(storage_client):
    """
    identifies the json files that need to be processed by comparing a list of all json files \
         in cloud storage with the records in etl_pipelines.coin_coingecko_metadata. json files \
        that have not been uploaded to the bigquery table need to be processed.

    return: coins_to_process <array> list of coins with valid json files that have not been \
        added to etl_pipelines.coin_coingecko_metadata
    """
    # pull list of all coins with json objects
    bucket = storage_client.get_bucket('dreams-labs-storage')
    files = bucket.list_blobs(prefix='data_lake/coingecko_coin_metadata/')
    coins_with_json = []
    for file in files:
        if file.name == 'data_lake/coingecko_coin_metadata/':
            # the folder name shows up as a separate file so skip it
            pass
        else:
            coins_with_json = coins_with_json + [
                file.name.replace('data_lake/coingecko_coin_metadata/','').replace('.json','')
            ]

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


def fetch_coin_json(coin,storage_client):
    """
    Retrieves the JSON blob for a coin from Google Cloud Storage.
    """
    bucket = storage_client.get_bucket('dreams-labs-storage')
    file_name = f'data_lake/coingecko_coin_metadata/{coin}.json'
    blob = bucket.blob(file_name)
    blob_contents = blob.download_as_string()

    return json.loads(blob_contents)


def upload_metadata(json_data, bigquery_client):
    """
    Extracts and uploads metadata to BigQuery, including description.
    """
    table_id = 'western-verve-411004.etl_pipelines.coin_coingecko_metadata'
    updated_at = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Safely fetch description with a fallback to None (which will be NULL in BigQuery)
    description = json_data.get('description', {}).get('en', None)

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
        'description': description,  # Using None as fallback for a NULL value in BigQuery
        'updated_at': updated_at
    }]

    insert_rows(bigquery_client, table_id, rows_to_insert)


def upload_categories(json_data,bigquery_client):
    """
    Extracts and uploads categories to BigQuery.
    """
    table_id = 'western-verve-411004.etl_pipelines.coin_coingecko_categories'
    updated_at = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    categories = json_data['categories']
    rows_to_insert = [
        {
            'coingecko_id': json_data['id'],
            'category': category,
            'coingecko_rank': i+1,
            'updated_at': updated_at
        }
        for i, category in enumerate(categories)
    ]

    insert_rows(bigquery_client, table_id, rows_to_insert)


def upload_contracts(json_data,bigquery_client):
    """
    Extracts and uploads contracts to BigQuery.
    """
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

    insert_rows(bigquery_client, table_id, rows_to_insert)


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
