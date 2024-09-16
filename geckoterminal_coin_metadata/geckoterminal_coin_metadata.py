import time
import datetime
import logging
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


def fetch_geckoterminal_data(blockchain, address, max_retries=3, retry_delay=30):
    '''
    Makes an API call to Geckoterminal and returns the response data.
    Retries the call if a rate limit error (429) is encountered.

    param: blockchain <string> this must match chain_text_coingecko from core.chains
    param: address <string> token contract address
    param: max_retries <int> number of times to retry on 429 error
    param: retry_delay <int> delay in seconds between retries
    returns: response_data <dict> JSON response data from Geckoterminal API
    '''
    url = f'https://api.geckoterminal.com/api/v2/networks/{blockchain}/tokens/{address}'

    for attempt in range(max_retries):
        response = requests.get(url, timeout=30)
        response_data = json.loads(response.text)

        # Retry if rate limit is exceeded
        if 'errors' in response_data and response_data['errors'][0].get('status') == 429:
            logging.info("Rate limit exceeded, retrying in %d seconds... (Attempt %d of %d)",
                         retry_delay, attempt + 1, max_retries)
            time.sleep(retry_delay)
        else:
            return response_data

    logging.error("Max retries reached. Returning the last response data.")
    return response_data






# # from whale chart
# def geckoterminal_metadata_search(
#         blockchain
#         ,address
#         ,verbose=False
#     ):
#     '''
#     attempts to look up a coin on geckoterminal (no api key required)

#     param: blockchain <string> this must match chain_text_coingecko from core.chains
#     param: address <string> token contract address
#     return: api_status_code <int> geckoterminal api status code
#     return: token_dict <dict> a dictionary containing standardized token fields
#     '''
#     token_dict = {}

#     # making the api call
#     url = 'https://api.geckoterminal.com/api/v2/networks/'+blockchain+'/tokens/'+address
#     response = requests.request("GET", url)
#     response_data = json.loads(response.text)

#     # handling bad api responses
#     if 'data' not in response_data.keys():
#         # error handling for inconsistent api response formata
#         try:
#             api_response_code = response_data['errors']['status']
#         except:
#             try:
#                 api_response_code = response_data['errors'][0]['status']
#             except:
#                 api_response_code = 400
#         if verbose:
#             print('geckoterminal search failed for '+blockchain+address)
#         return(api_response_code,token_dict)

#     # assess validity of api data
#     try:
#         token_dict['source'] = 'geckoterminal'
#         token_dict['source_id'] = response_data['data']['id']
#         token_dict['symbol'] = response_data['data']['attributes']['symbol']
#         token_dict['name'] = response_data['data']['attributes']['name']
#         token_dict['price'] = float(response_data['data']['attributes']['price_usd'])
#         token_dict['decimals'] = float(response_data['data']['attributes']['decimals'])
#         try:
#             token_dict['mc'] = float(response_data['data']['attributes']['market_cap_usd'])
#         except:
#             token_dict['mc'] = 0
#         token_dict['fdv'] = float(response_data['data']['attributes']['fdv_usd'])
#         search_successful = True
#         if verbose:
#             print('geckoterminal metadata search successful for '+token_dict['symbol'])
#     except:
#         if verbose:
#             print('coingecko data is malformed, cancelling function')
#         return(400,{})

#     if float(response_data['data']['attributes']['decimals']) == 0:
#         api_response_code = 400
#         if verbose:
#             print('FAILURE: invalid geckoterminal decimals data for '+blockchain+address)

#     # storing json in gcs
#     filepath = 'data_lake/geckoterminal_coin_metadata/'
#     filename = str(token_dict['source_id']+'.json')
#     client = storage.Client(project='dreams-labs-data')
#     bucket = client.get_bucket('dreams-labs-storage')
#     blob = bucket.blob(filepath + filename)
#     blob.upload_from_string(json.dumps(response_data),content_type = 'json')
#     api_response_code = 200
#     if verbose:
#         print(filename+' uploaded successfully')

#     return(api_response_code,token_dict)




# # from community_calls_processing
# def coin_search_geckoterminal(
#         blockchain
#         ,address=None
#         ,pool=None
#         ,use_existing_json=True
#         ,override_existing_json=False
#         ,verbose=False
#     ):
#     '''
#     attempts to look up a coin on geckoterminal (no api key required) and
#     stores its metadata in cloud storage if instructed to.
#     accepts either a pool address or a blockchain+contract

#     param: blockchain <string> this must match chain_text_geckoterminal from core.chains
#     param: address <string> token contract address
#     param: pool <string> liquidity pool contract address
#     param: use_existing_json <boolean> whether to use an existing metadata json file (if it exists) instead of requesting a new one
#     param: override_existing <boolean> whether to override the existing metadata json file in cloud storage
#     return: api_status_code <int> geckoterminal api status code
#     return: token_dict <dict> a dictionary containing standardized token fields
#     '''
#     token_dict = {}

#     # setting up cloud storage client variables
#     project_name = 'dreams-labs-data'
#     bucket_name = 'dreams-labs-storage'
#     filepath = 'data_lake/geckoterminal_coin_metadata/'
#     client = storage.Client(project=project_name)
#     bucket = client.get_bucket(bucket_name)

#     ### CONVERTING POOL ADDRESS TO TOKEN ADDRESS (IF NECESSARY)
#     # the pool address was provided, use it to look up the token address
#     if pool:
#         address = convert_pool_to_token_address(blockchain,pool,verbose=verbose)
#     if address==None:
#         return(400,token_dict)

#     ### RETREIVING TOKEN METADATA
#     # attempt to load existing token metadata json file if use_existing_json=True
#     geckoterminal_id = f'{blockchain}_{address}'

#     if use_existing_json==True:
#         blobs = list(bucket.list_blobs(prefix=filepath))
#         stored_tokens = []
#         for blob in blobs:
#             stored_tokens = stored_tokens + [blob.name.replace(filepath,'').replace('.json','')]

#     if use_existing_json==True and geckoterminal_id in stored_tokens:
#         file_name = 'data_lake/geckoterminal_coin_metadata/'+geckoterminal_id+'.json'
#         blob = bucket.blob(file_name)
#         blob_contents = blob.download_as_string()
#         response_data = json.loads(blob_contents)
#         geckoterminal_status_code=200
#         override_existing_json=False # regardless of inputs, don't re-save the file we just loaded
#         if verbose:
#             print(f'loaded existing geckoterminal metadata for token {geckoterminal_id}')

#     # if necessary retrieve new metadata for the token using its blockchain+address
#     else:
#         url = f'https://api.geckoterminal.com/api/v2/networks/{blockchain}/tokens/{address}'
#         response = requests.request("GET", url)
#         response_data = json.loads(response.text)

#         # if we were told to use the existing json but couldn't find it, upload the new response we retrieved
#         if use_existing_json==True:
#             override_existing_json=True
#         if verbose:
#             print(f'retrieved new geckoterminal metadata for token {geckoterminal_id}')

#     # handling bad api responses
#     if 'data' not in response_data.keys():
#         # error handling for inconsistent api response formata
#         try:
#             api_response_code = response_data['errors']['status']
#         except:
#             try:
#                 api_response_code = response_data['errors'][0]['status']
#             except:
#                 api_response_code = 400
#         if verbose:
#             print(f'geckoterminal metadata search failed for {geckoterminal_id}')
#         return(api_response_code,token_dict)


#     ### PARSING AND STORING TOKEN METADATA
#     # assess validity of api data
#     try:
#         token_dict['source'] = 'geckoterminal'
#         token_dict['source_id'] = response_data['data']['id']
#         token_dict['chain'] = blockchain
#         token_dict['address'] = address
#         token_dict['symbol'] = response_data['data']['attributes']['symbol']
#         token_dict['name'] = response_data['data']['attributes']['name']
#         token_dict['price'] = float(response_data['data']['attributes']['price_usd'])
#         try:
#             token_dict['mc'] = float(response_data['data']['attributes']['market_cap_usd'])
#         except:
#             token_dict['mc'] = None
#         try:
#             token_dict['fdv'] = float(response_data['data']['attributes']['fdv_usd'])
#         except:
#             token_dict['fdv'] = None
#         try:
#             token_dict['decimals'] = float(response_data['data']['attributes']['decimals'])
#         except:
#             token_dict['decimals'] = None
#         if token_dict['decimals']==0:
#             token_dict['decimals'] = None
#         api_response_code = 200
#         if verbose:
#             print('geckoterminal metadata successfully retrieved for '+token_dict['name'])
#     except:
#         if verbose:
#             print('geckoterminal data is malformed, cancelling function')
#         return(400,{})

#     # storing token json in gcs
#     if api_response_code==200 and override_existing_json==True:
#         filename = geckoterminal_id+'.json'
#         blob = bucket.blob(filepath + filename)
#         blob.upload_from_string(json.dumps(response_data),content_type = 'json')
#         if verbose:
#             print(f'geckoterminal metadata {filename} uploaded successfully')

#     return(api_response_code,token_dict)
