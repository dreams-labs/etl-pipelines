import base64
import functions_framework
import pandas as pd
import numpy as np
import time
import datetime
import math
import requests
import json
import os
import io
import pdb
from dotenv import load_dotenv
from google.cloud import bigquery
import pandas_gbq
from google.cloud import storage
from google.cloud import secretmanager_v1
from oauth2client.service_account import ServiceAccountCredentials
import gspread


# SHARED UTILITY FUNCTIONS
# job-specific code begins at line 646
def human_format(num):
    '''
    converts a number to a scaled human readable string (e.g 7437283-->7.4M)

    TODO: the num<1 code should technically round upwards when truncating the
    string, e.g. 0.0678 right now will display as 0.067 but should be 0.068

    param: num <numeric>: the number to be reformatted
    return: formatted_number <string>: the number formatted as a human-readable string
    '''
    if num < 1:
        # decimals are output with enough precision to show two non-0 numbers
        num = np.format_float_positional(num, trim='-')
        after_decimal = str(num[2:])
        keep = 4+len(after_decimal) - len(after_decimal.lstrip('0'))
        num = num[:keep]
    else:
        num = float('{:.3g}'.format(num))
        magnitude = 0
        while abs(num) >= 1000:
            magnitude += 1
            num /= 1000.0
        num='{}{}'.format('{:f}'.format(num).rstrip('0').rstrip('.'), ['','k','m','B','T','QA','QI','SX','SP','O','N','D'][magnitude])

    return(num)


def get_secret(
      secret_name
      ,version='latest'
    ):
    '''
    retrieves a secret. works within bigquery python notebooks and needs
    testing in cloud functions

    param: secret_name <string> the name of the secret in secrets manager, 
        e.g. "apikey_coingecko_tentabs_free"
    param: version <string> the version of the secret to be loaded (only valid for notebooks)
    return: secret_value <string> the value of the secret
    '''
    project_id = '954736581165' # dreams labs project id (western-verve-411004)
    secret_path=f'projects/{project_id}/secrets/{secret_name}/versions/{version}'

    try:
        # load credentials from environmental variables
        load_dotenv()
        service_account_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

        if service_account_path:
            # if path is found use it for credentials (works in vscode)
            credentials = service_account.Credentials.from_service_account_file(service_account_path)
            client = secretmanager_v1.SecretManagerServiceClient(credentials=credentials)
        else:
            # if path isn't found, use default env variables (works in bigquery notebooks)
            client = secretmanager_v1.SecretManagerServiceClient()

        # initiate client and request secret
        request = secretmanager_v1.AccessSecretVersionRequest(name=secret_path)
        response = client.access_secret_version(request=request)
        secret_value = response.payload.data.decode('UTF-8')
    except:
        # syntax that works in GCF
        secret_value = os.environ.get(secret_name)

    return secret_value


def bigquery_run_sql(
        query_sql
        ,location='US'
        ,project = 'western-verve-411004'
    ):
    '''
    returns the blockchain and contract address of a coin on coingecko
    documentation: https://cloud.google.com/python/docs/reference/bigquery/latest

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


def bigquery_cache_sql(
        query_sql
        ,cache_file_name
        ,freshness=24
        ,verbose=False
    ):
    '''
    tries to use a cached result of a query from gcs. if it doesn't exist or
    is stale, reruns the query and returns fresh results.

    cache location: dreams-labs-storage/cache

    param: query_sql <string> the query to run
    param: cache_file_name <string> what to name the cache
    param: freshness <float> how many hours before a refresh is required
    return query_df <dataframe> the cached or fresh result of the query
    '''
    project_name = 'dreams-labs-data'
    bucket_name = 'dreams-labs-storage'
    filepath = f'cache/query_{cache_file_name}.csv'
    client = storage.Client(project=project_name)
    bucket = client.get_bucket(bucket_name)

    # Attempt to retrieve file freshness
    try:
        blob = bucket.get_blob(filepath)
        file_freshness = blob.updated if blob else None
    except Exception as e:
        print(f"error retrieving blob: {e}")
        file_freshness = None

    # Determine cache staleness
    cache_stale = file_freshness is None or \
                  (datetime.datetime.now(tz=datetime.timezone.utc) - file_freshness).total_seconds() / 3600 > freshness

    # Refresh cache if stale
    if cache_stale:
        query_df = bigquery_run_sql(query_sql)
        blob = bucket.blob(filepath)
        blob.upload_from_string(query_df.to_csv(index=False), content_type='text/csv')
        if verbose:
            print('returned fresh csv and refreshed cache')
    else:
        query_df = pd.read_csv(f'gs://{bucket_name}/{filepath}')
        if verbose:
            print('returned cached csv')

    return query_df



def gcs_load_image(
      filepath
      ,bucket='dreams-labs-storage'
    ):
    '''
    loads an image from gcs

    param: bucket <string> e.g. 'dreams-labs-storage'
    param: filepath <string> e.g. 'assets/whale_watch_logo_cropped.png'
    return: image <PIL image>
    '''
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket)
    blob = bucket.blob(filepath)
    image = Image.open(blob.open('rb'))

    return(image)


def gcs_upload_file(
        local_file
        ,gcs_folder
        ,gcs_filename
        ,project_name='dreams-labs-data'
        ,bucket_name='dreams-labs-public'
        ,delete_local_file=False
        ,verbose=False
    ):
    '''
    uploads a local file to public gcs and returns its access url

    param: local_file <string> the location of a local file to upload
    param: gcs_folder <string> the folder in gcs to upload to, e.g. 'whale_charts/'
    param: gcs_filename <string> the name the gcs file will be given
    param: project <string> google cloud project name
    param: bucket <string> GCS bucket name
    param: delete_local_file <boolean> whether to delete the local file after upload
    return: file_url <string> the url to access the file
    '''

    client = storage.Client(project=project_name)
    bucket = client.get_bucket(bucket_name)

    blob = bucket.blob(gcs_folder + gcs_filename)
    blob.upload_from_filename(local_file)
    file_url = str('https://storage.googleapis.com/'+bucket_name+'/'+gcs_folder+ gcs_filename)
    if verbose:
         ('file access url: '+file_url)

    if delete_local_file:
        os.remove(local_file)

    return(file_url)


def sheets_append_df(
        upload_df
        ,spreadsheet_id
        ,worksheet
        ,service_account_secret='service_account_eng_general'
        ,verbose=False
    ):
    '''
    appends a df to a worksheet in a google sheet. this includes
    normalizing data types and values that break the append function.

    param: upload_df <datetime> the df to append
    param: spreadsheet_id <string> from the url, e.g. '1X6AJWBJHisADvyqoXwEvTPi1JSNReVU_woNW32Hz_yQ'
    param: worksheet <string> the name of the worksheet, e.g.'gcf_calls_automated'
    param: service_account <string> the name of the secret containing the service account json
    '''
    # set up credentials
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    service_account = json.loads(get_secret(service_account_secret))
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(service_account, scope)
    gc = gspread.authorize(credentials)

    # parameters
    spreadsheet = gc.open_by_key(spreadsheet_id)
    worksheet = spreadsheet.worksheet(worksheet)

    # fix NaNs: replace all NaNs with None because NaNs break the sheets append
    upload_df = upload_df.fillna(np.nan).replace([np.nan], [None])

    # fix datetimes: convert to strings so they don't break the sheets append
    date_format = '%Y-%m-%d %H:%M:%S'
    datetime_cols = [col for col, dtype in upload_df.dtypes.items() if 'datetime64' in str(dtype)]
    for col in datetime_cols:
        upload_df[col] = upload_df[col].dt.strftime(date_format)

    # Convert DataFrame to a list of lists (2D array)
    data = upload_df.values.tolist()

    # Append the DataFrame data to the worksheet starting from cell A1
    append_outcome = worksheet.append_rows(data)
    if verbose:
        print(str(append_outcome))


def translate_chain(
        input_chain
        ,verbose=False
    ):
    '''
    attempts to match a chain alias and returns a dictionary with all
    corresponding aliases

    TODO TO IMPROVE PERFORMANCE
    it would be faster to precalculate daily chain_nicknames_df and
    load it from a stored csv rather than run a new query each time

    param: input_chain <string> the chain name input by the user
    return: chain_dict <dictionary> a dictionary with all available chain aliases
    '''

    # retreive chain ids for all aliases
    query_sql = '''
        select cn.chain_id
        ,cn.chain_reference
        ,ch.*
        from reference.chain_nicknames cn
        left join core.chains ch on ch.chain_id = cn.chain_id
        '''
    chain_nicknames_df = bigquery_cache_sql(query_sql,'chain_nicknames',verbose=verbose)

    # set everything to be lower case
    chain_nicknames_df['chain_reference'] = chain_nicknames_df['chain_reference'].str.lower()
    input_chain = input_chain.lower()



    chain_dict = {}
    input_chain_nicknames_df = chain_nicknames_df[chain_nicknames_df['chain_reference'] == input_chain]

    # return empty dict if chain alias could not be found
    if input_chain_nicknames_df.shape[0]==0:
        if verbose:
            print(f'input value "{input_chain}" could not be matched to any known chain alias')

    else:
        chain_dict['chain_id'] = input_chain_nicknames_df['chain_id'].values[0]
        chain_dict['chain_name'] = input_chain_nicknames_df['chain'].values[0]
        chain_text_columns = chain_nicknames_df.filter(regex='chain_text_').columns
        for c in chain_text_columns:
            nickname = input_chain_nicknames_df[c].values[0]
            if nickname:
                chain_dict[c.replace('chain_text_','')] = nickname
    if verbose:
        print(f'retrieved chain nicknames for {str(chain_dict.keys())}')

    return(chain_dict)


def coin_search_coingecko(
        coingecko_api_key
        ,blockchain=None
        ,address=None
        ,coingecko_id=None
        ,use_existing_json=True
        ,override_existing_json=False
        ,verbose=False
    ):
    '''
    attempts to look up a coin on coingecko, stores its metadata in gcs, and returns a dictionary
    with commonly used token metadata. accepts either blockchain+address or the coingecko_id. default
    function settings will cause this to load an existing json rather than request a new one.

    TODO: add support for using contract address to check if blob already exists, right now the logic
    only holds if a coingecko_id is input

    param: coingecko_api_key <string>
    param: blockchain <string> this must match chain_text_coingecko from core.chains
    param: address <string> token contract address
    param: coingecko_id <string> the coingecko id
    param: use_existing <boolean> whether to use an existing metadata json file (if it exists) instead of requesting a new one
    param: override_existing <boolean> whether to override the existing metadata json file in cloud storage
    return: api_status_code <int> geckoterminal api status code
    return: token_dict <dict> a dictionary containing standardized token fields
    '''

    # create empty dictionary to return if api call fails
    token_dict = {}

    # setting up cloud storage access
    project_name = 'dreams-labs-data'
    bucket_name = 'dreams-labs-storage'
    filepath = 'data_lake/coingecko_coin_metadata/'
    client = storage.Client(project=project_name)
    bucket = client.get_bucket(bucket_name)

    # if inputs say to take existing data into account, check if it exists and retrieve it
    if use_existing_json==True or override_existing_json==False:
        blobs = list(bucket.list_blobs(prefix=filepath))
        stored_jsons = []
        for blob in blobs:
            stored_jsons = stored_jsons + [blob.name.replace(filepath,'').replace('.json','')]

    # retrieve existing json blob if necessary and coingecko_id is available
    if use_existing_json==True and coingecko_id in stored_jsons:
        file_name = 'data_lake/coingecko_coin_metadata/'+coingecko_id+'.json'
        blob = bucket.blob(file_name)
        blob_contents = blob.download_as_string()
        response_data = json.loads(blob_contents)
        coingecko_status_code=200
        if verbose:
            print(f'loaded existing coingecko metadata for {coingecko_id}')

    # make the api call if existing json blob isn't used
    else:
        headers = {'x_cg_pro_api_key': coingecko_api_key}
        if coingecko_id:
            url = f'https://api.coingecko.com/api/v3/coins/{coingecko_id}'
        else:
            url = 'https://api.coingecko.com/api/v3/coins/'+chain+'/contract/'+address
        response = requests.request("GET", url, headers=headers)
        coingecko_status_code = response.status_code
        response_data = json.loads(response.text)
        if verbose:
            print(f'retreived new coingecko metadata for {coingecko_id}')

    # parsing json data
    if coingecko_status_code == 200:
        if coingecko_id:
            chain = list(response_data['detail_platforms'].keys())[0]
            address = response_data['detail_platforms'][chain]['contract_address']
            # for layer 1s, these come back as empty strings (e.g. coingecko_id='clore-ai')
            # null values are preferable to empty strings for accurate downstream data handling
            if chain == '':
                chain = None
            if address == '':
                address = None
        api_response_code = coingecko_status_code
        token_dict['source'] = 'coingecko'
        token_dict['source_id'] = response_data['id']
        token_dict['chain'] = chain
        token_dict['address'] = address
        token_dict['symbol'] = response_data['symbol'].upper()
        token_dict['name'] = response_data['name']

        # prelaunch tokens have no values for price, e.g. https://www.coingecko.com/en/coins/omnikingdoms-gold
        try:
            token_dict['price'] = float(response_data['market_data']['current_price']['usd'])
        except:
            token_dict['price'] = None

        # some tokens don't have fdv/mc/decimals included in response, e.g. https://www.coingecko.com/en/coins/metazero
        try:
            token_dict['fdv'] = float(response_data['market_data']['fully_diluted_valuation']['usd'])
        except:
            token_dict['fdv'] = None
        try:
            token_dict['mc'] = float(response_data['market_data']['market_cap']['usd'])
        except:
            token_dict['mc'] = None
        try:
            token_dict['decimals'] = float(response_data['detail_platforms'][chain]['decimal_place'])
        except:
            token_dict['decimals'] = None
        if verbose:
            print('successfully created coingecko token_dict for '+token_dict['symbol'])
    else:
        coingecko_id = None
        api_response_code = coingecko_status_code
        if verbose:
            print(f'{str(api_response_code)}: coingecko metadata search failed')

    # storing json in gcs
    if coingecko_status_code==200 and override_existing_json==True:
        filename = token_dict['source_id']+'.json'
        blob = bucket.blob(filepath + filename)
        blob.upload_from_string(json.dumps(response_data),content_type = 'json')
        if verbose:
            print(f'coingecko metadata {filename} uploaded successfully')

    return(api_response_code,token_dict)


def convert_pool_to_token_address(
        blockchain
        ,pool
        ,use_existing_json=True
        ,override_existing_json=False
        ,verbose=False
    ):
    '''
    attempts to convert a dex pool address to a coin address using geckoterminal

    param: blockchain <string> this must match chain_text_geckoterminal from core.chains
    param: pool <string> liquidity pool contract address
    param: use_existing_json <boolean> whether to use an existing metadata json file (if it exists) instead of requesting a new one
    param: override_existing <boolean> whether to override the existing metadata json file in cloud storage
    return:
    '''
    # setting up cloud storage access
    project_name = 'dreams-labs-data'
    bucket_name = 'dreams-labs-storage'
    filepath = 'data_lake/geckoterminal_pool_metadata/'
    client = storage.Client(project=project_name)
    bucket = client.get_bucket(bucket_name)

    pool_id = f'{blockchain}_{pool}'
    address = None
    if verbose:
        print(f'converting pool address to token address...')

    # attempt to load existing liquidity pool json file if use_existing_json=True
    if use_existing_json==True:
        blobs = list(bucket.list_blobs(prefix=filepath))
        stored_pools = []
        for blob in blobs:
            stored_pools = stored_pools + [blob.name.replace(filepath,'').replace('.json','')]

    if use_existing_json==True and pool_id in stored_pools:
        file_name = 'data_lake/geckoterminal_pool_metadata/'+pool_id+'.json'
        blob = bucket.blob(file_name)
        blob_contents = blob.download_as_string()
        response_data = json.loads(blob_contents)
        pool_response_code = 200
        if verbose:
            print(f'loaded existing geckoterminal metadata for pool {pool_id}')

    # if necessary retrieve new metadata for the pool using its blockchain+address
    else:
        url = f'https://api.geckoterminal.com/api/v2/networks/{blockchain}/pools/{pool}?include=base_token'
        response = requests.request("GET", url)
        pool_response_code = response.status_code
        if pool_response_code==200:
            response_data = json.loads(response.text)
            if verbose:
                print(f'retreived new geckoterminal metadata for pool {pool_id}')
            # if we were told to use the existing json but couldn't find it, upload the new response we retrieved
            if use_existing_json==True:
                override_existing_json=True

    # storing pool json in gcs
    if pool_response_code==200 and override_existing_json==True:
        filename = pool_id+'.json'
        blob = bucket.blob(filepath + filename)
        blob.upload_from_string(json.dumps(response_data),content_type = 'json')
        if verbose:
            print(f'pool metadata {filename} uploaded successfully')

    # extract the address, which allows us to retrieve token metadata
    try:
        address = response_data['included'][0]['attributes']['address']
    except:
        if verbose:
            print('pool metadata does not include valid address')

    return(address)


def coin_search_geckoterminal(
        blockchain
        ,address=None
        ,pool=None
        ,use_existing_json=True
        ,override_existing_json=False
        ,verbose=False
    ):
    '''
    attempts to look up a coin on geckoterminal (no api key required) and
    stores its metadata in cloud storage if instructed to.
    accepts either a pool address or a blockchain+contract

    param: blockchain <string> this must match chain_text_geckoterminal from core.chains
    param: address <string> token contract address
    param: pool <string> liquidity pool contract address
    param: use_existing_json <boolean> whether to use an existing metadata json file (if it exists) instead of requesting a new one
    param: override_existing <boolean> whether to override the existing metadata json file in cloud storage
    return: api_status_code <int> geckoterminal api status code
    return: token_dict <dict> a dictionary containing standardized token fields
    '''
    token_dict = {}

    # setting up cloud storage client variables
    project_name = 'dreams-labs-data'
    bucket_name = 'dreams-labs-storage'
    filepath = 'data_lake/geckoterminal_coin_metadata/'
    client = storage.Client(project=project_name)
    bucket = client.get_bucket(bucket_name)

    ### CONVERTING POOL ADDRESS TO TOKEN ADDRESS (IF NECESSARY)
    # the pool address was provided, use it to look up the token address
    if pool:
        address = convert_pool_to_token_address(blockchain,pool,verbose=verbose)
    if address==None:
        return(400,token_dict)

    ### RETREIVING TOKEN METADATA
    # attempt to load existing token metadata json file if use_existing_json=True
    geckoterminal_id = f'{blockchain}_{address}'

    if use_existing_json==True:
        blobs = list(bucket.list_blobs(prefix=filepath))
        stored_tokens = []
        for blob in blobs:
            stored_tokens = stored_tokens + [blob.name.replace(filepath,'').replace('.json','')]

    if use_existing_json==True and geckoterminal_id in stored_tokens:
        file_name = 'data_lake/geckoterminal_coin_metadata/'+geckoterminal_id+'.json'
        blob = bucket.blob(file_name)
        blob_contents = blob.download_as_string()
        response_data = json.loads(blob_contents)
        geckoterminal_status_code=200
        override_existing_json=False # regardless of inputs, don't re-save the file we just loaded
        if verbose:
            print(f'loaded existing geckoterminal metadata for token {geckoterminal_id}')

    # if necessary retrieve new metadata for the token using its blockchain+address
    else:
        url = f'https://api.geckoterminal.com/api/v2/networks/{blockchain}/tokens/{address}'
        response = requests.request("GET", url)
        response_data = json.loads(response.text)

        # if we were told to use the existing json but couldn't find it, upload the new response we retrieved
        if use_existing_json==True:
            override_existing_json=True
        if verbose:
            print(f'retrieved new geckoterminal metadata for token {geckoterminal_id}')

    # handling bad api responses
    if 'data' not in response_data.keys():
        # error handling for inconsistent api response formata
        try:
            api_response_code = response_data['errors']['status']
        except:
            try:
                api_response_code = response_data['errors'][0]['status']
            except:
                api_response_code = 400
        if verbose:
            print(f'geckoterminal metadata search failed for {geckoterminal_id}')
        return(api_response_code,token_dict)


    ### PARSING AND STORING TOKEN METADATA
    # assess validity of api data
    try:
        token_dict['source'] = 'geckoterminal'
        token_dict['source_id'] = response_data['data']['id']
        token_dict['chain'] = blockchain
        token_dict['address'] = address
        token_dict['symbol'] = response_data['data']['attributes']['symbol']
        token_dict['name'] = response_data['data']['attributes']['name']
        token_dict['price'] = float(response_data['data']['attributes']['price_usd'])
        try:
            token_dict['mc'] = float(response_data['data']['attributes']['market_cap_usd'])
        except:
            token_dict['mc'] = None
        try:
            token_dict['fdv'] = float(response_data['data']['attributes']['fdv_usd'])
        except:
            token_dict['fdv'] = None
        try:
            token_dict['decimals'] = float(response_data['data']['attributes']['decimals'])
        except:
            token_dict['decimals'] = None
        if token_dict['decimals']==0:
            token_dict['decimals'] = None
        api_response_code = 200
        if verbose:
            print('geckoterminal metadata successfully retrieved for '+token_dict['name'])
    except:
        if verbose:
            print('geckoterminal data is malformed, cancelling function')
        return(400,{})

    # storing token json in gcs
    if api_response_code==200 and override_existing_json==True:
        filename = geckoterminal_id+'.json'
        blob = bucket.blob(filepath + filename)
        blob.upload_from_string(json.dumps(response_data),content_type = 'json')
        if verbose:
            print(f'geckoterminal metadata {filename} uploaded successfully')

    return(api_response_code,token_dict)



### CALLS PROCESSING SPECIFIC FUNCTIONS

def retrieve_call_metadata(
        calls_df
        ,verbose=False
    ):
    '''
    attempts to look up metadata for all calls_df records. if records have
    a valid url from any of these sources then the call can be automatically
    processed:
    * coingecko
    * geckoterminal
    * dexscreener
    * dextools

    the result of the metadata retrieval attempt is appended on to calls_df and
    returned in calls_df_processed

    param: calls_df <dataframe> new calls from etl_pipelines.dreambot_new_calls
    return: calls_df_processed <dataframe> the input df with match attempt outcomes
    '''
    # retrieve metadata for calls
    data_rows = []

    for i in range(calls_df.shape[0]):

        call_details = calls_df.iloc[i]

        # because we are dealing with nonstandardized user input we are wrapping the
        # whole thing in a try/except and assigning unforseen errors to the manual
        # processing queue
        try:

            # retrieve and parse user-submitted token url
            url = call_details['coingecko_url']
            # if there are ' 's in the url string, only use the text after the final ' '
            url = url.strip().split(' ')[-1]
            # remove all utm params etc after '?'
            url = url.split('?')[0]

            # get token_dict if one of the four most common sources were provided:
            token_dict = None
            api_response_code = 404

            if 'geckoterminal' in url:
                parts = url.split('/')
                index = parts.index('pools')
                geckoterminal_chain = parts[index-1]
                pool_contract = parts[index+1]

                api_response_code,token_dict = coin_search_geckoterminal(
                    geckoterminal_chain
                    ,pool=pool_contract
                    ,use_existing_json=True
                )

            elif 'dexscreener' in url:
                parts = url.split('/')
                index = parts.index('dexscreener.com')
                dexscreener_chain = parts[index+1]
                pool_contract = parts[index+2]
                chain_dict = translate_chain(dexscreener_chain)

                api_response_code,token_dict = coin_search_geckoterminal(
                    chain_dict['geckoterminal']
                    ,pool=pool_contract
                    ,use_existing_json=True
                )

            elif 'dextools' in url:
                parts = url.split('/')
                index = parts.index('pair-explorer')
                dextools_chain = parts[index-1]
                pool_contract = parts[index+1]
                chain_dict = translate_chain(dextools_chain)

                api_response_code,token_dict = coin_search_geckoterminal(
                    chain_dict['geckoterminal']
                    ,pool=pool_contract
                    ,use_existing_json=True
                )

            elif 'coingecko' in url:
                parts = url.split('/')
                try:
                    index = parts.index('coins')
                    coingecko_id = parts[index+1]
                except:
                # using 'coins' as index got broken by the indonesian url
                # https://www.coingecko.com/id/koin_koin/hathor
                    index = parts.index('www.coingecko.com')
                    coingecko_id = parts[index+2]

                api_response_code,token_dict = coin_search_coingecko(
                    coingecko_api_key
                    ,coingecko_id=coingecko_id
                    ,use_existing_json=True
                )

        # catch-all for coin metadata errors, this will put them into the manual processing queue
        except:
            api_response_code = 400
            token_dict = {}


        # assess validity of user-submitted initial market cap
        initial_mc_processed = call_details['init_market_cap'].lower().replace('million','').replace('mil','').replace('m','').replace(' ','').replace('$','')
        try:
            initial_mc_processed = float(initial_mc_processed)
            if 0.1<initial_mc_processed<5000:
                initial_mc_valid = True
            else:
                initial_mc_valid = False
        except:
            initial_mc_valid = False


        # processing is successful if the call can be automatically added to the calls sheet
        if api_response_code==200 and initial_mc_valid==True:
            processing_successful = True
        else:
            processing_successful = False

        # compile the processing results into dataframe rows
        row_data = {}
        row_data = {
            # calls processing metadata
            'call_id': call_details['call_id']
            ,'match_attempted_at': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ,'processing_successful': processing_successful
            ,'initial_mc_valid': initial_mc_valid
            ,'match_api_response': api_response_code

            # data input by the user
            ,'reference': call_details['symbol']
            ,'category': call_details['category']
            ,'call_date': call_details['call_date']
            ,'caller': call_details['nickname']
            ,'discord_url': call_details['discord_thread']
            ,'market_cap_url': url
            ,'initial_mc': initial_mc_processed if initial_mc_valid else call_details['init_market_cap']
            }

        # token metadata if successfully retrieved
        if api_response_code==200:
            # picking a human readable chain name to be included with the call
            try:
                # this statement will fail if the call is on a chain that does not
                # exist in core.chains. in this case token_dict is an empty dictionary
                # that does not have a 'chain_name'
                chain_reference = translate_chain(token_dict['chain'])['chain_name']
            except:
                chain_reference = token_dict['chain']

            row_data.update({
                'reference': str(token_dict['symbol']+' ('+token_dict['name']+')')
                ,'chain': chain_reference
                ,'token_contract': token_dict['address']
                ,'metadata_source': token_dict['source']
                ,'metadata_source_id': token_dict['source_id']
            })

        data_rows.append(row_data)


    # combine the rows into a dataframe and split it between calls that can be \
    # fully automated and those that need manual review
    calls_df_processed = pd.DataFrame(data_rows)
    columns_ordered = [
        'call_id'
        ,'match_attempted_at'
        ,'processing_successful'
        ,'initial_mc_valid'
        ,'match_api_response'
        ,'metadata_source'
        ,'metadata_source_id'
        ,'reference'
        ,'category'
        ,'call_date'
        ,'caller'
        ,'discord_url'
        ,'market_cap_url'
        ,'chain'
        ,'token_contract'
        ,'initial_mc'
    ]
    calls_df_processed = calls_df_processed.reindex(columns=columns_ordered)

    if verbose:
        print(
            f'processed {calls_df_processed.shape[0]} call(s) and '
            f'automated {(calls_df_processed["processing_successful"]==True).sum()}'
        )

    return calls_df_processed


# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def process_community_calls(cloud_event):
    verbose=True

    # load secrets
    coingecko_api_key = get_secret('apikey_coingecko_tentabs_free')
    service_account = json.loads(get_secret('service_account_eng_general'))

    # pull in the calls
    query_sql = '''
        select cn.*
        from etl_pipelines.dreambot_new_calls cn
        left join etl_pipelines.community_calls_intake_processed cp on cp.call_id = cn.call_id
        where cp.call_id is null
        and cn.call_date > '2024-04-03' # calls before this date were all manually processed
        '''
    calls_df = bigquery_run_sql(query_sql)
    if verbose:
        print(f'retrieved {str(calls_df.shape[0])} new calls')

    # retreieve metadata for calls
    calls_df_processed = retrieve_call_metadata(calls_df,verbose=True)


    # set df datatypes so it uploads to bigquery
    # this could likely be moved upstream to when the df is created
    dtype_mapping = {
        'call_id': str
        ,'match_attempted_at': 'datetime64[ns, UTC]'
        ,'processing_successful': bool
        ,'initial_mc_valid': bool
        ,'match_api_response': int
        ,'metadata_source': str
        ,'metadata_source_id': str
        ,'reference': str
        ,'category': str
        ,'call_date': 'datetime64[ns, UTC]'
        ,'caller': str
        ,'discord_url': str
        ,'market_cap_url': str
        ,'chain': str
        ,'token_contract': str
        ,'initial_mc': str             
    }
    upload_df = calls_df_processed.astype(dtype_mapping)


    # define table schema datatypes and upload df to it
    project_id = 'western-verve-411004'
    table_name = 'etl_pipelines.community_calls_intake_processed'
    schema = [
        {'name':'call_id', 'type': 'string'}
        ,{'name':'match_attempted_at', 'type': 'datetime'}
        ,{'name':'processing_successful', 'type': 'bool'}
        ,{'name':'initial_mc_valid', 'type': 'bool'}
        ,{'name':'match_api_response', 'type': 'int64'}
        ,{'name':'metadata_source', 'type': 'string'}
        ,{'name':'metadata_source_id', 'type': 'string'}
        ,{'name':'reference', 'type': 'string'}
        ,{'name':'category', 'type': 'string'}
        ,{'name':'call_date', 'type': 'datetime'}
        ,{'name':'caller', 'type': 'string'}
        ,{'name':'discord_url', 'type': 'string'}
        ,{'name':'market_cap_url', 'type': 'string'}
        ,{'name':'chain', 'type': 'string'}
        ,{'name':'token_contract', 'type': 'string'}
        ,{'name':'initial_mc', 'type': 'string'}
    ]
    pandas_gbq.to_gbq(
        upload_df
        ,table_name
        ,project_id=project_id
        ,if_exists='append'
        ,table_schema=schema
        ,progress_bar=False)


    # export processed calls to google sheets, splitting by automated vs manual
    # automated calls
    calls_df_automated = calls_df_processed[calls_df_processed['processing_successful']==True]
    sheets_append_df(
        calls_df_automated
        ,spreadsheet_id='1X6AJWBJHisADvyqoXwEvTPi1JSNReVU_woNW32Hz_yQ'
        ,worksheet='gcf_calls_automated'
        ,verbose=True
    )
    # manual calls
    calls_df_manual = calls_df_processed[calls_df_processed['processing_successful']==False]
    sheets_append_df(
        calls_df_manual
        ,spreadsheet_id='1X6AJWBJHisADvyqoXwEvTPi1JSNReVU_woNW32Hz_yQ'
        ,worksheet='gcf_calls_manual'
        ,verbose=True
    )
