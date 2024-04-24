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
import kaleido
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio
from dotenv import load_dotenv
from PIL import Image, ImageOps
from google.cloud import bigquery
from google.cloud import storage
from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from google.cloud import secretmanager_v1
import google.auth
from google.oauth2 import service_account
import pandas_gbq
import logging

'''

testable scenarios
* valid solana address
* valid erc20 address
* invalid blockchain input
* valid but unsupported blockchain input
* invalid days of history input
* invalid address input
* invalid whale_threshold_usd input
* invalid whale_threshold_tokens input
* coingecko match fails but geckoterminal succeeds
* coingecko and geckoterminal match fails
* dune query timeout
* dune query failure
* dune query results in less than 2 rows
* bad dune api key
* bad coingecko api key

# cloud function testing input syntax
{
    'blockchain': 'SOL'
    ,'address': '3bRTivrVsitbmCTGtqwp7hxXPsybkjn4XLNtPsHqa3zR'
    ,'whale_threshold': 25000
    ,'days_of_history': 10000
}

{
    'blockchain': 'ethereum'
    ,'address': '0x6982508145454ce325ddbe47a25d4ec3d2311933'
    ,'whale_threshold': 25000
    ,'days_of_history': 180
}

'''

# SHARED UTILITY FUNCTIONS
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


def get_secret(
        secret_name,
        service_account_path=None,
        project_id='954736581165',
        version='latest'
    ):
    '''
    Retrieves a secret from GCP Secrets Manager.

    Parameters:
    secret_name (str): The name of the secret in Secrets Manager.
    service_account_path (str, optional): Path to the service account JSON file.
    version (str): The version of the secret to be loaded.

    Returns:
    str: The value of the secret.
    '''
    
    # Construct the resource name of the secret version.
    secret_path = f'projects/{project_id}/secrets/{secret_name}/versions/{version}'

    # Initialize the Google Secret Manager client
    client = initialize_secret_manager_client(service_account_path)

    # Request to access the secret version
    request = secretmanager_v1.AccessSecretVersionRequest(name=secret_path)
    response = client.access_secret_version(request=request)
    return response.payload.data.decode('UTF-8')


def initialize_secret_manager_client(service_account_path):
    '''
    Initialize the Secret Manager client with the appropriate credentials.

    Parameters:
    service_account_path (str): Path to the service account JSON file.

    Returns:
    SecretManagerServiceClient: A client for the Secret Manager Service.
    '''
    if service_account_path:
        # Explicitly use the provided service account file for credentials
        credentials = service_account.Credentials.from_service_account_file(service_account_path)
    else:
        # Attempt to use default credentials
        credentials, _ = google.auth.default()

    return secretmanager_v1.SecretManagerServiceClient(credentials=credentials)


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
    whale_chart_url = str('https://storage.googleapis.com/'+bucket_name+'/'+gcs_folder+ gcs_filename)
    if verbose:
         ('file access url: '+whale_chart_url)

    if delete_local_file:
        os.remove(local_file)

    return(whale_chart_url)




# WHALE WATCH SPECIFIC FUNCTIONS
def lookup_chain_ids(
        input_chain
        ,verbose=False
    ):
    '''
    attempts to match a chain nickname and returns its chain_id

    TODO TO IMPROVE LOGIC
    this should return a dictionary with all available references. the return
    would be (chain_dict,match_outcome) and this new function could be 
    reusable and made universal

    TODO TO IMPROVE PERFORMANCE
    it would be faster to precalculate the match df and load it
    as a csv rather than as a new query each time

    param: input_chain <string> the chain name input by the user
    return: chain_id <int> the core.chains.chain_id of the input
    return: match_outcome <boolean> outcome of match
    '''

    query_sql = '''
        select cn.chain_id
        ,cn.chain_reference
        ,ch.chain_id
        ,ch.chain_text_dune
        ,ch.chain_text_coingecko
        ,ch.chain_text_geckoterminal
        from reference.chain_nicknames cn
        left join core.chains ch on ch.chain_id = cn.chain_id
        '''
    chain_nicknames = run_bigquery_sql(query_sql)

    # set everything to be lower case
    chain_nicknames['chain_reference'] = chain_nicknames['chain_reference'].str.lower()
    input_chain_raw = input_chain # store raw input value for error message
    input_chain = input_chain.lower()

    # declare variables
    chain_id = None
    chain_text_dune = None
    chain_text_coingecko = None
    chain_text_geckoterminal = None

    # attempt match
    try:
        # pull chain_id for alias
        chain_id = chain_nicknames[chain_nicknames['chain_reference'] == input_chain]['chain_id'].values[0]

        # determine whether chain is supported in dune
        chain_text_dune = chain_nicknames[chain_nicknames['chain_reference'] == input_chain]['chain_text_dune'].values[0]
        if chain_text_dune == None:
            match_outcome = 'unsupported chain'
        else:
            match_outcome = 'success'
            chain_text_coingecko = chain_nicknames[chain_nicknames['chain_reference'] == input_chain]['chain_text_coingecko'].values[0]
            chain_text_geckoterminal = chain_nicknames[chain_nicknames['chain_reference'] == input_chain]['chain_text_geckoterminal'].values[0]
            if verbose:
                print("chain '"+input_chain+"' valid for dune query...")

    except:
        match_outcome = 'invalid chain'

    return(chain_id,chain_text_dune,chain_text_coingecko,chain_text_geckoterminal,match_outcome)


def coingecko_metadata_search(
        coingecko_api_key
        ,blockchain
        ,address
        ,verbose=False
    ):
    '''
    attempts to look up a coin on coingecko and store its metadata in gcs

    param: coingecko_api_key <string>
    param: blockchain <string> this must match chain_text_coingecko from core.chains
    param: address <string> token contract address
    return: api_status_code <int> geckoterminal api status code
    return: token_dict <dict> a dictionary containing standardized token fields
    '''
    token_dict = {}

    # making the api call
    headers = {'x_cg_pro_api_key': coingecko_api_key}
    url = 'https://api.coingecko.com/api/v3/coins/'+blockchain+'/contract/'+address
    response = requests.request("GET", url, headers=headers)
    response_data = json.loads(response.text)

    if response.status_code == 200:
        search_successful = True
        token_dict['source'] = 'coingecko'
        token_dict['source_id'] = response_data['id']
        token_dict['symbol'] = response_data['symbol'].upper()
        token_dict['name'] = response_data['name']
        token_dict['price'] = float(response_data['market_data']['current_price']['usd'])
        token_dict['decimals'] = float(response_data['detail_platforms'][blockchain]['decimal_place'])
        token_dict['mc'] = float(response_data['market_data']['market_cap']['usd'])
        token_dict['fdv'] = float(response_data['market_data']['fully_diluted_valuation']['usd'])
        api_response_code = 200
        if verbose:
            print('coingecko metadata search successful for '+token_dict['symbol'])
    else:
        coingecko_id = None
        search_successful = False
        api_response_code = 400
        if verbose:
            print('coingecko metadata search failed for '+blockchain+':'+address)

    # storing json in gcs
    if search_successful:
        filepath = 'data_lake/coingecko_coin_metadata/'
        filename = str(token_dict['source_id']+'.json')

        client = storage.Client(project='dreams-labs-data')
        bucket = client.get_bucket('dreams-labs-storage')

        blob = bucket.blob(filepath + filename)
        blob.upload_from_string(json.dumps(response_data),content_type = 'json')
        if verbose:
            print(filename+' uploaded successfully')

    return(api_response_code,token_dict)


def geckoterminal_metadata_search(
        blockchain
        ,address
        ,verbose=False
    ):
    '''
    attempts to look up a coin on geckoterminal (no api key required)

    param: blockchain <string> this must match chain_text_coingecko from core.chains
    param: address <string> token contract address
    return: api_status_code <int> geckoterminal api status code
    return: token_dict <dict> a dictionary containing standardized token fields
    '''
    token_dict = {}

    # making the api call
    url = 'https://api.geckoterminal.com/api/v2/networks/'+blockchain+'/tokens/'+address
    response = requests.request("GET", url)
    response_data = json.loads(response.text)

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
            print('geckoterminal search failed for '+blockchain+address)
        return(api_response_code,token_dict)

    # assess validity of api data
    try:
        token_dict['source'] = 'geckoterminal'
        token_dict['source_id'] = response_data['data']['id']
        token_dict['symbol'] = response_data['data']['attributes']['symbol']
        token_dict['name'] = response_data['data']['attributes']['name']
        token_dict['price'] = float(response_data['data']['attributes']['price_usd'])
        token_dict['decimals'] = float(response_data['data']['attributes']['decimals'])
        try:
            token_dict['mc'] = float(response_data['data']['attributes']['market_cap_usd'])
        except:
            token_dict['mc'] = 0
        token_dict['fdv'] = float(response_data['data']['attributes']['fdv_usd'])
        search_successful = True
        if verbose:
            print('geckoterminal metadata search successful for '+token_dict['symbol'])
    except:
        if verbose:
            print('coingecko data is malformed, cancelling function')
        return(400,{})

    if float(response_data['data']['attributes']['decimals']) == 0:
        api_response_code = 400
        if verbose:
            print('FAILURE: invalid geckoterminal decimals data for '+blockchain+address)

    # storing json in gcs
    filepath = 'data_lake/geckoterminal_coin_metadata/'
    filename = str(token_dict['source_id']+'.json')
    client = storage.Client(project='dreams-labs-data')
    bucket = client.get_bucket('dreams-labs-storage')
    blob = bucket.blob(filepath + filename)
    blob.upload_from_string(json.dumps(response_data),content_type = 'json')
    api_response_code = 200
    if verbose:
        print(filename+' uploaded successfully')

    return(api_response_code,token_dict)



def dune_get_token_transfers(
        chain_text_dune,
        contract_address,
        decimals
    ):
    '''
    retrieves the daily net transfers from dune for each wallet that has transferred the given 
    token, consolidated by day and wallet address to reduce ETL load and table size. 

    param: chain_text_dune <string> the dune text of the blockchain 
    param: contract_address <string> the contract address of the token
    param: decimals <int> the number of decimals of the token
    return: transfers_df <dataframe> a dataframe with all historical transfers by wallet address
    '''
    # determine which query based on blockchain
    if chain_text_dune == 'solana':
        query_id = 3658238  # url: https://dune.com/queries/{query_id}
    else:
        query_id = 3628115 

    # define query params
    dune = DuneClient.from_env()
    transfers_query = QueryBase(
        query_id=query_id,
        params=[
            QueryParameter.text_type(name='blockchain_name', value=blockchain_name),
            QueryParameter.text_type(name='contract_address', value=contract_address),
            QueryParameter.number_type(name='decimals', value=decimals),
        ]
    )
    # run dune query and load to a dataframe
    logger = logging.getLogger('dune_client')
    logger.setLevel(logging.ERROR)
    transfers_df = dune.run_query_dataframe(transfers_query, ping_frequency=5)

    return transfers_df


def get_whale_counts_from_transfers(
        transfers_df,
        whale_threshold_tokens,
        shrimp_threshold_tokens
    ):
    '''
    adds up daily wallet transfers to determine balances, then returns a df showing the number
    of S/M/L wallets on a given date based on token thresholds

    Parameters:
        transfers_df (pandas.DataFrame): df of token transfers
        whale_threshold_tokens (float): threshold for whale wallet
        shrimp_threshold_tokens (float): threshold for small wallet

    Returns:
        pandas.DataFrame: df of daily s/m/whale wallet counts
    '''

    # sort and calculate cumulative balances
    whales_df = transfers_df.sort_values(['wallet_address', 'date'])
    whales_df['balance'] = whales_df.groupby('wallet_address')['daily_net_transfers'].cumsum()

    # Function to classify each balance
    def classify_balance(row):
        if row['balance'] >= whale_threshold_tokens:
            return 'whale_wallets'
        elif row['balance'] <= shrimp_threshold_tokens:
            return 'small_wallets'
        else:
            return 'medium_wallets'

    # Apply the classification
    whales_df['wallet_type'] = whales_df.apply(classify_balance, axis=1)

    # Group by 'date' and 'wallet_type' and count each type, then unstack to create columns for each wallet type
    whales_df = whales_df.groupby(['date', 'wallet_type']).size().unstack(fill_value=0)

    # Ensure all wallet types and dates are present in the DataFrame, even if they have zero counts
    whales_df = whales_df.reindex(columns=['whale_wallets', 'medium_wallets', 'small_wallets'], fill_value=0)

    whales_df.index = pd.to_datetime(whales_df.index)
    whales_df = whales_df.resample('D').ffill()

    return whales_df


def upload_transfers_to_bigquery(
        transfers_df,
        chain_text_dune,
        contract_address,
        verbose=False
    ):
    '''
    uploads token transfers to bigquery if no records exist for this token. if 
    records do exist they should be kept fresh by another pipelines. 

    parameters:
        transfers_df (pandas.DataFrame): df of token transfers
        chain_text_dune (str): chain text
        contract_address (str): contract address
    '''


    # check if records already exist for this token
    query_sql = f'''
        select count(*)
        from etl_pipelines.coin_wallet_net_transfers
        where data_source = 'dune'
        and chain_text_source = '{chain_text_dune}'
        and token_address = '{contract_address}'
    '''
    query_df = run_bigquery_sql(query_sql)
    if verbose:
        print(f'found {str(query_df.iloc[0,0])} records for <{chain_text_dune}:{contract_address}>')

    # if we don't already have data, upload it
    if query_df.iloc[0,0] == 0:
        if verbose:
            print(f'uploading {str(len(transfers_df))} records for <{chain_text_dune}:{contract_address}>')
        
        # add metadata to upload_df
        upload_df = transfers_df.rename(columns={'chain': 'chain_text_source'})
        upload_df['data_source'] = 'dune'
        upload_df['data_updated_at'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # set df datatypes of upload df
        dtype_mapping = {
            'date': 'datetime64[ns, UTC]',
            'chain_text_source': str,
            'token_address': str,
            'decimals': int,
            'wallet_address': str,
            'daily_net_transfers': float,
            'data_source': str,
            'data_updated_at': 'datetime64[ns, UTC]'
        }
        upload_df = upload_df.astype(dtype_mapping)

        # upload df to bigquery
        project_id = 'western-verve-411004'
        table_name = 'etl_pipelines.coin_wallet_net_transfers'
        schema = [
            {'name':'date', 'type': 'datetime'},
            {'name':'chain_text_source', 'type': 'string'},
            {'name':'token_address', 'type': 'string'},
            {'name':'decimals', 'type': 'int64'},
            {'name':'wallet_address', 'type': 'string'},
            {'name':'daily_net_transfers', 'type': 'float64'},
            {'name':'data_source', 'type': 'string'},
            {'name':'date', 'data_updated_at': 'datetime'},
        ]
        pandas_gbq.to_gbq(
            upload_df
            ,table_name
            ,project_id=project_id
            ,if_exists='append'
            ,table_schema=schema
            ,progress_bar=False)


def draw_whale_chart(
        query_df
        ,whale_threshold_usd
        ,whale_threshold_tokens
        ,shrimp_threshold_usd
        ,shrimp_threshold_tokens
        ,days_of_history
        ,token_dict
        ,verbose=False
    ):
    '''
    TODO: improve efficiency of border process to not save/load file, which 
    causes performance issues in GCF

    draws the whale chart and saves it as a png file

    param: query_df <dataframe> the results from the dune query
    param: whale_threshold_usd <int> the usd whale threshold submitted by the user
    param: whale_threshold_tokens <int> the token threshold used in the dune query
    param: token_dict <dict> standardized dictionary of token metadata
    param: verbose <boolean> whether to print each step 
    return: whale_chart <image> an img library image
    '''
    if verbose: 
        print('charting: starting function...') 
    
    # create 'date' column that will be used for x axis
    query_df = query_df.reset_index()
    query_df = query_df.rename(columns={'index': 'date'})
    query_df['date'] = pd.to_datetime(query_df['date'])

    # filter the df to only includes records since `days_of_history`
    most_recent_date = query_df['date'].max()
    chart_history_cutoff = most_recent_date - pd.DateOffset(days=days_of_history)
    query_df = query_df[query_df['date'] > chart_history_cutoff]

    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # General plot settings
    fig.update_layout(
        width=1450,
        height=1000,
        paper_bgcolor='#131722',
        plot_bgcolor='#131722',
        font=dict(
            family='Sans Serif', # tentabs:I want to use a custom fonts but can't get plotly to recognize a ttf
            color='white',
            size=18
        ),
        margin=dict(
            t=100,
            b=20,
            l=95,
            r=0
        )
    )
    if verbose: 
            print('charting: setting variables...')
    
    # define title and annotations based on coingecko metadata
    symbol = token_dict['symbol']
    name = token_dict['name']
    coin_reference = symbol+' ('+name+')'
    if len(coin_reference) > 25:
        coin_reference = coin_reference[:23]+'...'
    current_price = token_dict['price']
    mc = human_format(token_dict['mc'])
    if mc == '0':
        mc = 'Unknown'
    fdv = human_format(token_dict['fdv'])

    if verbose: 
        print('charting: adding annotations...')
    # add title
    fig.update_layout(
        title=dict(
            text=str(f'Whale Watch: {coin_reference}').replace('$','&#36;'),
            font=dict(size=32),
            xanchor='left', yanchor='top',
            x=0.100, y=.98
        )
    )
    # add annotations
    fig.add_annotation(
        text=str('Whale Threshold: $'+human_format(whale_threshold_usd)+' USD').replace('$','&#36;'),
        font=dict(size=24),
        xref='paper', yref='paper',
        x=0.040, y=1.06,
        showarrow=False
    )
    fig.add_annotation(
        text=str('Current Price: $'+human_format(current_price)+' USD').replace('$','&#36;'),
        font=dict(size=24),
        xref='paper', yref='paper',
        xanchor='right',
        x=0.99, y=1.10,
        showarrow=False
    )
    fig.add_annotation(
        # need to use "&#36" instead of "$" because multiple "$"s autoformats the string as LaTex
        text=str('Current Market Cap &#36;'+str(mc)+', FDV &#36;'+str(fdv)).replace('$','&#36;'),
        font=dict(size=24),
        xref='paper', yref='paper',
        xanchor='right',
        x=.99, y=1.06,
        showarrow=False
    )
    fig.add_annotation(
        text='generated at https://discord.gg/dreamcrypto',
        font=dict(
            size=14,
            color='gray'
        ),
        xref='paper', yref='paper',
        xanchor='right',
        x=0.935, y=0.01,
        showarrow=False
    )

    # add logo
    if verbose: 
        print('charting: adding logo...')
    # this function doesn't work in vscode so using the url instead
    # logo = gcs_load_image('assets/images/whale_watch_logo_cropped.png')
    fig.add_layout_image(
        dict(
            source='assets/images/whale_watch_logo_cropped.png',
            xref='paper', yref='paper',
            xanchor="left", yanchor="top",
            x=-0.06, y=1.11,
            sizex=0.1, sizey=0.1
        )
    )

    # Add traces
    if verbose: 
        print('charting: adding traces...') 
    fig.add_trace(
        go.Scatter(
            x=query_df['date'],
            y=query_df['small_wallets'],
            name=f'Small Wallets (<{human_format(shrimp_threshold_tokens)} {symbol} (${human_format(shrimp_threshold_usd)} USD today)'.replace('$','&#36;'),
            line=dict(
                color='#a9a9a9',
                width=2
            )
        ),
        secondary_y=False
    )

    fig.add_trace(
        go.Scatter(
            x=query_df['date'],
            y=query_df['medium_wallets'],
            name=f'Medium Wallets (<{human_format(whale_threshold_tokens)} {symbol} (${human_format(whale_threshold_usd)} USD today)'.replace('$','&#36;'),
            line=dict(
                color='#71368A',
                width=4
            )
        ),
        secondary_y=False
    )

    fig.add_trace(
        go.Scatter(
            x=query_df['date'],
            y=query_df['whale_wallets'],
            name=f'Whale Wallets (>={human_format(whale_threshold_tokens)} {symbol} (${human_format(whale_threshold_usd)} USD today)'.replace('$','&#36;'),
            line=dict(
                color='#00FFFF',
                width=6
            )
        ),
        secondary_y=True
    )

    # x-axis settings
    if verbose:
        print('charting: defining axis settings...')
    fig.update_xaxes(
        # title_text="Days of History: "+str(days_of_history),
        # title_font=dict(size=24),
        tickfont=dict(size=20),
        showgrid=True,
        gridcolor='#212530',
        showticklabels=True,
        gridwidth=2,
        linecolor='white',
        linewidth=2
    )

    # y axis: whale wallets
    max = query_df['whale_wallets'].max()
    min = query_df['whale_wallets'].min()/2
    range = max-min
    buffered_max = max+range*0.2
    buffered_min = min-range*0.1

    fig.update_yaxes(
        title_text='Whale Wallet Count',
        title_font=dict(size=24),
        tickfont=dict(size=20),
        range=[buffered_min,buffered_max],
        scaleanchor='y2' ,
        secondary_y=True,
        scaleratio=1,
        constraintoward='bottom',
        linecolor='#00FFFF',
        linewidth=3,
        showticklabels=True,
        tickformat=',.2s',
        showgrid=True,
        gridcolor='#212530',
        gridwidth=2
    )

    # y axis: s/m wallet
    fig.update_yaxes(
        title_text='S/M Wallet Count',
        title_font=dict(size=24),
        tickfont=dict(size=20),
        rangemode='normal',
        scaleanchor='y',
        scaleratio=1,
        constraintoward='bottom',
        linecolor='#71368A',
        linewidth=2,
        secondary_y=False,
        showgrid=False,
        showticklabels=True,
        tickformat=',.2s'
    )

    # Legend settings
    fig.update_layout(
        legend=dict(
            yanchor='top',
            y=0.99,
            xanchor='left',
            x=0.02,
            traceorder='reversed',
            font=dict(
                size=18
                ,family='Arial'
            ),
            bgcolor='rgba(0,0,0,.5)'
        )
    )

    # TODO: this step has the most performance issues out of the charting 
    # process. if this can be done in a way that doesn't save/load multiple 
    # files it will improve overall command response times
    if verbose: 
        print('charting: adding border...')
        
    # duct tape method to apply a green border to the image
    fig.write_image('whale_chart_temp.png',engine='kaleido')
    whale_chart = Image.open('whale_chart_temp.png')
    # whale_chart = whale_chart.crop((0,0,500,0))
    whale_chart = ImageOps.expand(whale_chart,border=8,fill='#4da64c')
    whale_chart.save('whale_chart_temp.png')
    if verbose:
        print('generated whale_chart_temp.png.')

    return(whale_chart)


def log_whale_chart_request(
        submitted_by
        ,blockchain_name
        ,contract_address
        ,whale_threshold_usd
        ,days_of_history
        ,api_response_code
        ,function_result_summary
        ,function_result_detail
        ,processing_time
        ,dune_total_time
        ,dune_execution_time
        ,request_json
        ,verbose=False
    ):
    '''
    logs all variables from the function
    params: all inputs and outputs
    param: processing_time <int> the seconds it took for the full cloud function to run
    param: dune_total_time <int> the seconds it took for dune to go through all api states
    param: dune_execution_time <int> the seconds it took for dune go through the QUERY_EXECUTING state
    param: request_json <json> the raw json input
    '''
    client = bigquery.Client()
    table_id = 'western-verve-411004.etl_pipelines.logs_whale_charts'

    rows_to_insert = [{
        'created_at': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ,'submitted_by': submitted_by
        ,'blockchain': blockchain_name
        ,'address': contract_address
        ,'whale_threshold_usd': whale_threshold_usd
        ,'days_of_history': days_of_history
        ,'api_response_code': api_response_code
        ,'function_result_summary': function_result_summary
        ,'function_result_detail': function_result_detail
        ,'processing_time': processing_time
        ,'request_json': json.dumps(request_json)
        ,'dune_total_time': dune_total_time
        ,'dune_execution_time': dune_execution_time
    }]

    errors = client.insert_rows_json(table_id, rows_to_insert)
    if verbose:
        if errors == []:
            print('new row added to '+table_id)
        else:
            print('Encountered errors while inserting rows: {}'.format(errors))


def whales_chart_wrapper(
        coingecko_api_key
        ,blockchain_name
        ,contract_address
        ,days_of_history
        ,whale_threshold_usd
        ,whale_threshold_tokens
        ,verbose=False
    ):
    '''
    wrapper that runs all functions necessary to return the whale chart image
    param: coingecko_api_key <string> retreived secret
    param: blockchain_name <string> user input blockchain
    param: contract_address <string> user input address
    param: days_of_history <int> user input days of history to query in dune
    param: whale_threshold_usd <float> user input whale threshold usd
    param: whale_threshold_tokens <float> user input whale threshold tokens
    return: api_response_code <int> outcome of full GCF function (can be 200 or 400)
    return: function_result_summary <string> outcome of the function in plaintext
    return: function_result_detail <string> either the whale chart url or technical error details
    return: discord_message <string> the error message that should be sent to the user
    return: dune_total_time <int> the seconds it took for dune to go through all api states
    return: dune_execution_time <int> the seconds it took for dune go through the QUERY_EXECUTING state
    '''
    # declare dune duration variables in case attempt never reaches dune stage
    dune_execution_time = 0
    dune_total_time = 0

    ### INPUT VALIDITY CHECKS ###
    # check if blockchain alias is valid
    chain_id,chain_text_dune,chain_text_coingecko,chain_text_geckoterminal,match_outcome = lookup_chain_ids(blockchain_name,verbose=verbose)
    if match_outcome != 'success':
        # API CODE 400: errors from blockchain alias match
        api_response_code = 400
        function_result_summary = 'blockchain match error'
        function_result_detail = match_outcome
        if match_outcome == 'unsupported chain':
            discord_message = 'Blockchain "'+blockchain_name+'" was found but is not supported by Whale Watch. Supported chains include Arbitrum, Avalanche, Binance, Base, Celo, Ethereum, Fantom, Gnosis, Optimism, Polygon, Scroll, Solana, Zora, zkSync. Most common chain aliases are supported.'
        else:
            discord_message = 'Blockchain "'+blockchain_name+'" could not be found in database. Supported chains include Arbitrum, Avalanche, Binance, Base, Celo, Ethereum, Fantom, Gnosis, Optimism, Polygon, Scroll, Solana, Zora, zkSync. Most common chain aliases are supported.'
        return(api_response_code,function_result_summary,function_result_detail,discord_message,dune_execution_time,dune_total_time)

    # check if days of history is valid
    if not 1 < days_of_history < 2000:
        # API CODE 400: invalid days_of_history
        api_response_code = 400
        function_result_summary = 'invalid days of history'
        function_result_detail = 'invalid days of history input value: '+str(days_of_history)
        discord_message = 'Days of history must be between 2 and 2000. (input value: '+str(days_of_history)+')'
        return(api_response_code,function_result_summary,function_result_detail,discord_message,dune_execution_time,dune_total_time)


    # # TODO: threshold validation logic needs to incorporate whale_threshold_tokens
    # # check if whale_threshold_usd is valid
    # if not whale_threshold_usd > 0:
    #     # API CODE 400: invalid whale_threshold
    #     api_response_code = 400
    #     function_result_summary = 'invalid whale threshold'
    #     function_result_detail = 'Whale threshold must be a number > 0. (input value: '+str(whale_threshold_usd)+')'
    #     discord_message = function_result_detail
    #     return(api_response_code,function_result_summary,function_result_detail,discord_message,dune_execution_time,dune_total_time)



    ### GETTING TOKEN METADATA ###
    # attempt coingecko search
    # the try/except logic is included because some coins have arbitrarily different api response data structure 
    # that breaks the code, e.g. '0x39142c18b6db2a8a41b7018f49e1478837560cad' on 'eth'
    try:
        coingecko_status_code,token_dict = coingecko_metadata_search(
            coingecko_api_key
            ,chain_text_coingecko
            ,contract_address
            ,verbose
        )
    except:
        coingecko_status_code = 400
    if coingecko_status_code != 200:

        # attempt geckoterminal search
        geckoterminal_status_code,token_dict = geckoterminal_metadata_search(
            chain_text_geckoterminal
            ,contract_address
            ,verbose=verbose
        )
        if geckoterminal_status_code != 200:

            # API CODE 404: couldn't find in either
            api_response_code = 404
            function_result_summary = 'token metadata search error'
            function_result_detail = f'coingecko result:{str(coingecko_status_code)}, geckoterminal result:{str(geckoterminal_status_code)}'
            discord_message = 'Metadata for '+blockchain_name+' contract '+contract_address+' could not be found on Coingecko or Geckoterminal. Make sure token has at least 2 days of history if requesting a chart.'
            return(api_response_code,function_result_summary,function_result_detail,discord_message,dune_execution_time,dune_total_time)



    ### WHALE THRESHOLD CALCULATION AND ADJUSTMENTS ###
    # calculate usd threshold if tokens were input
    if whale_threshold_tokens:
        whale_threshold_usd = whale_threshold_tokens*token_dict['price']

    # add whale ceiling of 1% FDV since microcap memes need tiny numbers
    if whale_threshold_usd > (0.01*token_dict['fdv']):
        whale_threshold_usd = .01*token_dict['fdv']
        if verbose:
            print(f'reducing whale threshold usd to {whale_threshold_usd}')
    
    # calculate (or restate) whale token threshold
    whale_threshold_tokens = whale_threshold_usd/token_dict['price']

    # calculate shrimp token threshold which has a max of $1000 USD. wallets with less than this
    # are counted as small
    if whale_threshold_usd > 20000: 
        shrimp_threshold_usd = 1000
    else:
        shrimp_threshold_usd = whale_threshold_usd/20
    shrimp_threshold_tokens = shrimp_threshold_usd/whale_threshold_usd



        ### GETTING DUNE QUERY DATA ###
    # retrieve token transfer data from dune
    transfers_df = dune_get_token_transfers(
            chain_text_dune,
            contract_address,
            decimals=token_dict['decimals']
        )

    # upload token transfer data to bigquery if it doesn't already exist
    upload_transfers_to_bigquery(
            transfers_df,
            chain_text_dune,
            contract_address
        )

    # convert transfer data into daily counts of wallets by size
    whales_df = get_whale_counts_from_transfers(transfers_df, whale_threshold_tokens, shrimp_threshold_tokens)
    api_response_code = 200

    # API CODE 400: insufficient dune history
    if whales_df.shape[0]<2:
        api_response_code = 400
        function_result_summary = 'insufficient dune data'
        function_result_detail = f'dune output shows {str(whales_df.shape[0])} days of history'
        discord_message = 'Dune shows less than 2 days of history for this token. Tokens must have 2+ days of history for a chart to generate.'
        if verbose:
            print(function_result_detail)


    ### DRAWING THE WHALES CHART ###
    # generate the chart if dune query was successful
    if api_response_code == 200:
        # make the chart
        if verbose:
            print('drawing chart...')
        whale_chart = draw_whale_chart(
            whales_df
            ,whale_threshold_usd
            ,whale_threshold_tokens
            ,shrimp_threshold_usd
            ,shrimp_threshold_tokens
            ,days_of_history
            ,token_dict
            ,verbose=verbose
            )

        # storing image in gcs
        if verbose:
            print('storing chart in gcs...')
        local_file = 'whale_chart_temp.png'
        gcs_folder = 'whale_charts/'
        gcs_filename = str(
            'whale_chart_'
            + blockchain_name + '_'
            + contract_address + '_'
            + datetime.datetime.now().strftime('%Y%m%d_%Hh_%Mm_%Ss')
            + '.png')

        # API CODE 200: success
        api_response_code = 200
        function_result_summary = 'success'
        function_result_detail = gcs_upload_file(local_file,gcs_folder,gcs_filename,delete_local_file=True)
        discord_message = 'Successfully generated whale chart for '+token_dict['name']
        if token_dict['source']=='coingecko':
             discord_message = str(discord_message+' (https://www.coingecko.com/en/coins/'+token_dict['source_id']+')')
        if verbose:
            print('chart successfully generated.')

    return(api_response_code,function_result_summary,function_result_detail,discord_message,dune_total_time,dune_execution_time)


import functions_framework
@functions_framework.http
def request_whales_chart(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        function_result_summary <string> the high level outcome of the function
        function_result_detail <string> the detailed outcome of the function
        discord_message <string> the message dreambot will send to the user
        api_response_code <int> the api code of the function run
    """

    # ########## CLOUD FUNCTIONS VARIABLES ##############
    # # turn these on when the code is run as a cloud function
    # os.environ['DUNE_API_KEY'] = os.environ.get('dune_api_key')
    # coingecko_api_key = os.environ.get('coingecko_api_key')
    # request_json = request.get_json(silent=True)
    # verbose=False
 

    ########## NOTEBOOK DEV VARIABLES ##############
    # turn these on when the code is run in a notebook

    load_dotenv()  # Load environment variables from .env file at the start of your application
    os.environ['DUNE_API_KEY'] = get_secret('apikey_dune_whale_watch')
    coingecko_api_key = get_secret('apikey_coingecko_tentabs_free')
    request_json = {
        # 'blockchain': 'solana'
        'blockchain': 'ethereum'
        # 'blockchain': 'avalanche'

        # ,'address': '947tEoG318GUmyjVYhraNRvWpMX7fpBTDQFBoJpipvSkSG3' # solchat sol
        # ,'address': 'AVLhahDcDQ4m4vHM4ug63oh7xc8Jtk49Dm5hoe9Sazqr' # solama sol
        # # ,'address': 'EJPtJEDogxzDbvM8qvAsqYbLmPj5n1vQeqoAzj9Yfv3q' # bozo sol
        # ,'address': 'B5LMXiuvbB5jN3auECUtdfyeFWm27krgFinrBrqJGFRM' #cern sol
        ,'address': '0x6982508145454ce325ddbe47a25d4ec3d2311933' # pepe eth
        # ,'address': '0x88f89be3e9b1dc1c5f208696fb9cabfcc684bd5f' # something avax

        # ,'address': 'yyx8fCqokZoTN6mhw9MvFT5m57bwvSZUxbCA1THdHnB' # open input
        
        ,'whale_threshold_usd': 30000
        # ,'whale_threshold_tokens': 1000000
        # ,'days_of_history': 90
    }
    request_json['submitted_by'] = 'dev'
    verbose=True


    ### USER VARIABLE PARSING
    '''
    # dreambot input validation rules
        blockchain: string
        address: string 
        whale_threshold_usd: int
        whale_threshold_tokens: int
        days_of_history: int
    '''
    # blockchain and address
    blockchain_name = request_json['blockchain']
    contract_address = request_json['address']
    print(f'starting whale watch process for <{blockchain_name}:{contract_address}>...')
    start_time = time.time()

    # days of history
    try:
        days_of_history = request_json['days_of_history']
    except:
        days_of_history = 90
    if not days_of_history:
        days_of_history = 90

    # token threshold
    try:
        whale_threshold_tokens = request_json['whale_threshold_tokens']
        whale_threshold_usd = None
    except:
        whale_threshold_tokens = None

    # usd threshold (only if token threshold isn't set)
    if not whale_threshold_tokens:
        try:
            whale_threshold_usd = request_json['whale_threshold_usd']
        except:
            whale_threshold_usd = 25000
        if not whale_threshold_usd:
            whale_threshold_usd = 25000

    # user id
    try:
        submitted_by = request_json['submitted_by']
    except:
        submitted_by = 'unknown'

    # run whale chart function
    api_response_code,function_result_summary,function_result_detail,discord_message,dune_total_time,dune_execution_time = whales_chart_wrapper(
        coingecko_api_key
        ,blockchain_name
        ,contract_address
        ,days_of_history
        ,whale_threshold_usd
        ,whale_threshold_tokens
        ,verbose
    )

    # log function performance
    end_time = time.time()
    processing_time = end_time - start_time
    discord_message = discord_message + ' ['+str(round(processing_time))+' seconds]'
    log_whale_chart_request(
        submitted_by
        ,blockchain_name
        ,contract_address
        ,whale_threshold_usd
        ,days_of_history
        ,api_response_code
        ,function_result_summary
        ,function_result_detail
        ,processing_time
        ,dune_total_time
        ,dune_execution_time
        ,request_json
        ,verbose
    )
    print(f'whale watch finished after {str(round(processing_time))}s ({round(dune_total_time)}s querying): <{str(api_response_code)}: {function_result_summary}: {function_result_detail}>')

return ([function_result_summary,function_result_detail,discord_message],api_response_code)