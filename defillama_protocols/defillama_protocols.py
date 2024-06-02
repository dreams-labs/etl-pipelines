import pandas as pd
import time
import datetime
import math
import requests
from urllib import parse
from google.cloud import storage
pd.options.mode.chained_assignment = None  # default='warn'

def prepare_analyze_df():
    """
    pull metrics on all protocols via the defillama protocols endpoint
    (see https://defillama.com/docs/api)

    return: protocols_api_df <dataframe> raw df of all protocol fields
    return: analyze_df <dataframe> df of selected columns
    """

    response = requests.get('https://api.llama.fi/protocols')
    if response.status_code == 200:
        data = response.json()
        protocols_api_df = pd.DataFrame(data)
        print("retrieved protocols data.")
    else:
        print('Error: API call failed with status code', response.status_code)

    # prepare analyze_df
    analyze_df = protocols_api_df[[
        'name'
        ,'address'
        ,'category'
        ,'symbol'
        ,'tvl'
        ]]
    analyze_df['primary_chain'] = protocols_api_df['chains'].str[0]

    # remove rows with empty 'address' values or 'tvl' of 0
    analyze_df = analyze_df[(analyze_df['address'] != '') & (analyze_df['tvl'] != 0)].dropna(subset=['address', 'tvl'])

    # remove addresses that are like "(network):-"
    analyze_df = analyze_df[~analyze_df['address'].str.endswith(':-')]

    # create a new column 'chain_category_rank' that displays a number for each row that displays the row's rank within each `category`,`primary_chain` desc
    analyze_df['chain_category_rank'] = analyze_df.groupby(['category', 'primary_chain'])['tvl'].rank(ascending=False).astype(int)

    # imputing the 'ethereum:' prefix if there isn't an existing network
    analyze_df['address_has_colon'] = analyze_df['address'].str.contains(':')
    analyze_df['address_fixed'] = analyze_df['address'].where(analyze_df['address_has_colon'])
    analyze_df['address_fixed'] = analyze_df['address_fixed'].fillna('ethereum:' + analyze_df['address'])
    analyze_df.drop('address_has_colon', axis=1, inplace=True)

    # remove rows from analyze_df where the 'address_fixed' string is shorter than 15 characters long
    analyze_df = analyze_df[analyze_df['address_fixed'].str.len() >= 15]

    print('prepared analyze_df.')

    return(protocols_api_df,analyze_df)

def df_to_gcs(
        df
        ,filepath
        ,filename
    ):
    '''
    uploads a df to google cloud storage
    param: df <dataframe> the dataframe to be uploaded
    param: filepath <string> the filepath to the folder the file will be put into
    param: filename <string> the complete filename including ".csv"
    '''
    current_date_string = datetime.datetime.now().strftime('%Y%m%d')
    filename_full = filename + '_' + current_date_string + '.csv'

    client = storage.Client(project='dreams-labs-data')
    bucket = client.get_bucket('dreams-labs-storage')

    blob = bucket.blob(filepath + filename_full)
    blob.upload_from_string(df.to_csv(index = False),content_type = 'csv')

    print('Uploaded '+filepath+filename_full)

def defillama_protocols(data,context):
    '''
    the wrapper function for google cloud function deployment
    '''
    protocols_api_df,analyze_df = prepare_analyze_df()
    df_to_gcs(
        protocols_api_df
        ,'data_lake/defillama_protocols/'
        ,'defillama_protocols')

# hopefully this lets the pub/sub trigger work and also tests ok
try:
    data
except: 
    data = 'test_data'
try:
    context
except: 
    context = 'test_context'

defillama_protocols(data, context)