import pandas as pd
import time
import datetime
import math
import requests
from urllib import parse
from google.cloud import storage
pd.options.mode.chained_assignment = None  # default='warn'

def request_chains_tvl():
    """
    pull data from the /v2/chains endpoint
    (see https://defillama.com/docs/api)

    return: chains_tvl_api_df <dataframe> raw df of all api fields
    """

    response = requests.get('https://api.llama.fi/v2/chains')
    if response.status_code == 200:
        data = response.json()
        chains_tvl_api_df = pd.DataFrame(data)
        print("retrieved API data.")
    else:
        print('Error: API call failed with status code', response.status_code)

    return(chains_tvl_api_df)

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

def defillama_chains_tvl(data,context):
    '''
    the wrapper function for google cloud function deployment
    '''
    chains_tvl_api_df = request_chains_tvl()
    df_to_gcs(
        chains_tvl_api_df
        ,'data_lake/defillama_chains_tvl/'
        ,'defillama_chains_tvl')

# hopefully this lets the pub/sub trigger work and also tests ok
try:
    data
except: 
    data = 'test_data'
try:
    context
except: 
    context = 'test_context'

defillama_chains_tvl(data, context)
