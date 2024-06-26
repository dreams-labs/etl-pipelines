'''
calculates metrics related to the distribution of coin ownership across wallets
'''
# pylint: disable=C0301

import logging
import time
from datetime import datetime
from pytz import utc
import pandas as pd
import numpy as np
from dreams_core.googlecloud import GoogleCloud as dgc
import functions_framework
import pandas_gbq


def configure_logger():
    '''
    retrieves an existing logger if one exists, otherwise creates a new silent logger object

    returns:
    - logger (logging object): a logger object that either retains existing logging settings or \
        creates a new silent logger
    '''
    logger = logging.getLogger(__name__)
    if not logger.handlers:
        handler = logging.NullHandler()
        logger.addHandler(handler)

    return logger


def prepare_datasets():
    '''
    retrieves the dfs necessary for wallet metric calculation

    returns:
    - metadata_df (df): includes metadata about each coin such as total supply
    - all_balances_df (df): includes the daily wallet data necessary to calculate relevant metrics

    '''
    logger = configure_logger()
    start_time = time.time()
    logger.debug('Retrieving datasets required for wallet balance metrics...')

    # sql queries
    metadata_sql = '''
        select c.coin_id
        ,chain_id
        ,c.address as token_address
        ,c.symbol
        ,cf.total_supply
        from `core.coins` c
        join `core.coin_facts_coingecko` cf on cf.coin_id = c.coin_id
        '''

    balances_sql = '''
        select wt.coin_id
        ,wt.wallet_address
        ,wt.date
        ,wt.balance as balance
        ,case when wt.net_transfers > 0 then wt.transfer_sequence end as buy_sequence
        from `core.coin_wallet_transfers` wt
        where wallet_address <> '0x0000000000000000000000000000000000000000'
        '''

    # run sql queries
    metadata_df = dgc().run_sql(metadata_sql)
    all_balances_df = dgc().run_sql(balances_sql)

    logger.debug('Wallet balance datasets retrieved after %.2f seconds.', time.time() - start_time)

    return metadata_df,all_balances_df



def generate_wallet_bins(total_supply):
    '''
    defines bins for wallet balances based on what percent of total supply they own

    params:
    - total_supply (float): total supply of the coin

    returns:
    - wallet_bins (list of floats): the number of tokens a wallet needs to be included in a bin
    - wallet_labels (list of strings): the label for each bin
    '''

    # defining bin boundaries
    percentages = [
        0.0000010,
        0.0000018,
        0.0000032,
        0.0000056,
        0.0000100,
        0.0000180,
        0.0000320,
        0.0000560,
        0.0001000,
        0.0001800,
        0.0003200,
        0.0005600,
        0.0010000,
        0.0018000,
        0.0032000,
        0.0056000,
        0.0100000
    ]

    wallet_bins = [total_supply * pct for pct in percentages] + [np.inf]

    # defining labels, 0s are handled nonstandardly so they aren't done as a loop
    wallet_labels = [
        'wallets_0p00010_pct',
        'wallets_0p00018_pct',
        'wallets_0p00032_pct',
        'wallets_0p00056_pct',
        'wallets_0p0010_pct',
        'wallets_0p0018_pct',
        'wallets_0p0032_pct',
        'wallets_0p0056_pct',
        'wallets_0p010_pct',
        'wallets_0p018_pct',
        'wallets_0p032_pct',
        'wallets_0p056_pct',
        'wallets_0p10_pct',
        'wallets_0p18_pct',
        'wallets_0p32_pct',
        'wallets_0p56_pct',
        'wallets_1p0_pct'
    ]

    return wallet_bins, wallet_labels



def calculate_wallet_counts(balances_df,total_supply):
    '''
    Consolidates wallet transactions into a daily count of wallets that control a certain \
        percentage of total supply

    params:
    - balances_df (dataframe): df showing daily wallet balances of a coin_id token that \
        has been filtered to only include one coin_id. 
    - total_supply (float): the total supply of the coin

    returns:
    - wallets_df (dataframe): df of wallet counts based on percent of total supply
    '''
    logger = configure_logger()
    start_time = time.time()

    # Calculate total supply and generate wallet bins
    wallet_bins, wallet_labels = generate_wallet_bins(total_supply)

    logger.debug('Calculating daily balances for each wallet...')
    start_time = time.time()

    # Forward fill balances to ensure each date has the latest balance for each wallet
    balances_df = balances_df.sort_values(by=['wallet_address', 'date'])
    balances_df['balance'] = balances_df.groupby('wallet_address')['balance'].ffill()

    # Classify each balance into ownership percentage bins
    balances_df['wallet_types'] = pd.cut(balances_df['balance'], bins=wallet_bins, labels=wallet_labels)

    # Group by date and wallet type, then count the number of occurrences
    wallets_df = balances_df.groupby(['date', 'wallet_types'], observed=False).size().unstack(fill_value=0)

    # Add rows for dates with 0 transactions
    date_range = pd.date_range(start=wallets_df.index.min(), end=wallets_df.index.max(), freq='D')
    wallets_df = wallets_df.reindex(date_range, fill_value=0)

    # Fill empty cells with 0s
    wallets_df.fillna(0, inplace=True)

    logger.debug('Daily balance calculations complete after %.2f seconds', time.time() - start_time)

    return wallets_df



def calculate_buyer_counts(balances_df):
    '''
    computes the number of first time buyers and repeat buyers on each day. on the date that \
        a wallet makes its first purchase of the token, it will add 1 ato the 'buyers_new' \
        column, and on days where they make any subsequent purchases, it will add 1 to the \
        'buyers_repeat' column. 

    params:
    - balances_df (dataframe): df showing daily wallet balances of a coin_id token that \
        has been filtered to only include one coin_id. 
    
    returns:
    - buyers_df (dataframe): df of the number of new and repeat buyers on each date
    '''
    logger = configure_logger()
    start_time = time.time()

    # Ensure 'date' column is of datetime type
    balances_df['date'] = pd.to_datetime(balances_df['date'])

    # Group by 'date' and calculate the counts
    buyers_df = balances_df.groupby('date').agg(
        buyers_new = ('buy_sequence', lambda x: (x == 1).sum()),
        buyers_repeat = ('buy_sequence', lambda x: (x != 1).sum())
    ).reset_index()

    # Set 'date' as the index
    buyers_df.set_index('date', inplace=True)

    # Fill empty cells with 0s
    buyers_df.fillna(0, inplace=True)

    logger.debug('New vs repeat buyer counts complete after %.2f seconds', time.time() - start_time)

    return buyers_df


def calculate_daily_gini(balances_df):
    '''
    Calculates the Gini coefficient for the distribution of wallet balances for each date.

    params:
    - balances_df (dataframe): df showing daily wallet balances of a coin_id token that \
        has been filtered to only include one coin_id. 

    returns:
    - gini_df (dataframe): df with dates as the index and the Gini coefficients as the values.
    '''
    logger = configure_logger()
    start_time = time.time()

    # Get the most recent balance for each wallet each day
    balances_df = balances_df.sort_values(by=['wallet_address', 'date'])
    daily_balances = balances_df.drop_duplicates(subset=['wallet_address', 'date'], keep='last')

    def efficient_gini(arr):
        arr = np.sort(arr)
        n = len(arr)
        if n == 0:
            return None
        index = np.arange(1, n + 1)
        return (2 * np.sum(index * arr) - (n + 1) * np.sum(arr)) / (n * np.sum(arr))

    # Calculate Gini coefficient for each day
    gini_coefficients = daily_balances.groupby('date')['balance'].apply(efficient_gini)

    # Convert the Series to a DataFrame for better readability
    gini_df = gini_coefficients.reset_index(name='gini_coefficient')
    gini_df.set_index('date', inplace=True)

    logger.debug('Daily gini coefficients complete after %.2f seconds', time.time() - start_time)
    return gini_df



def calculate_gini_without_mega_whales(balances_df, total_supply):
    '''
    computes the gini coefficient while ignoring wallets that have ever held >5% of \
        the total supply. the hope is that this removes treasury accounts, burn addresses, \
        and other wallets that are not likely to be wallet owners. 


    params:
    - balances_df (dataframe): df showing daily wallet balances of a coin_id token that \
        has been filtered to only include one coin_id. 
    - total_supply (float): the total supply of the coin

    returns:
    - gini_filtered_df (dataframe): df of gini coefficient without mega whales
    '''
    # filter out addresses that have ever owned 5% or more supply
    mega_whales = balances_df[balances_df['balance']>=(total_supply*0.05)]['wallet_address'].unique()
    balances_df_filtered = balances_df[~balances_df['wallet_address'].isin(mega_whales)]

    # calculate gini
    gini_filtered_df = calculate_daily_gini(balances_df_filtered)
    gini_filtered_df.rename(columns={'gini_coefficient': 'gini_coefficient_excl_mega_whales'}, inplace=True)

    return gini_filtered_df



def calculate_coin_metrics(metadata_df,balances_df):
    '''
    Calculate various metrics for a specific cryptocurrency coin and merge them into a single df.

    Parameters:
    - all_balances_df (dataframe): Contains balance information on all dates for all coins.
    - all_metadata_df (dataframe): Contains containing metadata for all coins.
    - coin_id (str): The coin ID to calculate metrics for.

    Returns:
    - coin_metrics_df (dataframe): Contains the calculated metrics and metadata for the specified coin.
    '''
    logger = configure_logger()
    logger.info('Calculating metrics for %s', metadata_df['symbol'].iloc[0])
    total_supply = metadata_df['total_supply'].iloc[0]
    coin_id = metadata_df['coin_id'].iloc[0]

    # Calculate Metrics
    # -----------------

    # Metric 1: Wallets by Ownership
    # Shows what whale wallets and small wallets are doing
    wallets_by_ownership_df = calculate_wallet_counts(balances_df, total_supply)

    # Metric 2: Buyers New vs Repeat
    # Shows how many daily buyers are first-time vs repeat buyers
    buyers_new_vs_repeat_df = calculate_buyer_counts(balances_df)

    # Metric 3: Gini Coefficients
    # Gini coefficients based on wallet balances
    gini_df = calculate_daily_gini(balances_df)
    gini_df_excl_mega_whales = calculate_gini_without_mega_whales(balances_df, total_supply)


    # Merge All Metrics into One DataFrame
    # ------------------------------------
    metrics_dfs = [
        wallets_by_ownership_df,
        buyers_new_vs_repeat_df,
        gini_df,
        gini_df_excl_mega_whales
    ]
    coin_metrics_df = metrics_dfs[0]
    for df in metrics_dfs[1:]:
        coin_metrics_df = coin_metrics_df.join(df, how='outer')

    # reset index
    coin_metrics_df = coin_metrics_df.reset_index().rename(columns={'index': 'date'})

    # add coin_id
    coin_metrics_df['coin_id'] = coin_id

    return coin_metrics_df



def upload_coin_metrics_data(all_coin_metrics_df):
    '''
    Appends the all_coin_metrics_df to the BigQuery table etl_pipelines.coin_metrics_data. 

    Steps:
        1. Explicitly map datatypes onto new dataframe upload_df
        2. Declare schema datatypes
        3. Upload using pandas_gbq

    Params:
        all_coin_metrics_df (pandas.DataFrame): df of coin metrics data
    Returns:
        None
    '''
    logger = logging.getLogger(__name__)

    # Add metadata to upload_df
    upload_df = all_coin_metrics_df.copy()
    upload_df['updated_at'] = datetime.now(utc)

    # Localize date column
    upload_df['date'] = pd.to_datetime(upload_df['date']).dt.tz_localize(utc)

    # Set df datatypes of upload df
    dtype_mapping = {
        'date': 'datetime64[us, UTC]',
        'wallets_0p00010_pct': int,
        'wallets_0p00018_pct': int,
        'wallets_0p00032_pct': int,
        'wallets_0p00056_pct': int,
        'wallets_0p0010_pct': int,
        'wallets_0p0018_pct': int,
        'wallets_0p0032_pct': int,
        'wallets_0p0056_pct': int,
        'wallets_0p010_pct': int,
        'wallets_0p018_pct': int,
        'wallets_0p032_pct': int,
        'wallets_0p056_pct': int,
        'wallets_0p10_pct': int,
        'wallets_0p18_pct': int,
        'wallets_0p32_pct': int,
        'wallets_0p56_pct': int,
        'wallets_1p0_pct': int,
        'buyers_new': int,
        'buyers_repeat': int,
        'gini_coefficient': float,
        'gini_coefficient_excl_mega_whales': float,
        'coin_id': str,
        'updated_at': 'datetime64[us, UTC]'
    }
    upload_df = upload_df.astype(dtype_mapping)
    logger.info('Prepared upload df with %s rows.', len(upload_df))

    # Reorder columns to have coin_id first
    upload_df = upload_df[['coin_id'] + [col for col in upload_df.columns if col != 'coin_id']]

    # Upload df to BigQuery
    project_id = 'western-verve-411004'
    table_name = 'core.coin_wallet_metrics'
    schema = [
        {'name': 'coin_id', 'type': 'string'},
        {'name': 'date', 'type': 'datetime'},
        {'name': 'wallets_0p00010_pct', 'type': 'int'},
        {'name': 'wallets_0p00018_pct', 'type': 'int'},
        {'name': 'wallets_0p00032_pct', 'type': 'int'},
        {'name': 'wallets_0p00056_pct', 'type': 'int'},
        {'name': 'wallets_0p0010_pct', 'type': 'int'},
        {'name': 'wallets_0p0018_pct', 'type': 'int'},
        {'name': 'wallets_0p0032_pct', 'type': 'int'},
        {'name': 'wallets_0p0056_pct', 'type': 'int'},
        {'name': 'wallets_0p010_pct', 'type': 'int'},
        {'name': 'wallets_0p018_pct', 'type': 'int'},
        {'name': 'wallets_0p032_pct', 'type': 'int'},
        {'name': 'wallets_0p056_pct', 'type': 'int'},
        {'name': 'wallets_0p10_pct', 'type': 'int'},
        {'name': 'wallets_0p18_pct', 'type': 'int'},
        {'name': 'wallets_0p32_pct', 'type': 'int'},
        {'name': 'wallets_0p56_pct', 'type': 'int'},
        {'name': 'wallets_1p0_pct', 'type': 'int'},
        {'name': 'buyers_new', 'type': 'int'},
        {'name': 'buyers_repeat', 'type': 'int'},
        {'name': 'gini_coefficient', 'type': 'float'},
        {'name': 'gini_coefficient_excl_mega_whales', 'type': 'float'},
        {'name': 'updated_at', 'type': 'datetime'}
    ]

    pandas_gbq.to_gbq(
        upload_df,
        table_name,
        project_id=project_id,
        if_exists='replace',
        table_schema=schema,
        progress_bar=False
    )
    logger.info('Replaced data in %s.', table_name)



@functions_framework.cloud_event
def update_coin_wallet_metrics():
    '''
    runs all functions in sequence to update and upload core.coin_wallet_metrics
    '''
    # configure logger
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s',
        datefmt='%d/%b/%Y %H:%M:%S'
        )

    # retrieve full sets of metadata and daily wallet balances
    all_metadata_df,all_balances_df = prepare_datasets()

    # prepare list and df for loop iteration
    unique_coin_ids = all_balances_df['coin_id'].unique().tolist()
    all_coin_metrics_df = pd.DataFrame()

    # generate metrics for all coins
    for c in unique_coin_ids:
        # retrieve coin-specific dfs
        metadata_df = all_metadata_df[all_metadata_df['coin_id']==c].copy()
        balances_df = all_balances_df[all_balances_df['coin_id']==c].copy()

        # calculate and merge metrics
        coin_metrics_df = calculate_coin_metrics(metadata_df,balances_df)

        # fill zeros for missing dates (currently impacts buyer behavior and gini columns)
        all_coin_metrics_df.fillna(0, inplace=True)
        all_coin_metrics_df = pd.concat([all_coin_metrics_df,coin_metrics_df])

    # upload metrics to bigquery
    upload_coin_metrics_data(all_coin_metrics_df)