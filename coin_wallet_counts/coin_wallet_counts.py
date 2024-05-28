'''
calculates metrics related to the distribution of coin ownership across wallets
'''

import logging
import time
import pandas as pd
import numpy as np
from dreams_core.googlecloud import GoogleCloud as dgc



def prepare_datasets():
    '''
    retrieves the dfs necessary for wallet metric calculation

    returns:
        metadata_df (df): includes metadata about each coin such as total supply
        all_balances_df (df): includes the daily wallet data necessary to calculate relevant metrics

    '''
    # logging setup
    logger = logging.getLogger(__name__)
    logger.addHandler(logging.NullHandler())
    start_time = time.time()
    logger.info('Retrieving datasets required for wallet balance metrics...')

    # sql queries
    metadata_sql = '''
        select c.coin_id
        ,chain_id
        ,c.address as token_address
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
        '''

    # run sql queries
    metadata_df = dgc().run_sql(metadata_sql)
    all_balances_df = dgc().run_sql(balances_sql)
    
    logger.info('Wallet balance datasets retrieved after %.2f seconds.', time.time() - start_time)

    return metadata_df,all_balances_df



def generate_wallet_bins(total_supply):
    '''
    defines bins for wallet balances based on what percent of total supply they own

    params:
        total_supply (float): total supply of the coin

    returns:
        wallet_bins (list of floats): the number of tokens a wallet needs to be included in a bin
        wallet_labels (list of strings): the label for each bin
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



def calculate_wallet_counts(balances_df):
    '''
    Consolidates wallet transactions into a daily count of wallets that control a certain \
        percentage of total supply

    params:
        balances_df (dataframe): df showing daily wallet balances of a coin_id token that \
            has been filtered to only include one coin_id. 

    returns:
        wallets_df (dataframe): df of wallet counts based on percent of total supply
    '''
    logger = logging.getLogger(__name__)
    logger.addHandler(logging.NullHandler())

    # Calculate total supply and generate wallet bins
    total_supply = balances_df.iloc[0]['total_supply']
    wallet_bins, wallet_labels = generate_wallet_bins(total_supply)

    logger.info('Calculating daily balances for each wallet...')
    start_time = time.time()

    # Forward fill balances to ensure each date has the latest balance for each wallet
    balances_df = balances_df.sort_values(by=['wallet_address', 'date'])
    balances_df['balance'] = balances_df.groupby('wallet_address')['balance'].ffill()

    # Classify each balance into ownership percentage bins
    balances_df['wallet_types'] = pd.cut(balances_df['balance'], bins=wallet_bins, labels=wallet_labels)

    logger.debug('Duration to classify by size: %s.2f seconds', time.time() - start_time)
    step_time = time.time()

    # Group by date and wallet type, then count the number of occurrences
    wallets_df = balances_df.groupby(['date', 'wallet_types'], observed=False).size().unstack(fill_value=0)

    # Add rows for dates with 0 transactions
    date_range = pd.date_range(start=wallets_df.index.min(), end=wallets_df.index.max(), freq='D')
    wallets_df = wallets_df.reindex(date_range, fill_value=0)

    logger.debug('Duration to aggregate daily wallet counts: %.2f seconds', time.time() - step_time)
    logger.info('Daily balance calculations complete. Total processing time: %.2f seconds', time.time() - start_time)

    return wallets_df



def calculate_buyer_counts(balances_df):
    '''
    computes the number of first time buyers and repeat buyers on each day. on the date that \
        a wallet makes its first purchase of the token, it will add 1 ato the 'buyers_new' \
        column, and on days where they make any subsequent purchases, it will add 1 to the \
        'buyers_repeat' column. 

    balances_df (dataframe): df showing daily wallet balances of a coin_id token that \
        has been filtered to only include one coin_id. 
    
    returns:
        buyers_df (dataframe): df of the number of new and repeat buyers on each date
    '''
    # Ensure 'date' column is of datetime type
    balances_df['date'] = pd.to_datetime(balances_df['date'])
    
    # Group by 'date' and calculate the counts
    buyers_df = balances_df.groupby('date').agg(
        buyers_new = ('buy_sequence', lambda x: (x == 1).sum()),
        buyers_repeat = ('buy_sequence', lambda x: (x != 1).sum())
    ).reset_index()

    # Set 'date' as the index
    buyers_df.set_index('date', inplace=True)

    return buyers_df
