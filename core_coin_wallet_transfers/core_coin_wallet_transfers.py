'''
cloud function that runs a query to refresh the data in bigquery table core.coin_wallet_transfers
'''
import datetime
import time
import logging
import os
from pytz import utc
import pandas as pd
import functions_framework
from google.cloud import bigquery_storage
from dreams_core.googlecloud import GoogleCloud as dgc
from dreams_core import core as dc

# set up logger at the module level
logger = dc.setup_logger()


def rebuiild_core_coin_wallet_transfers():
    '''
    adds new records in etl_pipelines.coin_market_data_coingecko to core.coin_market_data after 
    normalizing and filling relevant fields

    returns:
        counts_df <df>: dataframe showing the number of rows in the core and etl transfers tables
    '''

    query_sql = '''
        truncate table core.coin_wallet_transfers;

        insert into core.coin_wallet_transfers (
            select c.coin_id
            ,c.chain_id
            ,wnt.token_address
            ,wnt.wallet_address
            ,wnt.date
            ,cast(wnt.daily_net_transfers as bigdecimal) as net_transfers
            ,sum(cast(daily_net_transfers as bigdecimal)) 
                over (partition by wnt.token_address,wnt.wallet_address order by wnt.date asc) as balance
            ,count(daily_net_transfers) 
                over (partition by wnt.token_address,wnt.wallet_address order by wnt.date asc) as transfer_sequence
            from core.coins c
            join core.chains ch on ch.chain_id = c.chain_id
            join etl_pipelines.coin_wallet_net_transfers wnt on wnt.token_address = c.address 
                and (wnt.chain_text_source = ch.chain_text_dune and wnt.data_source = 'dune')
        );

        select 'core.coin_wallet_transfers' as table
        ,count(*) as records
        from core.coin_wallet_transfers

        union all 

        select 'etl_pipelines.coin_wallet_net_transfers'
        ,count(*)
        from etl_pipelines.coin_wallet_net_transfers
        ;
        '''

    counts_df = dgc().run_sql(query_sql)

    return counts_df


@functions_framework.http
def update_core_coin_wallet_transfers(request):
    '''
    runs all functions in sequence to refresh core.coin_market_data
    '''
    # insert new coingecko market data records to core.coin_market_data
    logger.info(f'rebuilding table core.coin_wallet_transfers...')
    counts_df = rebuiild_core_coin_wallet_transfers()

    # log job summary
    core_count = counts_df[counts_df['table']=='core.coin_wallet_transfers']['records'].iloc[0]
    etl_count = counts_df[counts_df['table']=='etl_pipelines.coin_wallet_net_transfers']['records'].iloc[0]
    logger.info('rebuilt core.coin_wallet_transfers [%s rows] from etl_pipelines.coin_wallet_net_transfers [%s rows].',core_count,etl_count)

    return f'{{"rebuild of core.coin_wallet_transfers complete."}}'
