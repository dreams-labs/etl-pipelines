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


@functions_framework.http
def update_core_coin_wallet_transfers(request):
    '''
    runs all functions in sequence to refresh core.coin_market_data
    '''
    # update list of addresses to be excluded from the core table
    update_exclusions_table()

    # insert new coingecko market data records to core.coin_market_data
    logger.info(f'rebuilding table core.coin_wallet_transfers...')
    counts_df = rebuild_core_coin_wallet_transfers()

    # log job summary
    core_count = counts_df[counts_df['table']=='core.coin_wallet_transfers']['records'].iloc[0]
    etl_count = counts_df[counts_df['table']=='etl_pipelines.coin_wallet_net_transfers']['records'].iloc[0]
    logger.info('rebuilt core.coin_wallet_transfers [%s rows] from etl_pipelines.coin_wallet_net_transfers [%s rows].',core_count,etl_count)

    return f'{{"rebuild of core.coin_wallet_transfers complete."}}'



def update_exclusions_table():
    '''
    refreshes the etl_pipelines.core_coin_wallet_transfers_exclusions bigquery table by ingesting 
    the underlying sheets data, formatting it, and uploading it to bigquery

    exclusions are maintained in the 'core_coin_wallet_transfers_exclusions' tab of this google sheet:
    https://docs.google.com/spreadsheets/d/11Mi1a3SeprY_GU_QGUr_srtd7ry2UrYwoaRImSACjJs/edit?gid=388901135#gid=388901135
    '''
    # load the tab into a df
    df = dgc().read_google_sheet('11Mi1a3SeprY_GU_QGUr_srtd7ry2UrYwoaRImSACjJs','core_coin_wallet_transfers_exclusions!A:E')

    # format and upload df
    df['created_date'] = pd.to_datetime(df['created_date'])
    df['updated_at'] = datetime.datetime.now(utc)
    dgc().upload_df_to_bigquery(
        df,
        'etl_pipelines',
        'core_coin_wallet_transfers_exclusions',
        if_exists='replace'
    )



def rebuild_core_coin_wallet_transfers():
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
            left join (
                select e.chain_text_source
                ,case 
                    when ch.is_case_sensitive=False then lower(e.wallet_address)
                    else e.wallet_address
                    end as wallet_address
                from `etl_pipelines.core_coin_wallet_transfers_exclusions` e
                join `core.chains` ch on ch.chain_text_dune = e.chain_text_source
            ) exclusions on exclusions.wallet_address = wnt.wallet_address
                and exclusions.chain_text_source = wnt.chain_text_source

            -- remove dune's various representations of burn/mint addresses
            where wnt.wallet_address <> 'None' -- removes burn/mint address for solana
            and wnt.wallet_address <> '0x0000000000000000000000000000000000000000' -- removes burn/mint addresses
            and wnt.wallet_address <> wnt.token_address 
            and wnt.wallet_address <> '<nil>'

            -- remove manually excluded addresses
            and exclusions.wallet_address is null
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
