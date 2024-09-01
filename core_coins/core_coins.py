'''
cloud function that runs a query to refresh the data in bigquery table core.coin_market_data
'''
import datetime
import time
import logging
import os
from pytz import utc
import pandas as pd
import functions_framework
from dreams_core.googlecloud import GoogleCloud as dgc



def intake_new_community_calls():
    '''
    ingests new coins from the etl_pipelines.community_calls table into the 
    etl_pipelines.coins_intake table through the following steps:
        1. normalizes coin addresses and chains
        2. checks for duplicates and coins that have already been ingested
        3. inserts the remaining coins
    '''

    query_sql = '''
        insert into etl_pipelines.coins_intake (

        with all_calls as (
            select generate_uuid() as coin_id
            ,c.blockchain as chain_input
            ,c.address as address_input
            ,'community_calls' as source
            ,cast(c.call_date as datetime) as source_date
            ,case when ch.chain_id is not null then TRUE else FALSE end as has_valid_chain
            ,ch.chain
            ,ch.chain_id
            ,case
                when ch.is_case_sensitive = FALSE then lower(c.address)
                else c.address
                end as address
            ,current_datetime() as created_at
            from etl_pipelines.community_calls c
            left join `reference.chain_nicknames` chn on lower(chn.chain_reference) = lower(c.blockchain)
            left join core.chains ch on ch.chain_id = chn.chain_id
            where c.address is not null
        ),

        data_checks as (
            select ac.*
            ,row_number() over (partition by ac.chain_id,ac.address order by ac.source_date asc) as record_count
            ,case when c.coin_id is not null then 1 else 0 end as already_ingested
            from all_calls ac
            left join etl_pipelines.coins_intake c on c.address = ac.address and c.chain_id = ac.chain_id
        )

        select coin_id
        ,chain_input
        ,address_input
        ,source
        ,source_date
        ,has_valid_chain
        ,chain
        ,chain_id
        ,address
        ,created_at
        from data_checks dc

        -- don't add duplicates within the source table
        where record_count = 1

        -- don't add calls that share a normalized chain+address with existing coins
        and already_ingested = 0

        )
        '''

    dgc().run_sql(query_sql)



def intake_new_wallet_transfer_coins():
    '''
    ingests new coins from the etl_pipelines.coin_wallet_net_transfers table into the 
    etl_pipelines.coins_intake table through the following steps:
        1. normalizes coin addresses and chains
        2. checks for duplicates and coins that have already been ingested
        3. inserts the remaining coins
    '''

    query_sql = '''
        insert into etl_pipelines.coins_intake (

        with all_coins as (
            select generate_uuid() as coin_id
            ,c.blockchain as chain_input
            ,c.address as address_input
            ,data_source as source
            ,source_date
            ,case when ch.chain_id is not null then TRUE else FALSE end as has_valid_chain
            ,ch.chain
            ,ch.chain_id
            ,case
                when ch.is_case_sensitive = FALSE then lower(c.address)
                else c.address
                end as address
            ,current_datetime() as created_at
            from (
                select token_address as address
                ,chain_text_source as blockchain
                ,data_source
                ,min(data_updated_at) as source_date
                from `etl_pipelines.coin_wallet_net_transfers` 
                group by 1,2,3
            ) c
            left join `reference.chain_nicknames` chn on lower(chn.chain_reference) = lower(c.blockchain)
            left join core.chains ch on ch.chain_id = chn.chain_id
            where c.address is not null
        ),

        data_checks as (
            select ac.*
            ,row_number() over (partition by ac.chain_id,ac.address order by ac.source_date asc) as record_count
            ,case when c.coin_id is not null then 1 else 0 end as already_ingested
            from all_coins ac
            left join etl_pipelines.coins_intake c on c.address = ac.address and c.chain_id = ac.chain_id
        )

        -- select 
        -- case when record_count > 1 then 1 else 0 end as is_dupe
        -- ,already_ingested
        -- ,count(coin_id)
        -- from data_checks
        -- group by 1,2
        -- order by 1,2

        select coin_id
        ,chain_input
        ,address_input
        ,source
        ,source_date
        ,has_valid_chain
        ,chain
        ,chain_id
        ,address
        ,created_at
        from data_checks dc

        -- don't add duplicates within the source table
        where record_count = 1

        -- don't add calls that share a normalized chain+address with existing coins
        and already_ingested = 0

        )

        '''

    dgc().run_sql(query_sql)



def refresh_core_coins():
    '''
    truncates and recreates core.coins based on the current etl_pipelines.coins_intake table
    '''

    query_sql = '''
        truncate table core.coins;
        insert into core.coins (

            select ci.coin_id
            ,ci.chain
            ,ci.chain_id
            ,ci.address
            ,ci.source
            ,ci.source_date
            ,cfcg.symbol as symbol
            ,cfcg.name as name
            ,cfcg.decimals as decimals
            ,cfcg.total_supply as total_supply
            ,cfcg.coingecko_id as coingecko_id
            ,case when cmd.coin_id is not null then TRUE else FALSE end as has_market_data
            ,case when cwt.coin_id is not null then TRUE else FALSE end as has_wallet_transfer_data
            ,ci.created_at
            from etl_pipelines.coins_intake ci
            left join `core.coin_facts_coingecko` cfcg on cfcg.coin_id = ci.coin_id
            left join (
                select coin_id
                from core.coin_market_data
                group by 1
            ) cmd on cmd.coin_id = ci.coin_id
            left join (
                select coin_id
                from core.coin_wallet_transfers 
                group by 1
            ) cwt on cwt.coin_id = ci.coin_id

        )

        '''

    dgc().run_sql(query_sql)


def check_coin_counts():
    '''
    checks how many records exist in core.coins for each source
    '''

    query_sql = '''
        select source
        ,count(coin_id) as coins
        from core.coins
        group by 1
        '''

    df = dgc().run_sql(query_sql)

    calls_coins = df.loc[df['source'] == 'community_calls', 'coins'].values[0]
    dune_coins = df.loc[df['source'] == 'dune', 'coins'].values[0]
    other_coins = df['coins'].sum() - calls_coins - dune_coins

    return calls_coins,dune_coins,other_coins


@functions_framework.http
def update_core_coins(request):
    '''
    updates core.coins by adding new records from calls and dune, then refreshing the core.coins table. 
    even if there are no new coins added the table should still be refreshed to make updates to new 
    data connections, such as new coingecko_ids, market data, etc. 
    '''
    # configure logger
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s',
        datefmt='%d/%b/%Y %H:%M:%S'
        )
    logger = logging.getLogger(__name__)

    # add new community calls to etl_pipelines.coins_intake
    intake_new_community_calls()
    
    # add new coins with wallet transfer data from the whale chart function
    intake_new_wallet_transfer_coins()

    # refresh core.coins to add coins or update data completeness (e.g. has_market_data, etc)
    calls_coins_old,dune_coins_old,other_coins_old = check_coin_counts()
    refresh_core_coins()

    # summarize changes for logging purposes
    calls_coins,dune_coins,other_coins = check_coin_counts()

    new_calls = calls_coins - calls_coins_old
    new_dune = dune_coins - dune_coins_old
    new_other = other_coins - other_coins_old
    total_coins = calls_coins + dune_coins + other_coins
    total_new_coins = new_calls + new_dune + new_other

    logger.info(
        f"refreshed core.coins to {total_coins} total records."
        f"{new_calls} new coins added from community calls"
        f"{new_dune} new coins added from dune wallet transfer data"
        f"{new_other} new coins added from other sources"
    )

    return f'{{"finished updating core.coins to {total_coins} records ({total_new_coins} newly added)"}}'

