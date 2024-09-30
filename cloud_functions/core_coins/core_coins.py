'''
cloud function that runs a query to refresh the data in bigquery table core.coins
'''
import functions_framework
import dreams_core.core as dc
from dreams_core.googlecloud import GoogleCloud as dgc

# set up logger at the module level
logger = dc.setup_logger()


@functions_framework.http
def update_core_coins(request):
    '''
    updates core.coins by adding new records from calls and dune, then refreshing the core.coins
    table. even if there are no new coins added the table should still be refreshed to reflect
    updates to new data connections, such as new coingecko_ids, market data, etc.
    '''
    # Get query parameter to control whether to intake new coins or to just rebuild core.coins
    intake_new_coins = request.args.get('intake_new_coins', 'false')

    # intake new coins if instructed to do so
    if intake_new_coins == 'true':
        logger.info("ingesting new coins to etl_pipelines.coin_intake...")

        # load new community calls into bigquery
        refresh_community_calls_table()

        # add new coins in the etl_pipelines.community_calls to etl_pipelines.coins_intake
        intake_new_community_calls_coins()

        # add new coins with wallet transfer data from the whale chart function
        intake_new_wallet_transfer_coins()

        # add new coins from the coingecko_all_coins etl tables
        intake_new_coingecko_all_coins()

    # refresh core.coins, logging the number of coins before and after the refresh
    logger.info("rebuilding core.coins table...")
    calls_coins_old,dune_coins_old,other_coins_old = check_coin_counts()
    refresh_core_coins()
    calls_coins,dune_coins,other_coins = check_coin_counts()

    # summarize changes for logging purposes
    new_calls = calls_coins - calls_coins_old
    new_dune = dune_coins - dune_coins_old
    new_other = other_coins - other_coins_old
    total_coins = calls_coins + dune_coins + other_coins
    total_new_coins = new_calls + new_dune + new_other

    logger.info("refreshed core.coins to %s total records.", total_coins)
    logger.info("%s new coins added from community calls.", new_calls)
    logger.info("%s new coins added from dune wallet transfer data.", new_dune)
    logger.info("%s new coins added from other sources.", new_other)

    outcome = ('{"finished updating core.coins to %s records (%s newly added)"}'
                % (total_coins, total_new_coins))

    return outcome


def refresh_community_calls_table():
    '''
    refreshes the etl_pipelines.community_calls bigquery table by uploading the gcs_export tab
        in the Community Calls google sheet
    '''
    # read the community calls gcs_export tab as a df
    # link: https://docs.google.com/spreadsheets/d/1X6AJWBJHisADvyqoXwEvTPi1JSNReVU_woNW32Hz_yQ/edit?pli=1&gid=1640621634 # pylint: disable=C0301
    df = dgc().read_google_sheet('1X6AJWBJHisADvyqoXwEvTPi1JSNReVU_woNW32Hz_yQ','gcs_export!A:H')

    # use the df to refresh the etl_pipelines.community_calls table
    dgc().upload_df_to_bigquery(
        df,
        'etl_pipelines',
        'community_calls',
        if_exists='replace'
    )


def intake_new_community_calls_coins():
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
            and c.address <> '#n/a'
        ),

        data_checks as (
            select ac.*
            ,row_number() over (partition by ac.chain_id,ac.address order by ac.source_date asc) as record_count
            ,case when c.coin_id is not null then 1 else 0 end as already_ingested
            ,case when c.coin_id is not null then 1 else 0 end as already_in_queue
            from all_calls ac
            left join core.coins c on c.address = ac.address and c.chain_id = ac.chain_id
            left join etl_pipelines.coins_intake ci on ci.address = ac.address and ci.chain_id = ac.chain_id
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

        -- don't add calls that share a normalized chain+address with existing core.coins
        and already_ingested = 0

        -- don't add calls that share a normalized chain+address with coins in the intake_queue
        and already_in_queue = 0

        -- don't add calls with invalid chain values
        and has_valid_chain = True

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



def intake_new_coingecko_all_coins():
    '''
    ingests new coins from the etl_pipelines.coingecko_all_coins_intake_queue table into
    the etl_pipelines.coins_intake table through the following steps:
        1. normalizes coin addresses and chains
        2. checks for duplicates and coins that have already been ingested
        3. inserts the remaining coins
    '''

    query_sql = '''
        insert into etl_pipelines.coins_intake (

        -- format intake tablefields for insert statement
        with all_additions as (
            select generate_uuid() as coin_id
            ,c.blockchain as chain_input
            ,c.address as address_input
            ,'coingecko_all_coins' as source
            ,c.source_date
            ,case when ch.chain_id is not null then TRUE else FALSE end as has_valid_chain
            ,ch.chain
            ,ch.chain_id
            ,case
                when ch.is_case_sensitive = FALSE then lower(c.address)
                else c.address
                end as address
            ,current_datetime() as created_at
            from etl_pipelines.coingecko_all_coins_intake_queue c
            left join `reference.chain_nicknames` chn on lower(chn.chain_reference) = lower(c.blockchain)
            left join core.chains ch on ch.chain_id = chn.chain_id
        )


        -- data checks on the contents
        ,data_checks as (
            select ac.*
            ,row_number() over (partition by ac.coin_id order by ac.source_date desc) as rn
            ,case when c.coin_id is not null then 1 else 0 end as address_matched
            ,case when ci.coin_id is not null then 1 else 0 end as already_ingested
            from all_additions ac
            left join core.coins c on c.address = ac.address and c.chain_id = ac.chain_id
            left join etl_pipelines.coins_intake ci on ci.address = ac.address and ci.chain_id = ac.chain_id

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

        -- only include coins with addresses
        where dc.address is not null

        -- don't add duplicates within the source table
        and rn = 1

        -- don't add calls that share a normalized chain+address with core.coins records
        and address_matched = 0

        -- don't add calls that share a normalized chain+address with intake queue records
        and already_ingested = 0

        -- don't add calls with invalid chain values
        and has_valid_chain = True

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
            with all_coins_with_transfers as (
                -- core.coin_wallet_transfers
                select coin_id
                from core.coin_wallet_transfers

                union distinct

                -- etl_pipelines.coin_wallet_net_transfers (dune)
                select c.coin_id
                from core.coins c
                join core.chains ch on ch.chain_id = c.chain_id
                join etl_pipelines.coin_wallet_net_transfers wnt on wnt.token_address = c.address
                    and (wnt.chain_text_source = ch.chain_text_dune and wnt.data_source = 'dune')

                union distinct

                -- ethereum_net_transfers
                select c.coin_id
                from `etl_pipelines.ethereum_net_transfers` t
                join core.coins c on c.address = t.token_address and c.chain = 'Ethereum'
            )

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
            ,cfcg.geckoterminal_id as geckoterminal_id
            ,case when cmd.coin_id is not null then TRUE else FALSE end as has_market_data
            ,case when cwt.coin_id is not null then TRUE else FALSE end as has_wallet_transfer_data
            ,ci.created_at
            from etl_pipelines.coins_intake ci
            left join `core.coin_facts_metadata` cfcg on cfcg.coin_id = ci.coin_id
            left join (
                select coin_id
                from core.coin_market_data
                group by 1
            ) cmd on cmd.coin_id = ci.coin_id
            left join all_coins_with_transfers cwt on cwt.coin_id = ci.coin_id
            where has_valid_chain = True
        )
        '''

    df = dgc().run_sql(query_sql)

    return df



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
