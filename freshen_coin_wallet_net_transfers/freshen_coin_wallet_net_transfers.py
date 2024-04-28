'''
provides updated whale chart data by following this sequence:
1. updates the dune table net_transfers_state with the current state of the bigquery table \
    etl_pipelines.coin_wallet_net_transfers

'''
import datetime
import os
import logging
import pandas as pd
from dune_client.client import DuneClient
from dune_client.query import QueryBase
import pandas_gbq
import functions_framework
from dreams_core.googlecloud import GoogleCloud as dgc

logging.basicConfig(
    level=logging.ERROR,
    format='[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s',
    datefmt='%d/%b/%Y %H:%M:%S'
    )
logger = logging.getLogger(__name__)

def update_dune_freshness_table():
    '''
    updates the dune table etl_net_transfers_freshness with the current state of the bigquery table 
    etl_pipelines.coin_wallet_net_transfers.

    params: None
    returns:
        update_chains <array>: an array of all blockchains that need freshness updates
    '''
    # retrieve freshness df
    query_sql = '''
        select chain_text_source as chain
        ,token_address
        ,decimals
        ,max(date) as freshest_date
        ,current_timestamp() as updated_at
        from etl_pipelines.coin_wallet_net_transfers
        where data_source = 'dune'
        group by 1,2,3
    '''
    freshness_df = dgc().run_sql(query_sql)
    logger.info('retrieved freshness data for %s tokens', freshness_df.shape[0])

    # store df locally as csv
    local_csv = 'net_transfers_freshness.csv'
    dune_df = freshness_df[['chain', 'token_address', 'freshest_date', 'updated_at']]
    dune_df.to_csv(local_csv, index=False)

    # append the csv to dune
    dune = DuneClient.from_env()
    with open(local_csv, "rb") as data:
        response = dune.insert_table(
                    namespace='dreamslabs',
                    table_name='etl_net_transfers_freshness',
                    data=data,
                    content_type='text/csv'
                )
    logger.info('dune append outcome: <%s>', response)

    # remove local csv
    os.remove(local_csv)

    return freshness_df


def generate_net_transfers_update_query(dune_chains):
    '''
    generates a long dune sql query that includes a separate CTE for each applicable blockchain \
    and unions them all together. the query will return all wallet-coin-days needed to fully \
    freshen the etl_pipelines.coin_wallet_net_transfers table in bigquery. 

    the function starts with long dune sql queries to get solana transfers and a template to \
    get transfers from any of the erc20 chains. these are then merged into a complete sql query.

    params:
        dune_chains <set>: a set of all blockchains that need freshness updates

    returns:
        full_query <str>: the long dune query that will return all wallet-coin-days needed to update 
    '''

    sol_query = '''
    with solana as (
        --  retrieving the most recent bigquery records available in dune
        with current_net_transfers_freshness as (
            with last_updated as (
                select max(updated_at) as updated_at
                from dune.dreamslabs.etl_net_transfers_freshness ts
            )
            select ts.*
            from dune.dreamslabs.etl_net_transfers_freshness ts
            join last_updated on last_updated.updated_at = ts.updated_at
        ),
        
        -- filter the transfers table on indexed columns (block_time) to improve subsequent query performance
        transfers_filtered as (
            -- find the earliest possible date that we need data for
            with most_out_of_date as (
                select min(cast(ts.freshest_date as date)) as date
                from current_net_transfers_freshness ts
                where ts.chain = 'solana'
            )
            select 'solana' as chain
            ,t.block_time
            ,t.from_token_account
            ,t.to_token_account
            ,t.token_mint_address
            ,t.amount
            from tokens_solana.transfers t
            -- remove all rows earlier than the earliest possible relevant date
            where date_trunc('day', t.block_time at time zone 'UTC') > (select date from most_out_of_date)
            -- remove rows from today since the daily net totals aren't finalized
            and date_trunc('day', t.block_time at time zone 'UTC') < 
                date(current_timestamp at time zone 'UTC')
        ),
        transfers as (
            select t.chain
            ,date_trunc('day', t.block_time at time zone 'UTC') as date
            ,t.from_token_account as address
            ,-cast(t.amount as double) as amount
            ,token_mint_address as contract_address
            from transfers_filtered t
            join current_net_transfers_freshness ts
                on ts.token_address = t.token_mint_address
                and ts.chain = t.chain
                and date_trunc('day', t.block_time at time zone 'UTC') > cast(ts.freshest_date as date)
            
            union all
            
            select t.chain
            ,date_trunc('day', t.block_time at time zone 'UTC') as date
            ,t.to_token_account as address
            ,cast(t.amount as double) as amount
            ,token_mint_address as contract_address
            from transfers_filtered t
            join current_net_transfers_freshness ts
                on ts.token_address = t.token_mint_address
                and ts.chain = t.chain
                and date_trunc('day', t.block_time at time zone 'UTC') > cast(ts.freshest_date as date)
        ),
        daily_net_transfers as (
            select chain
            ,date
            ,address
            ,contract_address
            ,sum(amount) as amount
            ,sum(abs(amount)) as gross_amount
            from transfers  
            group by 1,2,3,4
        )
        
        select date
        ,chain
        ,contract_address
        ,address as wallet_address
        ,amount as daily_net_transfers
        from daily_net_transfers
        where amount <> 0 -- excludes wallet days with equal to/from transactions that net to 0
        order by contract_address,address,date
    )'''

    def erc20_query(chain_text_dune):
        return f'''
    ,{chain_text_dune} as (
        --  retrieving the most recent bigquery records available in dune
        with current_net_transfers_freshness as (
            with last_updated as (
                select max(updated_at) as updated_at
                from dune.dreamslabs.etl_net_transfers_freshness ts
            )
            select ts.*
            from dune.dreamslabs.etl_net_transfers_freshness ts
            join last_updated on last_updated.updated_at = ts.updated_at
        ),
        
        -- filter the transfers table on indexed columns (block_time) to improve subsequent query performance
        transfers_filtered as (
            -- find the earliest possible date that we need data for
            with most_out_of_date as (
                select min(cast(ts.freshest_date as date)) as date
                from current_net_transfers_freshness ts
                where ts.chain = '{chain_text_dune}'
            )
            select '{chain_text_dune}' as chain
            ,t.evt_block_time as block_time
            ,cast(t."from" as varchar) as from_token_account
            ,cast(t."to" as varchar) as to_token_account
            ,cast(t.contract_address as varchar) as token_mint_address
            ,t.value as amount
            from erc20_{chain_text_dune}.evt_Transfer t
            -- remove all rows earlier than the earliest possible relevant date
            where date_trunc('day', t.evt_block_time at time zone 'UTC') > (select date from most_out_of_date)
            -- remove rows from today since the daily net totals aren't finalized
            and date_trunc('day', t.evt_block_time at time zone 'UTC') < 
                date(current_timestamp at time zone 'UTC')
        ),
        transfers as (
            select t.chain
            ,date_trunc('day', t.block_time at time zone 'UTC') as date
            ,t.from_token_account as address
            ,-cast(t.amount as double) as amount
            ,token_mint_address as contract_address
            from transfers_filtered t
            join current_net_transfers_freshness ts
                on ts.token_address = t.token_mint_address
                and ts.chain = t.chain
                and date_trunc('day', t.block_time at time zone 'UTC') > cast(ts.freshest_date as date)
            
            union all
            
            select t.chain
            ,date_trunc('day', t.block_time at time zone 'UTC') as date
            ,t.to_token_account as address
            ,cast(t.amount as double) as amount
            ,token_mint_address as contract_address
            from transfers_filtered t
            join current_net_transfers_freshness ts
                on ts.token_address = t.token_mint_address
                and ts.chain = t.chain
                and date_trunc('day', t.block_time at time zone 'UTC') > cast(ts.freshest_date as date)
        ),
        daily_net_transfers as (
            select chain
            ,date
            ,address
            ,contract_address
            ,sum(amount) as amount
            ,sum(abs(amount)) as gross_amount
            from transfers  
            group by 1,2,3,4
        )
        
        select date
        ,chain
        ,contract_address
        ,address as wallet_address
        ,amount as daily_net_transfers
        from daily_net_transfers
        where amount <> 0 -- excludes wallet days with equal to/from transactions that net to 0
        order by contract_address,address,date
    )
    '''

    query_ctes = sol_query
    query_selects = 'select * from solana'

    for chain_text_dune in dune_chains:
        if chain_text_dune=='solana':
            continue
        query_ctes = ''.join([query_ctes,erc20_query(chain_text_dune)])
        query_selects = '\nunion all\n'.join([query_selects,f'select * from {chain_text_dune}'])

    full_query = query_ctes+query_selects
    logger.info('generated full query.')

    return full_query



def get_fresh_dune_data(full_query):
    '''
    runs the query in dune and retreives the results as a df. note that decimal adjustments have \
    not yet been applied so the dune query values are not the same order of magnitude as the \
    bigquery values. the query may take >10 minutes to run as it retrieves transfers from \
    multiple chains

    params:
        full_query (str): sql query to run
    returns:
        transfers_df (pandas.DataFrame): df of token transfers without decimal calculations applied
    '''
    dune = DuneClient.from_env()

    # update the query with a version that includes all necessary blockchains
    query_id = dune.update_query(
        query_id = 3675936,
        query_sql=full_query
    )

    transfers_query = QueryBase(
        query_id=query_id,
    )
    # run dune query and load to a dataframe
    logger.info('fetching fresh dune data...')
    transfers_df = dune.run_query_dataframe(transfers_query, ping_frequency=10)
    logger.info('fetched fresh dune data with %s rows.', len(transfers_df))

    return transfers_df


def append_to_bigquery_table(freshness_df,transfers_df):
    '''
    uploads the new transfers data to bigquery to ensure the table is fully refreshed through
    the last full UTC day. 

    steps:
        1. map decimals data from bigquery onto the retrieved dune data and
            reduce transfer amounts by applicable decimals
        2. format upload_df to match bigquery table
        3. append upload_df to etl_pipelines.coin_wallet_net_transfers

    params:
        freshness_df (pandas.DataFrame): df of fresh dune data
        transfers_df (pandas.DataFrame): df of token transfers
    returns:
        none
    '''

    # map decimals data from bigquery onto the retrieved dune data
    freshness_df.rename(columns={'token_address':'contract_address'},inplace=True)
    transfers_df = pd.merge(
        transfers_df,
        freshness_df[['chain', 'contract_address', 'decimals']],
        how='left',
        on=['chain','contract_address']
    )

    # reduce transfer amounts by applicable decimals
    transfers_df['daily_net_transfers'] = transfers_df['daily_net_transfers'] \
        / (10 ** transfers_df['decimals'])

    # add metadata to upload_df
    upload_df = pd.DataFrame()
    upload_df['date'] = transfers_df['date']
    upload_df['chain_text_source'] = transfers_df['chain']
    upload_df['token_address'] = transfers_df['contract_address']
    upload_df['decimals'] = transfers_df['decimals']
    upload_df['wallet_address'] = transfers_df['wallet_address']
    upload_df['daily_net_transfers'] = transfers_df['daily_net_transfers']
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
    logger.info('prepared upload df with %s rows.',len(upload_df))

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
        ,progress_bar=False
    )
    logger.info('appended upload df to %s.', table_name)


# def create_dune_freshness_table():
#     '''
#     this is the code that was used to create dune.dreamslabs.etl_net_transfers_freshness.
#     it is not intended to be reran as part of normal operations but is retained in case it needs
#     to be referenced or altered.
#
#     params:
#         none
#     returns:
#         none
#     '''
#     # make empty dune table
#     dune = DuneClient.from_env()
#
#     table = dune.create_table(
#         namespace='dreamslabs',
#         table_name='etl_net_transfers_freshness',
#         description='coin wallet daily net transfer data freshness by chain and token address',
#         schema= [
#             {'name': 'chain', 'type': 'varchar'},
#             {'name': 'token_address', 'type': 'varchar'},
#             {'name': 'freshest_date', 'type': 'date'},
#             {'name': 'updated_at', 'type': 'timestamp'},
#         ],
#         is_private=False
#     )

@functions_framework.cloud_event
def freshen_coin_wallet_net_transfers():
    '''
    runs all functions in sequence to complete all update steps
    '''
    logger.info('initiating sequence to freshen etl_pipelines.coin_wallet_net_transfers...')

    # update the dune table that tracks how fresh the data is
    freshness_df = update_dune_freshness_table()

    # generate the sql query needed to refresh the transfers table
    update_chains = freshness_df['chain'].unique()
    full_query = generate_net_transfers_update_query(update_chains)

    # retrieve the fresh dune data using the generated query
    transfers_df = get_fresh_dune_data(full_query)

    # upload the fresh dune data to bigquery
    append_to_bigquery_table(freshness_df,transfers_df)
