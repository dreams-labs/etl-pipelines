"""
Retrieves a count of how many wallet transfer days would need to be imported from Dune to
add the coin's transfer records and stores them in etl_pipelines.dune_new_coin_transfer_counts.
"""
import datetime
import os
from dune_client.client import DuneClient
from dune_client.query import QueryBase
import pandas_gbq
from dreams_core.googlecloud import GoogleCloud as dgc
from dreams_core import core as dc

# set up logger at the module level
logger = dc.setup_logger()


def retrieve_new_coins_list(dune_chains, coin_limit=500, refresh_existing_counts=False):
    """
    Retrieves a list of coins that don't have any Dune transfer data. By default it
    won't queue up coins that have any existing transfer counts because the resulting
    query takes a long time to run on Dune.

    Params:
    - dune_chains (list): a list that includes chain names that match Dune's terminology
    - coin_limit (int): the maximum number of coins to refresh at once
    - refresh_existing_counts (bool): whether to refresh transfer counts for coins that
        already have records in etl_pipelines.dune_new_coin_transfer_counts

    Returns:
    - new_coins_df (pd.DataFrame):
    """
    if refresh_existing_counts:
        exclude_counts_sql = ""
    else:
        exclude_counts_sql = "and has_counts.contract_address is null"

    # retrieve freshness df
    if dune_chains:
        dune_chains_string = "','".join(dune_chains)
        chain_filter_sql = f"where chain in ('{dune_chains_string}')"
    else:
        chain_filter_sql = ""
    query_sql = f"""
        with existing_records as (
            select chain_text_source as chain
            ,token_address
            ,decimals
            ,max(date) as freshest_date
            from etl_pipelines.coin_wallet_net_transfers
            where data_source = 'dune'

            -- all ethereum transfers are sourced from the public ethereum transfers table
            and chain_text_source <> 'ethereum'
            group by 1,2,3
        )
        ,new_records as (
            select ch.chain_text_dune as chain
            ,c.address as token_address
            ,c.decimals
            ,volume.total_volume
            ,cast('2000-01-01' as datetime) as freshest_date
            from core.coins c
            join core.chains ch on ch.chain_id = c.chain_id
            join (
                select coin_id
                ,sum(volume) as total_volume
                from core.coin_market_data cmd
                group by 1
            ) volume on volume.coin_id = c.coin_id
            left join existing_records e on e.token_address = c.address
                and e.chain = ch.chain_text_dune
            left join etl_pipelines.core_transfers_coin_exclusions coin_exclusions on coin_exclusions.coin_id = c.coin_id
            left join etl_pipelines.stables_and_wraps_exclusions stables on stables.coin_id = c.coin_id
            left join (
                select chain
                ,contract_address
                from etl_pipelines.dune_new_coin_transfer_counts
                group by 1,2
             ) has_counts on has_counts.chain = ch.chain_text_dune
                and has_counts.contract_address = c.address

            -- remove coin exclusions
            where coin_exclusions.coin_id is null
            and stables.coin_id is null

            -- all ethereum transfers are sourced from the public ethereum transfers table
            -- Dune Solana data is suspect
            and c.chain not in ('Ethereum','Solana')

            -- new coins don't have existing transfer data
            and e.token_address is null

            -- new coins must have dune-supported blockchains
            and ch.chain_text_dune is not null

            -- new coins currently need decimal data to run the dune queries
            and c.decimals is not null

            -- ignore coins that already have counts if configured to do so
            {exclude_counts_sql}
        )
        select chain
        ,token_address
        ,current_timestamp() as updated_at
        from (
            select * from new_records
        )
        {chain_filter_sql}
        -- where chain not in ('bnb','base','polygon','avalanche_c')
        order by total_volume asc
        limit {coin_limit}
    """
    new_coins_df = dgc().run_sql(query_sql)
    logger.info('Retrieved metadata for %s tokens with no transfers data.', len(new_coins_df))

    return new_coins_df



def load_new_coins_to_dune(new_coins_df):
    """
    Updates the Dune table etl_new_erc20_coins with all coins that don't have any
    Dune transfer records.

    Params:
    - dune_chains (list): a list that includes chain names that match Dune's terminology
    - refresh_existing_counts (bool): whether to refresh transfer counts for coins that
        already have records in etl_pipelines.dune_new_coin_transfer_counts
    """

    # store df locally as csv
    local_csv = 'new_erc20_coins.csv'
    new_coins_df.to_csv(local_csv, index=False)

    # append the csv to dune
    dune = DuneClient.from_env()
    with open(local_csv, "rb") as data:
        response = dune.insert_table(
                    namespace='dreamslabs',
                    table_name='etl_new_erc20_coins',
                    data=data,
                    content_type='text/csv'
                )
    logger.info('Dune append outcome: <%s>', response)

    # remove local csv
    os.remove(local_csv)



def generate_erc_20_counts_query(dune_chains):
    """
    Generates a query with counts of how many transfer records need to be ingested
    for all the coins of the input dune chains.

    params:
        dune_chains <set>: a set of all blockchains that need freshness updates

    returns:
        full_query <str>: the long dune query that will generates the wallet-coin-day-transfers
    """

    # all erc20 tokens have identical table structures so this query can be repeated for each
    def erc20_query(chain_text_dune):
        return f"""
        {chain_text_dune} as (
            --  retrieving the most recent batch of bigquery records available in dune
            with new_coin_batch as (
                select chain
                ,from_hex(token_address) as token_address
                ,updated_at
                from (
                    select *
                    ,rank() over (order by updated_at desc) as batch_recency
                    from dune.dreamslabs.etl_new_erc20_coins t
                )
                where batch_recency = 1
            ),
            -- filter transfers on index column 'block_time' to improve performance
            transfers_filtered as (
                select '{chain_text_dune}' as chain
                ,t.evt_block_time as block_time
                ,t."from" as from_token_account
                ,t."to" as to_token_account
                ,t.contract_address as token_mint_address
                from erc20_{chain_text_dune}.evt_Transfer t
                -- filter to only coins in the current batch
                join new_coin_batch ts on ts.token_address = t.contract_address
                -- remove rows from today since the daily net totals aren't finalized
                and date_trunc('day', t.evt_block_time at time zone 'UTC') <
                    date(current_timestamp at time zone 'UTC')
            ),
            transfers as (
                select t.chain
                ,date_trunc('day', t.block_time at time zone 'UTC') as date
                ,t.from_token_account as address
                ,token_mint_address as contract_address
                from transfers_filtered t

                union all

                select t.chain
                ,date_trunc('day', t.block_time at time zone 'UTC') as date
                ,t.to_token_account as address
                ,token_mint_address as contract_address
                from transfers_filtered t
            ),
            daily_net_transfers as (
                select chain
                ,date
                ,address
                ,contract_address
                from transfers
                group by 1,2,3,4
            )

            select chain
            ,contract_address
            ,count(date) as transfer_records
            from daily_net_transfers
            group by 1,2
            order by 3 asc
        )
        """
    # Define strings that will become queries
    query_ctes = None
    query_selects = None

    # ERC20 blockchain query logic
    for chain_text_dune in dune_chains:

        # Solana is not ERC20
        if chain_text_dune=='solana':
            continue

        # The first queries shouldn't have a UNION appended to them
        if not query_ctes:
            query_ctes = f'with {erc20_query(chain_text_dune)}'
            query_selects = f'select * from {chain_text_dune}'
            continue

        # Join all queries together to be run at once
        query_ctes = '\n,'.join([query_ctes,erc20_query(chain_text_dune)])
        query_selects = '\nunion all\n'.join([query_selects,f'select * from {chain_text_dune}'])

    full_query = query_ctes+query_selects
    logger.info('generated full transfer count query.')

    return full_query


def get_dune_transfer_counts(new_coins_query):
    """
    runs the query in dune and retrieves the results as a df.

    params:
        full_query (str): sql query to run
    returns:
        new_coins_df (json): df showing how many transfers records exist for the coins
    """
    dune = DuneClient.from_env()

    # Update the query with a version that includes all necessary blockchains
    query_id = dune.update_query(
        query_id = 4375482,
        query_sql=new_coins_query
    )

    transfers_query = QueryBase(
        query_id=query_id,
    )
    # Run Dune query and load to a dataframe
    logger.info('fetching fresh dune data...')
    transfer_counts_df = dune.run_query_dataframe(
        transfers_query,
        performance='large',
        ping_frequency=20
        )
    logger.info('Retrieved transfer counts for %s coins.', len(transfer_counts_df))

    return transfer_counts_df



def upload_transfer_counts(transfer_counts_df):
    """
    Appends the Dune query results to BigQuery table etl_pipelines.dune_new_coin_transfer_counts.

    Params:
    - transfer_counts_df (pd.DataFrame): result from Dune query with columns
        chain,contract_address,transfer_records
    """
    # copy df and add updated_at column
    upload_df = transfer_counts_df.copy()
    upload_df['updated_at'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # set df datatypes of upload df
    dtype_mapping = {
        'chain': str,
        'contract_address': str,
        'transfer_records': int,
        'updated_at': 'datetime64[ns, UTC]'
    }
    upload_df = upload_df.astype(dtype_mapping)
    logger.info('Prepared upload df with %s rows.',len(upload_df))

    # upload df to bigquery
    project_id = 'western-verve-411004'
    table_name = 'etl_pipelines.dune_new_coin_transfer_counts'
    schema = [
        {'name':'chain', 'type': 'string'},
        {'name':'contract_address', 'type': 'string'},
        {'name':'transfer_records', 'type': 'int64'},
        {'name':'updated_at', 'type': 'datetime'}
    ]
    pandas_gbq.to_gbq(
        upload_df
        ,table_name
        ,project_id=project_id
        ,if_exists='append'
        ,table_schema=schema
        ,progress_bar=False
    )
    logger.info('Updated etl_pipelines.dune_new_coin_transfer_counts with new counts.')



# def create_dune_new_coins_table():
#     """
#     this is the code that was used to create dune.dreamslabs.etl_new_erc20_coins.
#     it is not intended to be reran as part of normal operations but is retained in case it needs
#     to be referenced or altered.

#     params:
#         none
#     returns:
#         none
#     """
#     # make empty dune table
#     dune = DuneClient.from_env()

#     table = dune.create_table(
#         namespace='dreamslabs',
#         table_name='etl_new_erc20_coins',
#         description='erc20 coins that do not have dune transfer data',
#         schema= [
#             {'name': 'chain', 'type': 'varchar'},
#             {'name': 'token_address', 'type': 'varchar'},
#             {'name': 'updated_at', 'type': 'timestamp'},
#         ],
#         is_private=False
#     )
