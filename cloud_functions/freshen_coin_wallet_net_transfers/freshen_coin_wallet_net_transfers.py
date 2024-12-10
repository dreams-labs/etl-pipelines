"""
provides updated whale chart data by following this sequence:
1. updates the dune table net_transfers_state with the current state of the bigquery table \
    etl_pipelines.coin_wallet_net_transfers
2. generates a dune query for all blockchains in need of updates and unions them together
3. retrieves the dune results and uploads them to bigquery

"""
from datetime import datetime, timezone
import os
import json
import pandas as pd
from dune_client.client import DuneClient
from dune_client.query import QueryBase
import pandas_gbq
import functions_framework
from dreams_core.googlecloud import GoogleCloud as dgc
from dreams_core import core as dc

# set up logger at the module level
logger = dc.setup_logger()


@functions_framework.http
def freshen_existing_coin_wallet_net_transfers(request):  # pylint: disable=W0613
    """
    runs all functions in sequence to complete all update steps
    """
    logger.info('initiating sequence to freshen etl_pipelines.coin_wallet_net_transfers...')

    # retrieve the list of coins that will be updated
    freshness_df = retrieve_existing_coin_freshness()

    # update the dune freshness table
    update_dune_freshness_table(freshness_df)

    # generate the sql query needed to refresh the transfers table
    update_chains = freshness_df['chain'].unique()
    full_query = generate_net_transfers_update_query(update_chains)

    # retrieve the fresh dune data using the generated query
    transfers_json_df = get_fresh_dune_data(full_query)

    # convert the json column into a df
    transfers_df, parse_errors_df = parse_transfers_json(transfers_json_df)
    # expand the json data into df columns
    logger.info('completed translation from dune export json to dataframe.')
    if not parse_errors_df.empty:
        logger.info("JSON parse errors occurred for %s records. ", len(parse_errors_df))

    # upload the fresh dune data to bigquery
    append_to_bigquery_table(freshness_df,transfers_df)

    return "finished refreshing etl_pipelines.coin_wallet_net_transfers."



def retrieve_existing_coin_freshness():
    """
    Retrieves a df containing the coins that have existing Dune transfers data.
    """
    # retrieve freshness df
    query_sql = """
        select chain_text_source as chain
        ,token_address
        ,decimals
        ,max(date) as freshest_date
        from etl_pipelines.coin_wallet_net_transfers
        where data_source = 'dune'
        -- all ethereum transfers are sourced from the public ethereum transfers table
        and chain_text_source not in ('ethereum','solana')
        group by 1,2,3
        """
    freshness_df = dgc().run_sql(query_sql)
    logger.info('Retrieved freshness data for %s tokens with existing Dune data.',
                len(freshness_df))

    return freshness_df



def retrieve_new_coin_freshness(dune_chains=None, batch_size=1000000):
    """
    Retrieves a df containing the coins that will have their full history retrieved from Dune.

    Params:
    - dune_chains (list): a list of blockchains that new coins will be added from, with chain
        names that match Dune's terminology
    - batch_size (int): how many new transfer records belonging to the new coins should be added.
        the new coins will be added in the order of least new records to most.

    Returns:
    - freshness_df (pd.DataFrame): dataframe with info about the coins that will be queued
        for update
    """
    if dune_chains:
        dune_chains_string = "','".join(dune_chains)
        chain_filter_sql = f"and ch.chain_text_dune in ('{dune_chains_string}')"
    else:
        chain_filter_sql = ""

    # retrieve freshness df
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
            ,cast('2000-01-01' as datetime) as freshest_date
            ,transfer_counts.transfer_records
            ,sum(transfer_records) over (order by transfer_records asc) as records_running_total
            from core.coins c
            join core.chains ch on ch.chain_id = c.chain_id
            left join existing_records e on e.token_address = c.address
                and e.chain = ch.chain_text_dune
            left join etl_pipelines.core_transfers_coin_exclusions coin_exclusions on coin_exclusions.coin_id = c.coin_id
            left join etl_pipelines.stables_and_wraps_exclusions stables on stables.coin_id = c.coin_id
            left join (
                select chain
                ,contract_address
                ,transfer_records
                ,row_number() over (partition by chain,contract_address order by updated_at desc) as rn
                from etl_pipelines.dune_new_coin_transfer_counts
                order by transfer_records
            ) transfer_counts on transfer_counts.chain = ch.chain_text_dune
                and transfer_counts.contract_address = c.address
                and transfer_counts.rn = 1

            -- remove coin exclusions
            where coin_exclusions.coin_id is null
            and stables.coin_id is null

            -- all ethereum transfers are sourced from the public ethereum transfers table
            -- do not update solana tokens with negative wallets per dune data
            -- source: https://dune.com/queries/4094516
            -- dune github ticket: https://github.com/duneanalytics/spellbook/issues/6690
            and c.chain not in ('Ethereum','Solana')

            -- remove coins with existing transfer data
            and e.token_address is null

            -- only include coins with dune-supported blockchains
            and ch.chain_text_dune is not null

            -- coins currently need decimal data to run the dune queries
            and c.decimals is not null

            -- filter to only specific blockchains if instructed to
            {chain_filter_sql}
        )
        select chain
        ,token_address
        ,decimals
        ,freshest_date
        ,current_timestamp() as updated_at
        ,transfer_records
        ,records_running_total
        from new_records

        -- custom filters
        where records_running_total between 1 and {batch_size}
        order by transfer_records desc
        """
    freshness_df = dgc().run_sql(query_sql)
    logger.info('retrieved freshness data for %s tokens', freshness_df.shape[0])

    return freshness_df


def update_dune_freshness_table(freshness_df):
    """
    updates the dune table etl_net_transfers_freshness with the current state of the bigquery table
    etl_pipelines.coin_wallet_net_transfers.

    Params
    - freshness_df (pd.DataFrame): dataframe with info about the coins that will be queued
        for update
    """
    # define dune upload
    dune_df = freshness_df[['chain', 'token_address', 'freshest_date']]
    dune_df['updated_at'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

    # store df locally
    local_csv = 'net_transfers_freshness.csv'
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



def generate_net_transfers_update_query(dune_chains):
    """
    generates a long dune sql query that includes a separate CTE for each applicable blockchain \
    and unions them all together. the query will return all wallet-coin-days needed to fully \
    freshen the etl_pipelines.coin_wallet_net_transfers table in bigquery.

    the function starts with long dune sql queries to get solana transfers and a template to \
    get transfers from any of the erc20 chains. these are then merged into a complete sql query.

    params:
        dune_chains <set>: a set of all blockchains that need freshness updates

    returns:
        full_query <str>: the long dune query that will generates the wallet-coin-day-transfers
    """

    # query to retrieve solana transfers (solana tables have different structure than erc20 tables)
    sol_query = """
        with solana as (
            --  retrieving the most recent batch of bigquery records available in dune
            with current_net_transfers_freshness as (
                select chain
                ,token_address
                ,freshest_date
                ,updated_at
                from (
                    select *
                    ,rank() over (order by updated_at desc) as batch_recency
                    from dune.dreamslabs.etl_net_transfers_freshness t
                )
                where batch_recency = 1
            ),

            -- filter transfers on index (block_date) to improve query performance
            transfers_filtered as (
                -- find the earliest possible date that we need data for
                with most_out_of_date as (
                    select min(cast(ts.freshest_date as date)) as date
                    from current_net_transfers_freshness ts
                    where ts.chain = 'solana'
                )
                select 'solana' as chain
                ,t.block_date
                ,t.from_token_account
                ,t.to_token_account
                ,t.token_mint_address
                ,t.amount
                from tokens_solana.transfers t
                -- remove all rows earlier than the earliest possible relevant date
                where t.block_date > (select date from most_out_of_date)
                -- remove rows from today since the daily net totals aren't finalized
                and t.block_date < date(current_timestamp at time zone 'UTC')
            ),
            transfers as (
                select t.chain
                ,t.block_date as date
                ,t.from_token_account as address
                ,-cast(t.amount as double) as amount
                ,token_mint_address as contract_address
                from transfers_filtered t
                join current_net_transfers_freshness ts
                    on ts.token_address = t.token_mint_address
                    and ts.chain = t.chain
                    and t.block_date > cast(ts.freshest_date as date)

                union all

                select t.chain
                ,t.block_date as date
                ,t.to_token_account as address
                ,cast(t.amount as double) as amount
                ,token_mint_address as contract_address
                from transfers_filtered t
                join current_net_transfers_freshness ts
                    on ts.token_address = t.token_mint_address
                    and ts.chain = t.chain
                    and t.block_date > cast(ts.freshest_date as date)
            ),
            daily_net_transfers as (
                select chain
                ,date
                ,address
                ,contract_address
                ,sum(amount) as amount
                from transfers
                group by 1,2,3,4
            )

            select json_object(
                'date': date
                ,'chain': chain
                ,'contract_address': contract_address
                ,'wallet_address': address
                ,'daily_net_transfers': amount
                ) as transfers_json
            from daily_net_transfers
            where amount <> 0 -- excludes wallet days with equal to/from transactions that net to 0
        )
        """

    # all erc20 tokens have identical table structures so this query can be repeated for each
    def erc20_query(chain_text_dune):
        return f"""
        {chain_text_dune} as (
            --  retrieving the most recent batch of bigquery records available in dune
            with current_net_transfers_freshness as (
                select chain
                ,token_address
                ,freshest_date
                ,updated_at
                from (
                    select *
                    ,rank() over (order by updated_at desc) as batch_recency
                    from dune.dreamslabs.etl_net_transfers_freshness t
                )
                where batch_recency = 1
            ),

            -- filter transfers on index column 'block_time' to improve performance
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
                from transfers
                group by 1,2,3,4
            )

            select json_object(
                'date': date
                ,'chain': chain
                ,'contract_address': contract_address
                ,'wallet_address': address
                ,'daily_net_transfers': amount
                ) as transfers_json
            from daily_net_transfers
            where amount <> 0 -- excludes wallet days with equal to/from transactions that net to 0
        )
        """

    # Define strings that will become queries
    query_ctes = None
    query_selects = None

    # Solana blockchain query logic
    if 'solana' in dune_chains:
        query_ctes = sol_query
        query_selects = 'select * from solana'

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

        # Add the blockchain to the list of CTEs and select statements
        query_ctes = '\n,'.join([query_ctes,erc20_query(chain_text_dune)])
        query_selects = '\nunion all\n'.join([query_selects,f'select * from {chain_text_dune}'])

    # Combine CTEs and select statements into one big query
    full_query = query_ctes+query_selects
    logger.info('generated full query.')

    return full_query



def get_fresh_dune_data(full_query):
    """
    runs the query in dune and retrieves the results as a df. note that decimal adjustments have \
    not yet been applied so the dune query values are not the same order of magnitude as the \
    bigquery values. the query may take >10 minutes to run as it retrieves transfers from \
    multiple chains

    params:
        full_query (str): sql query to run
    returns:
        transfers_json_df (json): raw dune query response containing one column with json objects
    """
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
    transfers_json_df = dune.run_query_dataframe(
        transfers_query,
        performance='large',
        ping_frequency=15
        )
    logger.info('fetched fresh dune data with %s rows.', len(transfers_json_df))

    return transfers_json_df



def parse_transfers_json(transfers_json_df):
    """
    Parses the JSON data from the raw Dune response and converts it into a DataFrame.
    Any records that fail to parse are logged and skipped.

    Parameters:
        transfers_json_df (DataFrame): DataFrame containing one column with JSON objects.

    Returns:
        DataFrame: A DataFrame with successfully parsed records.
        DataFrame: A DataFrame with records that failed to parse, for further investigation.
    """
    parsed_records = []
    failed_records = []

    for index, record in enumerate(transfers_json_df['transfers_json']):
        try:
            parsed_record = json.loads(record)
            parsed_records.append(parsed_record)
        except json.JSONDecodeError as e:
            logger.error("Failed to parse JSON at index %d: %s", index, e)
            failed_records.append({'index': index, 'record': record, 'error': str(e)})

    # Create DataFrames for successfully parsed records and failed records
    parsed_df = pd.DataFrame(parsed_records)
    failed_df = pd.DataFrame(failed_records)

    logger.info("Parsed %d records successfully, %d records failed to parse.", len(parsed_df), len(failed_df))

    return parsed_df, failed_df



def append_to_bigquery_table(freshness_df,transfers_df):
    """
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
    """

    # Check if transfers_df is empty and terminate upload process if so
    if transfers_df.empty:
        logger.warning('No new wallet transfer data to append as transfers_df is empty.')
        return


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
    upload_df['data_updated_at'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

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
#     """
#     this is the code that was used to create dune.dreamslabs.etl_net_transfers_freshness.
#     it is not intended to be reran as part of normal operations but is retained in case it needs
#     to be referenced or altered.
#
#     params:
#         none
#     returns:
#         none
#     """
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
