'''
cloud function that runs a query to refresh the data in bigquery table core.coin_wallet_transfers
'''
import datetime
from pytz import utc
import pandas as pd
import functions_framework
from dreams_core.googlecloud import GoogleCloud as dgc
from dreams_core import core as dc

# set up logger at the module level
logger = dc.setup_logger()


@functions_framework.http
def update_core_coin_wallet_transfers(request):  # pylint: disable=W0613
    '''
    runs all functions in sequence to refresh core.coin_wallet_transfers
    '''
    # update list of addresses to be excluded from the core table
    update_exclusions_table()

    # rebuild core table
    logger.info('rebuilding table core.coin_wallet_transfers...')
    counts_df = rebuild_core_coin_wallet_transfers()

    # log job summary
    core_count = counts_df[counts_df['table']=='core.coin_wallet_transfers']['records'].iloc[0]
    etl_count = counts_df[
        counts_df['table']=='etl_pipelines.coin_wallet_net_transfers'
        ]['records'].iloc[0]
    logger.info(
        'rebuilt core.coin_wallet_transfers [%s rows] from '
        'etl_pipelines.coin_wallet_net_transfers [%s rows].',
        core_count, etl_count
    )

    return '{{"rebuild of core.coin_wallet_transfers complete."}}'



def update_exclusions_table():
    '''
    this is a list of excluded wallet addresses and includes uniswap, burn/mint addresses,
    bridges, etc that result in negative balances and transaction bloat. this function
    refreshes the etl_pipelines.core_coin_wallet_transfers_exclusions bigquery table by ingesting
    the underlying sheets data, formatting it, and uploading it to bigquery.

    exclusions are maintained in the 'core_coin_wallet_transfers_exclusions' tab of this sheet:
    https://docs.google.com/spreadsheets/d/11Mi1a3SeprY_GU_QGUr_srtd7ry2UrYwoaRImSACjJs/edit?gid=388901135
    '''
    # load the tab into a df
    df = dgc().read_google_sheet(
        '11Mi1a3SeprY_GU_QGUr_srtd7ry2UrYwoaRImSACjJs',
        'core_coin_wallet_transfers_exclusions!A:E')

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
    rebuilds core.coin_wallet_transfers based on the current records in both the dune transfers table
    and the ethereum_net_transfers table.

    returns:
        counts_df <df>: dataframe showing the number of rows in the core and etl transfers tables
    '''

    query_sql = '''
        truncate table core.coin_wallet_transfers;

        insert into core.coin_wallet_transfers (

            with dune_transfers as (
                select c.coin_id
                ,c.chain_id
                ,wnt.token_address
                ,wnt.wallet_address
                ,wnt.date
                ,cast(wnt.daily_net_transfers as float64) as net_transfers
                ,sum(cast(daily_net_transfers as float64))
                    over (partition by wnt.token_address,wnt.wallet_address,ch.chain_id order by wnt.date asc) as balance
                ,count(daily_net_transfers)
                    over (partition by wnt.token_address,wnt.wallet_address,ch.chain_id order by wnt.date asc) as transfer_sequence
                from core.coins c
                join core.chains ch on ch.chain_id = c.chain_id
                join etl_pipelines.coin_wallet_net_transfers wnt on wnt.token_address = c.address
                    and (wnt.chain_text_source = ch.chain_text_dune and wnt.data_source = 'dune')

                -- remove dune's various representations of burn/mint addresses
                and wnt.wallet_address <> 'None' -- removes burn/mint address for solana
                and wnt.wallet_address <> '0x0000000000000000000000000000000000000000' -- removes burn/mint addresses
                and wnt.wallet_address <> '<nil>'
            )

            ,eth_transfers as (
                select c.coin_id
                ,c.chain_id
                ,eth.token_address
                ,eth.wallet_address
                ,eth.date
                ,cast(eth.amount as float64) as net_transfers
                ,sum(cast(eth.amount as float64))
                    over (partition by eth.token_address,eth.wallet_address order by eth.date asc) as balance
                ,count(eth.amount)
                    over (partition by eth.token_address,eth.wallet_address order by eth.date asc) as transfer_sequence
                from etl_pipelines.ethereum_net_transfers eth
                join core.coins c on c.address = eth.token_address and c.chain = 'Ethereum'

                -- remove any coins that also have dune transfers data
                -- this should be covered by dbt test_eEthereumNetTransfers_no_overlap_with_dune
                left join (
                select token_address from dune_transfers group by 1
                ) dune_coins on dune_coins.token_address = eth.token_address
                where dune_coins.token_address is null
            )

            ,all_transfers as (
                select * from dune_transfers
                union all
                select * from eth_transfers
            )

            ,exclusion_wallet_addresses as (
                -- manual exclusions from https://docs.google.com/spreadsheets/d/11Mi1a3SeprY_GU_QGUr_srtd7ry2UrYwoaRImSACjJs/edit?gid=1863435581#gid=1863435581
                select ch.chain_id
                ,e.chain_text_source
                ,case
                    when ch.is_case_sensitive=False then lower(e.wallet_address)
                    else e.wallet_address
                    end as wallet_address
                from `etl_pipelines.core_coin_wallet_transfers_exclusions` e
                join `core.chains` ch on (
                    (ch.chain_text_dune = e.chain_text_source)
                    or e.chain_text_source = 'all'
                    )

                union all

                -- cex addresses from dune query https://dune.com/queries/4057433
                select ch.chain_id
                ,e.blockchain as chain_text_source
                ,case
                    when ch.is_case_sensitive=False then lower(e.wallet_address)
                    else e.wallet_address
                    end as wallet_address
                from `reference.addresses_cexes` e
                join `core.chains` ch on ch.chain_text_dune = e.blockchain

                union all

                -- contract addresses from dune query https://dune.com/queries/4057525
                select ch.chain_id
                ,e.blockchain as chain_text_source
                ,case
                    when ch.is_case_sensitive=False then lower(e.address)
                    else e.address
                    end as address
                from `reference.addresses_contracts` e
                join `core.chains` ch on ch.chain_text_dune = e.blockchain
            ),

            coin_wallet_transfers_draft as (
                -- create a draft of the output table that can be audited for inconsistencies
                select t.coin_id
                ,t.chain_id
                ,t.token_address
                ,t.wallet_address
                ,t.date
                ,t.net_transfers
                ,t.balance
                ,t.transfer_sequence
                from all_transfers t

                -- wallet address exclusions
                left join exclusion_wallet_addresses wallet_exclusions on wallet_exclusions.wallet_address = t.wallet_address
                    and wallet_exclusions.chain_id = t.chain_id

                -- token exclusions
                left join etl_pipelines.ethereum_transfers_exclusions coin_exclusions on coin_exclusions.coin_id = t.coin_id

                -- removes self custody contract transactions
                where t.token_address <> t.wallet_address

                -- remove the manually excluded addresses
                and wallet_exclusions.wallet_address is null

                -- remove stablecoins, staked tokens, very large caps, etc
                and coin_exclusions.coin_id is null
            ),

            negative_wallets as (
            -- identify the minimum balance for each coin-wallet pair
                select coin_id
                ,wallet_address
                ,min(balance) as lowest_balance
                from coin_wallet_transfers_draft
                group by 1,2
            ),

            negative_wallets_coins as (
                -- identify how many negative balance wallets are associated with each coin.
                -- balances below -0.1 tokens are classified as negative to ignore rounding errors.
                select coin_id
                ,count(wallet_address) as wallets
                ,count(case when lowest_balance < -.1 then wallet_address end) as negative_wallets
                from (
                    select coin_id
                    ,wallet_address
                    ,min(balance) as lowest_balance
                    from coin_wallet_transfers_draft
                    group by 1,2
                )
                group by 1
            )

            select cwt.*
            from coin_wallet_transfers_draft cwt
            join negative_wallets_coins nwc on nwc.coin_id = cwt.coin_id
            join negative_wallets nw on nw.coin_id = cwt.coin_id and nw.wallet_address = cwt.wallet_address

            -- exclude all coin-wallet pairs with negative balances
            and nw.lowest_balance > -0.1

            -- if a coin has more than 10 negative wallets, exclude all coin-wallet pairs
            -- for that coin. there is a buffer of 10 to allow for rounding errors, mint addresses \
            -- that haven't been excluded, etc
            where nwc.negative_wallets < 10


        );

        select 'core.coin_wallet_transfers' as table
        ,count(*) as records
        from core.coin_wallet_transfers

        union all

        select 'etl_pipelines.coin_wallet_net_transfers'
        ,count(*)
        from etl_pipelines.coin_wallet_net_transfers

        union all

        select 'etl_pipeline.ethereum_net_transfers'
        ,count(*)
        from etl_pipelines.ethereum_net_transfers
        ;
        '''

    counts_df = dgc().run_sql(query_sql)

    return counts_df
