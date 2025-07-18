"""
Cloud function that runs a query to refresh the data in bigquery table core.coin_wallet_transfers
using etl_pipelines.coin_wallet_net_transfers and etl_pipelines.ethereum_net_transfers.
"""
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
    """
    runs all functions in sequence to refresh core.coin_wallet_transfers
    """
    logger.info('updating coin and wallet exclusion tables...')
    # update list of addresses to be excluded from the core table
    update_wallet_exclusions_tables()

    # update list of coins to be excluded from the core table
    update_coin_exclusions_tables()

    # check for dupes and raise an exception if there are
    check_for_dupes()

    # rebuild core table
    logger.info('rebuilding table core.coin_wallet_transfers...')
    counts_df = rebuild_core_coin_wallet_transfers()

    # log job summary
    core_count = counts_df[counts_df['table']=='core.coin_wallet_transfers']['records'].iloc[0]
    dune_count = counts_df[counts_df['table']=='etl_pipelines.coin_wallet_net_transfers']['records'].iloc[0]
    eth_count = counts_df[counts_df['table']=='etl_pipeline.ethereum_net_transfers']['records'].iloc[0]
    logger.info(
        'rebuilt core.coin_wallet_transfers [%s rows] from '
        'Dune transfer data (etl_pipelines.coin_wallet_net_transfers) [%s rows]. '
        'Ethereum blockchain data (etl_pipeline.ethereum_net_transfers) [%s rows].',
        core_count, dune_count, eth_count
    )

    # rebuild address to ID mapping tables
    update_wallet_id_table()
    update_wallet_coin_id_table()

    return ({
        'message': 'rebuild of core.coin_wallet_transfers complete.',
        'core_count': int(core_count),
        'dune_count': int(dune_count),
        'eth_count': int(eth_count)
    }, 200)



def update_wallet_exclusions_tables():
    """
    this is a list of excluded wallet addresses and includes uniswap, burn/mint addresses,
    bridges, etc that result in negative balances and transaction bloat. this function
    refreshes the etl_pipelines.core_coin_wallet_transfers_exclusions bigquery table by ingesting
    the underlying sheets data, formatting it, and uploading it to bigquery.

    exclusions are maintained in the 'core_coin_wallet_transfers_exclusions' tab of this sheet:
    https://docs.google.com/spreadsheets/d/11Mi1a3SeprY_GU_QGUr_srtd7ry2UrYwoaRImSACjJs/edit?gid=388901135
    """
    # load the tab into a df
    df = dgc().read_google_sheet(
        '11Mi1a3SeprY_GU_QGUr_srtd7ry2UrYwoaRImSACjJs',
        'core_coin_wallet_transfers_exclusions!A:E')

    # format and upload df
    df['created_date'] = pd.to_datetime(df['created_date'], format='mixed').dt.tz_localize('UTC')
    df['updated_at'] = datetime.datetime.now(utc)
    dgc().upload_df_to_bigquery(
        df,
        'etl_pipelines',
        'core_coin_wallet_transfers_exclusions',
        if_exists='replace'
    )



def update_coin_exclusions_tables():
    """
    this is a list of excluded coins, mostly based on excessive transfer counts. this function
    refreshes the etl_pipelines.core_wallet_transfers_coin_exclusions bigquery table by ingesting
    the underlying sheets data, formatting it, and uploading it to bigquery.

    exclusions are maintained in the 'core_transfers_coin_exclusions' tab of this sheet:
    https://docs.google.com/spreadsheets/d/11Mi1a3SeprY_GU_QGUr_srtd7ry2UrYwoaRImSACjJs/edit?gid=388901135
    """
    # load the tab into a df
    df = dgc().read_google_sheet(
        '11Mi1a3SeprY_GU_QGUr_srtd7ry2UrYwoaRImSACjJs',
        'core_transfers_coin_exclusions!A:D')

    # format and upload df
    df['created_at'] = pd.to_datetime(df['created_at'], format='mixed').dt.tz_localize('UTC')
    df['updated_at'] = datetime.datetime.now(utc)
    dgc().upload_df_to_bigquery(
        df,
        'etl_pipelines',
        'core_transfers_coin_exclusions',
        if_exists='replace'
    )



def check_for_dupes():
    """
    Checks if there are any duplicate records for coin-wallet-date groupings in
    any of the source tables used for the core transfers table.

    Raises:
    - ValueError if duplicate records are found in either table
    """
    dupes_sql = """
        with dune_dupes as (
            select chain_text_source
            ,token_address
            ,wallet_address
            ,date
            ,count(date) as dupes
            ,max(data_updated_at) as recent_update
            from etl_pipelines.coin_wallet_net_transfers
            group by 1,2,3,4
        )

        ,eth_dupes as (
            select date
            ,token_address
            ,wallet_address
            ,count(date) as dupes
            from etl_pipelines.ethereum_net_transfers
            group by 1,2,3
        )

        select 'dune' as source
        ,count(*) as dupes
        from dune_dupes
        where dupes > 1

        union all

        select 'ethereum' as source
        ,count(*) as dupes
        from eth_dupes
        where dupes > 1

        """
    counts_df = dgc().run_sql(dupes_sql)

    if counts_df['dupes'].sum() > 0:
        dune_dupes = counts_df[counts_df['source']=='dune']['dupes'].iloc[0]
        eth_dupes = counts_df[counts_df['source']=='ethereum']['dupes'].iloc[0]
        raise ValueError(f"Dupes detected in source tables: {dune_dupes} in Dune and {eth_dupes} "
                        "in Ethereum. Aborting rebuild of core.coin_wallet_transfers.")



def rebuild_core_coin_wallet_transfers():
    """
    rebuilds core.coin_wallet_transfers based on the current records in both the dune transfers table
    and the ethereum_net_transfers table.

    returns:
        counts_df <df>: dataframe showing the number of rows in the core and etl transfers tables
    """

    query_sql = """
        CREATE OR REPLACE TABLE core.coin_wallet_transfers
        PARTITION BY date(date)
        CLUSTER BY coin_id AS (

            with eth_transfers as (
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
            ),

            dune_transfers as (
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

                -- all eth transfers come from bigquery
                where c.chain <> 'Ethereum'

                -- remove dune's various representations of burn/mint addresses
                and wnt.wallet_address <> 'None' -- removes burn/mint address for solana
                and wnt.wallet_address <> '0x0000000000000000000000000000000000000000' -- removes burn/mint addresses
                and wnt.wallet_address <> '<nil>'
            ),

            all_transfers as (
                select * from dune_transfers
                union all
                select * from eth_transfers
            ),

            exclusion_wallet_addresses as (
                select wallet_address
                from (
                    -- manual exclusions from https://docs.google.com/spreadsheets/d/11Mi1a3SeprY_GU_QGUr_srtd7ry2UrYwoaRImSACjJs/edit?gid=1863435581#gid=1863435581
                    select case
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
                    select case
                        when ch.is_case_sensitive=False then lower(e.wallet_address)
                        else e.wallet_address
                        end as wallet_address
                    from `reference.addresses_cexes` e
                    join `core.chains` ch on ch.chain_text_dune = e.blockchain

                    union all

                    -- contract addresses from dune query https://dune.com/queries/4057525
                    select case
                        when ch.is_case_sensitive=False then lower(e.address)
                        else e.address
                        end as address
                    from `reference.addresses_contracts` e
                    join `core.chains` ch on ch.chain_text_dune = e.blockchain

                    union all

                    -- contract addresses from `bigquery-public-data.crypto_ethereum.contracts`
                    select lower(address) as address
                    from `reference.addresses_ethereum_contracts`

                    union all

                    -- 0x000000000s are almost definitely not normal investors
                    select wallet_address
                    from all_transfers t
                    where wallet_address like '0x000000000%'
                )
                group by 1
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
                left join core.coins contract_addresses on contract_addresses.address = t.wallet_address

                -- token exclusions
                left join etl_pipelines.ethereum_transfers_exclusions coin_exclusions_eth on coin_exclusions_eth.coin_id = t.coin_id
                left join etl_pipelines.core_transfers_coin_exclusions coin_exclusions on coin_exclusions.coin_id = t.coin_id
                left join etl_pipelines.stables_and_wraps_exclusions coin_exclusions_stables on coin_exclusions_stables.coin_id = t.coin_id

                -- remove the manually excluded addresses
                where wallet_exclusions.wallet_address is null
                and contract_addresses.address is null

                -- remove coin exclusions
                and coin_exclusions_eth.coin_id is null
                and coin_exclusions.coin_id is null
                and coin_exclusions_stables.coin_id is null
            ),


            -- LOGIC TO REMOVE NEGATIVE BALANCE WALLETS AND COINS
            -- --------------------------------------------------
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
            ),


            -- LOGIC TO REMOVE BALANCES OVER TOTAL SUPPLY
            -- ------------------------------------------
            -- These are caused by issues such as out of date total supply in core.coins,
            -- bad total supply or decimals data in coingecko, bridged tokens having
            -- fluctuating total supply, old contracts that have been migrated from,
            -- and probably more causes.
            -- This removes about 750 wallet addresses and 25 coins as of 11/27/24.
            balance_overage_wallets as (
                select cwt.coin_id
                ,cwt.wallet_address
                ,c.total_supply
                ,max(cwt.balance) as highest_balance
                from coin_wallet_transfers_draft cwt
                join core.coins c on c.coin_id = cwt.coin_id
                    and cwt.balance > c.total_supply
                group by 1,2,3
            ),

            balance_overage_coins as (
                select coin_id
                ,count(wallet_address) as overage_wallets
                from balance_overage_wallets
                group by 1
            )

            select cwt.*
            from coin_wallet_transfers_draft cwt

            join negative_wallets nw on nw.coin_id = cwt.coin_id
                and nw.wallet_address = cwt.wallet_address
            join negative_wallets_coins nwc on nwc.coin_id = cwt.coin_id

            left join (
                select coin_id,wallet_address from balance_overage_wallets bow group by 1,2
                ) bow on bow.coin_id = cwt.coin_id
                    and bow.wallet_address = cwt.wallet_address
            left join balance_overage_coins boc on boc.coin_id = cwt.coin_id
                and boc.overage_wallets >= 5

            -- exclude all coin-wallet pairs with negative balances
            where nw.lowest_balance > -0.1

            -- if a coin has more than 10 negative wallets, exclude all coin-wallet pairs
            -- for that coin. there is a buffer of 10 to allow for rounding errors, mint addresses \
            -- that haven't been excluded, etc
            and nwc.negative_wallets < 10

            -- exclude wallet addresses that have ever had any balance over total supply for any coin
            and bow.wallet_address is null

            -- exclude all coins that have ever had 5+ wallets over total supply
            and boc.coin_id is null

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
        """

    counts_df = dgc().run_sql(query_sql)

    return counts_df


def update_wallet_id_table():
    """
    Creates/updates a reference table that maps each wallet_address to a permanent integer ID.
    Preserves existing IDs and only assigns new ones to previously unseen wallet addresses.
    """

    mapping_sql = """
        create table if not exists reference.wallet_ids (
            wallet_address string,
            wallet_id int64,
            updated_at datetime
        )
        cluster by wallet_address;

        insert into reference.wallet_ids (wallet_address, wallet_id, updated_at)
        select
            new_wallets.wallet_address,
            row_number() over (order by new_wallets.wallet_address) +
                coalesce((select max(wallet_id) from reference.wallet_ids), 0) as wallet_id,
            current_datetime('UTC') as updated_at
        from (
            select wallet_address
            from core.coin_wallet_transfers
            group by 1
        ) new_wallets
        left join reference.wallet_ids existing
            on new_wallets.wallet_address = existing.wallet_address
        where existing.wallet_address is null;
        """

    _ = dgc().run_sql(mapping_sql)
    logger.info("Added new wallet addresses to reference.wallet_ids.")



def update_wallet_coin_id_table():
    """
    Creates/updates a reference table that maps each wallet_address + coin_id pair to a permanent integer ID.
    Preserves existing IDs and only assigns new ones to previously unseen wallet-coin pairs.
    """

    mapping_sql = """
        create table if not exists reference.wallet_coin_ids (
            wallet_address string,
            coin_id string,
            hybrid_cw_id int64,
            updated_at datetime
        )
        cluster by wallet_address, coin_id;

        insert into reference.wallet_coin_ids (wallet_address, coin_id, hybrid_cw_id, updated_at)
        select
            new_pairs.wallet_address,
            new_pairs.coin_id,
            row_number() over (order by new_pairs.wallet_address, new_pairs.coin_id) +
                coalesce((select max(hybrid_cw_id) from reference.wallet_coin_ids), 3000000000) as hybrid_cw_id,
            current_datetime('UTC') as updated_at
        from (
            select wallet_address, coin_id
            from core.coin_wallet_transfers
            group by 1, 2
        ) new_pairs
        left join reference.wallet_coin_ids existing
            on new_pairs.wallet_address = existing.wallet_address
            and new_pairs.coin_id = existing.coin_id
        where existing.wallet_address is null;
        """

    _ = dgc().run_sql(mapping_sql)
    logger.info("Added new wallet-coin pairs to reference.wallet_coin_ids.")
