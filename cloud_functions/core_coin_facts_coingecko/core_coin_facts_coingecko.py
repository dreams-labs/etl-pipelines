'''
cloud function that runs a query to refresh the data in bigquery table core.coin_facts_coingecko
'''
import functions_framework
import dreams_core.core as dc
from dreams_core.googlecloud import GoogleCloud as dgc

# set at logger at the module level
logger = dc.setup_logger()


@functions_framework.http
def rebuild_coin_facts_coingecko(request):  # pylint: disable=unused-argument  # noqa: F841
    '''
    rebuilds the core.coin_facts_coingecko table to incorporate any new metadata
    '''

    query_sql = '''
        create or replace table core.coin_facts_coingecko as (
        with prep_recent_searches as (
        select
            coin_id
            ,coingecko_id
            ,search_successful
            ,search_date
            ,search_log
            ,row_number()over (partition by coingecko_id order by search_date desc) as rn
        from etl_pipelines.coin_coingecko_ids
        where search_successful = true
        )

        ,coingecko_coins as (
        select
            coin_id
            ,coingecko_id
            ,search_successful
            ,search_date
            ,search_log
        from prep_recent_searches where rn =1
        )

        ,prep_cg_metadata as (
        select
            coingecko_id
            ,symbol
            ,name
            ,homepage
            ,twitter_screen_name
            ,telegram_channel_identifier
            ,total_supply
            ,max_supply
            ,circulating_supply
            ,description
            ,updated_at
            ,row_number() over (partition by coingecko_id order by updated_at desc) as rn --to remove duplicates
        from etl_pipelines.coin_coingecko_metadata
        )

        ,cg_metadata as (
        select
            coingecko_id
            ,symbol
            ,name
            ,homepage
            ,twitter_screen_name
            ,telegram_channel_identifier
            ,total_supply
            ,max_supply
            ,circulating_supply
            ,description
            ,updated_at
        from prep_cg_metadata
        where rn = 1
        )

        ,cg_contracts as (
        select
            coingecko_id
            ,coingecko_rank
            ,blockchain
            ,address
            ,decimals
            ,max(updated_at) as latest_updated_at
        from etl_pipelines.coin_coingecko_contracts
        where coingecko_rank = 1
        group by
            coingecko_id,
            coingecko_rank,
            blockchain,
            address,
            decimals
        )

        ,cg_categories as (
        select distinct coingecko_id,category
        from etl_pipelines.coin_coingecko_categories
        where coingecko_rank = 1 and updated_at is not null
        )

        ,stg_coingecko_facts as (
        select
            cc.coin_id
            ,cc.coingecko_id
            ,cm.symbol
            ,cm.name
            ,cm.homepage
            ,cm.twitter_screen_name
            ,cm.telegram_channel_identifier
            ,cm.total_supply
            ,cm.max_supply
            ,cm.circulating_supply
            ,cm.description
            ,cm.updated_at
            ,cgc.blockchain
            ,cgc.address
            ,cgc.decimals
            ,cgct.category
        from coingecko_coins cc
        join cg_metadata cm on cc.coingecko_id = cm.coingecko_id
        join cg_contracts cgc on cgc.coingecko_id = cc.coingecko_id
            and cgc.address is not null
            and cgc.address <> ''
        left join cg_categories cgct on cgct.coingecko_id = cc.coingecko_id
        )

        select * from stg_coingecko_facts
        )
        ;
        '''

    dgc().run_sql(query_sql)
    logger.info('rebuilt core.coin_facts_coingecko.')

    return "rebuild of core.coin_facts_coingecko complete."
