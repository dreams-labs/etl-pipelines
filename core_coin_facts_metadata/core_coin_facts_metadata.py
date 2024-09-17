'''
cloud function that runs a query to refresh the data in bigquery table core.coin_facts_metadata
'''
import functions_framework
import dreams_core.core as dc
from dreams_core.googlecloud import GoogleCloud as dgc

# set up logger at the module level
logger = dc.setup_logger()


@functions_framework.http
def rebuild_coin_facts_metadata(request):  # pylint: disable=unused-argument  # noqa: F841
    '''
    rebuilds the core.coin_facts_metadata table to incorporate any new metadata
    '''

    query_sql = '''
        CREATE OR REPLACE TABLE `core.coin_facts_metadata` AS (

        WITH
        -- CTE to pull and rank rows from coin_geckoterminal_metadata
        prep_geckoterminal_metadata AS (
        SELECT
            coingecko_coin_id as coingecko_id,
            description,
            websites,
            gt_score,
            discord_url,
            telegram_handle,
            twitter_handle,
            row_number() OVER (PARTITION BY coingecko_coin_id ORDER BY retrieval_date DESC) AS rn
        FROM etl_pipelines.coin_geckoterminal_metadata
        ),

        -- CTE to select only the latest metadata entry per coingecko_id
        latest_geckoterminal_metadata AS (
        SELECT
            coingecko_id,
            description,
            websites,
            gt_score,
            discord_url,
            telegram_handle,
            twitter_handle
        FROM prep_geckoterminal_metadata
        WHERE rn = 1
        ),

        -- CTE to pull data from core.coin_facts_coingecko
        core_coin_facts AS (
        SELECT
            coin_id,
            coingecko_id,
            symbol,
            name,
            homepage,
            twitter_screen_name,
            telegram_channel_identifier,
            total_supply,
            max_supply,
            circulating_supply,
            updated_at,
            blockchain,
            address,
            decimals,
            category
        FROM core.coin_facts_coingecko
        )

        SELECT
        core.coin_id,
        core.coingecko_id,
        core.symbol,
        core.name,
        core.homepage,
        core.twitter_screen_name,
        core.telegram_channel_identifier,
        core.total_supply,
        core.max_supply,
        core.circulating_supply,
        core.updated_at AS core_updated_at,
        core.blockchain,
        core.address,
        core.decimals,
        core.category,
        gt.description,
        gt.websites,
        gt.gt_score,
        gt.discord_url,
        gt.telegram_handle,
        gt.twitter_handle
        FROM core_coin_facts core
        LEFT JOIN latest_geckoterminal_metadata gt
        ON core.coingecko_id = gt.coingecko_id

        )
        '''

    dgc().run_sql(query_sql)
    logger.info('rebuilt core.coin_facts_metadata.')

    return "rebuild of core.coin_facts_metadata complete."
