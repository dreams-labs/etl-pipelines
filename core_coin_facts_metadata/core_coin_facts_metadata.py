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
        SELECT
            COALESCE(cg.coin_id, gt.coin_id) AS coin_id,
            COALESCE(cg.coingecko_id, gt.coingecko_coin_id) AS coingecko_id,
            gt.geckoterminal_id,
            COALESCE(cg.symbol, gt.symbol) AS symbol,
            COALESCE(cg.name, gt.name) AS name,

            -- Coalesce with logic to prevent 0 values from overwriting non-zero values
            CASE
                WHEN cg.total_supply IS NOT NULL AND cg.total_supply != 0 THEN cg.total_supply
                ELSE gt.total_supply
            END AS total_supply,

            CASE
                WHEN cg.decimals IS NOT NULL AND cg.decimals != 0 THEN cg.decimals
                ELSE gt.decimals
            END AS decimals,

            COALESCE(cg.description, gt.description) AS description,

            -- Keep fields exclusive to Table 1
            gt.gt_score,
            gt.websites,
            gt.top_pools,

            -- Coalescing Twitter handle fields
            COALESCE(cg.twitter_screen_name, gt.twitter_handle) AS twitter_handle,

            -- Coalescing Telegram handle fields
            COALESCE(cg.telegram_channel_identifier, gt.telegram_handle) AS telegram_handle,

            -- Data lineage booleans
            CASE WHEN cg.coin_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_coingecko_metadata,
            CASE WHEN gt.coin_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_geckoterminal_metadata

        FROM
            core.coin_facts_coingecko cg
        FULL OUTER JOIN
            etl_pipelines.coin_geckoterminal_metadata gt
        ON
            cg.coin_id = gt.coin_id
        '''

    dgc().run_sql(query_sql)
    logger.info('rebuilt core.coin_facts_metadata.')

    return "rebuild of core.coin_facts_metadata complete."
