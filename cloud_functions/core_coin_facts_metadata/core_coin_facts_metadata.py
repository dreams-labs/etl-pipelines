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
        CREATE OR REPLACE TABLE core.coin_facts_metadata AS (

            -- find the coin_id associated with each geckoterminal_id
            with geckoterminal_coin_ids as (
                select id.geckoterminal_id
                ,id.coin_id
                ,row_number() over (partition by id.geckoterminal_id order by search_date desc) as rn
                from `etl_pipelines.coin_geckoterminal_ids` id
                join core.coins c on c.coin_id = id.coin_id
                where search_successful = True
            ),

            -- retrieve coin_id-keyed geckoterminal metadata
            geckoterminal_metadata as (
                select id.coin_id
                ,gt.geckoterminal_id
                ,gt.coingecko_coin_id
                ,gt.symbol
                ,gt.name
                ,gt.total_supply
                ,gt.decimals
                ,gt.description
                ,gt.gt_score
                ,gt.twitter_handle
                ,gt.telegram_handle

                -- complicated syntax to extract the first string from the array while handling nulls
                ,IF(
                    gt.websites IS NOT NULL AND ARRAY_LENGTH(gt.websites) > 0
                    ,gt.websites[OFFSET(0)]
                    ,NULL
                    ) AS website
                -- complicated syntax to exctract the first struct in the array while handling nulls
                ,(
                    SELECT IF (
                        ARRAY_LENGTH(gt.top_pools) > 0,
                            (SELECT elem.id FROM UNNEST(gt.top_pools) AS elem LIMIT 1)
                        ,NULL)
                ) AS top_pool

                from etl_pipelines.coin_geckoterminal_metadata gt
                join geckoterminal_coin_ids id on id.geckoterminal_id = gt.geckoterminal_id
                where id.rn = 1
            )

            SELECT
                COALESCE(cg.coin_id, gt.coin_id) AS coin_id
                ,cg.coingecko_id  -- coalescing with gt.coingecko_coin_id creates duplicates
                ,gt.geckoterminal_id
                ,COALESCE(cg.symbol, gt.symbol) AS symbol
                ,COALESCE(cg.name, gt.name) AS name

                -- Coalesce with logic to prevent 0 values from overwriting non-zero values
                ,CAST(
                    CASE
                        WHEN cg.total_supply IS NOT NULL AND cg.total_supply != 0 THEN cg.total_supply
                        ELSE gt.total_supply
                    END AS numeric) AS total_supply

                ,CASE
                    WHEN cg.decimals IS NOT NULL AND cg.decimals != 0 THEN cg.decimals
                    ELSE gt.decimals
                END AS decimals

                ,COALESCE(cg.description, gt.description) AS description
                ,COALESCE(cg.homepage, gt.website) AS homepage

                -- Keep fields exclusive to Geckoterminal
                ,gt.gt_score
                ,gt.top_pool

                -- Coalescing Twitter handle fields
                ,COALESCE(cg.twitter_screen_name, gt.twitter_handle) AS twitter_handle

                -- Coalescing Telegram handle fields
                ,COALESCE(cg.telegram_channel_identifier, gt.telegram_handle) AS telegram_handle

                -- Data lineage booleans
                ,CASE WHEN cg.coin_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_coingecko_metadata
                ,CASE WHEN gt.coin_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_geckoterminal_metadata

            FROM
                core.coin_facts_coingecko cg
            FULL OUTER JOIN
                geckoterminal_metadata gt
            ON
                cg.coin_id = gt.coin_id
        )
        '''

    dgc().run_sql(query_sql)
    logger.info('rebuilt core.coin_facts_metadata.')

    return "rebuild of core.coin_facts_metadata complete."
