"""
cloud function that loads some coins from the coingecko_all_coins tables into the
coingecko_all_coins_intake_queue table, where they will be added to core.coins by the
standard ETL workflow.
"""
from dreams_core.googlecloud import GoogleCloud as dgc


def add_coins_to_queue(min_rank,max_rank):
    """
    Adds coins to etl_pipelines.coingecko_all_coins_intake_queue based on their market
    cap rank.

    Params:
        max_rank (int): the highest allowed market cap rank per coingecko
        min_rank (int): the lowest allowed market cap rank per coingecko
    """
    query_sql = f"""
        insert into etl_pipelines.coingecko_all_coins_intake_queue (

            with all_coins_deduped as (
                select *
                from (
                    select *
                    ,row_number() over (partition by id order by created_at desc) as rn
                    from `etl_pipelines.coingecko_all_coins` ac
                )
                where rn=1
            )

            ,market_summary_deduped as (
                select *
                ,coalesce(
                  market_cap_rank,
                  row_number() over (order by fully_diluted_valuation desc, total_volume desc)
                 ) as imputed_rank
                from (
                    select *
                    ,row_number() over (partition by id order by created_at desc) as rn
                    from `etl_pipelines.coingecko_all_coins_market_summary` ac
                )
                where rn=1
            )

            SELECT
                ac.id as coingecko_id,
                ac.contract_addresses[OFFSET(0)].blockchain AS blockchain,
                ac.contract_addresses[OFFSET(0)].contract_address AS address,
                ac.created_at as source_date
            from all_coins_deduped ac
            join market_summary_deduped ms on ac.id = ms.id
            left join core.coins c on c.coingecko_id = ac.id
            left join etl_pipelines.coingecko_all_coins_intake_queue iq on iq.coingecko_id = ac.id

            -- ensure there is at least one contract in the array
            where ARRAY_LENGTH(contract_addresses) > 0

            -- don't intake coingecko_ids already associated with a coin
            and c.coingecko_id is null

            -- don't intake coingecko_ids we've already put in the queue
            and iq.coingecko_id is null
            -- market cap filter
            and ms.imputed_rank between {min_rank} and {max_rank}

        )
        """

    dgc().run_sql(query_sql)
