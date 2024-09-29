-- this query loads the first cohort into etl_pipelines.ethereum_transfers_cohorts

insert into etl_pipelines.ethereum_transfers_cohorts (

    -- these need to be manually iterated
    select 1 as cohort_number
    ,'initial coingecko_all_coins cohort (coins rank 100-600)' as cohort_description
    ,c.coin_id
    ,c.coingecko_id
    ,c.chain
    ,c.address
    ,current_datetime("UTC") as created_at
    ,c.decimals
    from core.coins c
    left join etl_pipelines.ethereum_transfers_exclusions excl on excl.coin_id = c.coin_id
    where chain = 'Ethereum'
    and has_wallet_transfer_data = False
    and excl.coin_id is null
    and c.decimals is not null
    and c.decimals > 0

)
