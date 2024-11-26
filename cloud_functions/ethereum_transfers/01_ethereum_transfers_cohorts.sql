-- these queries loads cohorts into etl_pipelines.ethereum_transfers_cohorts table

-- COHORT 1: 204 coins
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


-- COHORT 2: 1118 coins
insert into etl_pipelines.ethereum_transfers_cohorts (

    select 2 as cohort_number
    ,'coingecko_all_coins rank 600-3000 + all dune eth coins' as cohort_description
    ,c.coin_id
    ,c.coingecko_id
    ,c.chain
    ,c.address
    ,current_datetime("UTC") as created_at
    ,c.decimals
    from core.coins c
    left join (
    select token_address
    from etl_pipelines.ethereum_net_transfers t
    group by 1
    ) t on t.token_address = c.address
    left join etl_pipelines.ethereum_transfers_exclusions excl on excl.coin_id = c.coin_id

    -- only include coins on eth
    where chain = 'Ethereum'

    -- decimals are needed in the etl pipelines
    and c.decimals is not null

    -- only include coins that aren't already in the eth transfers table
    and t.token_address is null

    -- exclude the exclusion list
    and excl.coin_id is null

)


-- COHORT 3: 1329 coins
insert into etl_pipelines.ethereum_transfers_cohorts (

    select 3 as cohort_number
    ,'coingecko_all_coins rank up to 5000 as of nov 25 2024' as cohort_description
    ,c.coin_id
    ,c.coingecko_id
    ,c.chain
    ,c.address
    ,current_datetime("UTC") as created_at
    ,c.decimals
    from core.coins c
    left join (
        select token_address
        from etl_pipelines.ethereum_net_transfers t
        group by 1
    ) t on t.token_address = c.address
    left join etl_pipelines.ethereum_transfers_exclusions excl on excl.coin_id = c.coin_id

    -- only include coins on eth
    where chain = 'Ethereum'

    -- decimals are needed in the etl pipelines
    and c.decimals is not null

    -- only include coins that aren't already in the eth transfers table
    and t.token_address is null

    -- exclude the exclusion list
    and excl.coin_id is null

)


-- COHORT 4: 592 coins
insert into etl_pipelines.ethereum_transfers_cohorts (

    select 4 as cohort_number
    ,'coingecko_all_coins rank up to 10000 as of nov 26 2024' as cohort_description
    ,c.coin_id
    ,c.coingecko_id
    ,c.chain
    ,c.address
    ,current_datetime("UTC") as created_at
    ,c.decimals
    from core.coins c
    left join (
        select token_address
        from etl_pipelines.ethereum_net_transfers t
        group by 1
    ) t on t.token_address = c.address
    left join etl_pipelines.ethereum_transfers_exclusions excl on excl.coin_id = c.coin_id

    -- only include coins on eth
    where chain = 'Ethereum'

    -- decimals are needed in the etl pipelines
    and c.decimals is not null

    -- only include coins that aren't already in the eth transfers table
    and t.token_address is null

    -- exclude the exclusion list
    and excl.coin_id is null

)
