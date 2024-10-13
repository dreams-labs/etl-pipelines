with ordered_profits as (
    select
        coin_id,
        wallet_address,
        date,
        usd_inflows_cumulative,
        lag(usd_inflows_cumulative) over (
            partition by coin_id, wallet_address
            order by date
        ) as prev_usd_inflows_cumulative
    from {{ ref('coin_wallet_profits') }}
),

-- selects any records where inflows are less than previous inflows
validation_check as (
    select *
    from ordered_profits
    -- 0.0001 allowance for floating point issues
    where usd_inflows_cumulative < (least(prev_usd_inflows_cumulative * -0.0001, -0.001))
    and prev_usd_inflows_cumulative is not null
)

select *
from validation_check