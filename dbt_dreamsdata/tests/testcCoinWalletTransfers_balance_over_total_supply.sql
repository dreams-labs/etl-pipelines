with current_balances as (
    select *
    ,row_number() over (partition by coin_id,wallet_address order by date desc) as rn
    from {{ ref('coin_wallet_transfers') }} cwt
),

overages as (
    select cwt.*
    ,c.total_supply
    ,c.chain
    from current_balances cwt
    join {{ ref('coins') }} c on c.coin_id = cwt.coin_id
    where cwt.rn = 1
    and cwt.balance > c.total_supply
)

select *
from overages
