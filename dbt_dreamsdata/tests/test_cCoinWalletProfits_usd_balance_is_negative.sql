select *
from {{ ref('coin_wallet_profits') }}
where usd_balance < -0.0001 -- 0.0001 allowance for floating point issues
