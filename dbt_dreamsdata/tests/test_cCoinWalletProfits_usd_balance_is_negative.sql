select *
from {{ ref('coin_wallet_profits') }}
where usd_balance < -0.01 -- 0.01 allowance for floating point issues
