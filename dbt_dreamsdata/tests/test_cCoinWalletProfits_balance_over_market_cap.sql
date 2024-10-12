select cwp.coin_id
,cwp.wallet_address
,cwp.usd_balance
,cmd.market_cap
from {{ ref('coin_wallet_profits') }} cwp
join {{ ref('coin_market_data') }} cmd on cmd.coin_id = cwp.coin_id and cmd.date = cwp.date
where cwp.usd_balance > cmd.market_cap
