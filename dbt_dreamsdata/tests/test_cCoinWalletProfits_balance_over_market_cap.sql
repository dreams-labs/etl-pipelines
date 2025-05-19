-- Coins that have many wallets with USD balance above the market cap are
--  usually caused by bad market cap data but could use more review
--  e.g. market cap chart of https://www.coingecko.com/en/coins/pixer-eternity
--  e.g. market cap chart of https://www.coingecko.com/en/coins/tokoin
{{ config(
    severity = 'warn'
) }}

select cwp.coin_id
from {{ ref('coin_wallet_profits') }} cwp
join {{ ref('coin_market_data') }} cmd on cmd.coin_id = cwp.coin_id and cmd.date = cwp.date
where cwp.usd_balance > cmd.market_cap
and cmd.market_cap > 0
group by 1
