-- retrieves market data records that were retrieved before the \
-- day had completed

select *
from {{ ref('coin_market_data_coingecko') }} cmd
where cast(updated_at as date) = cmd.date
