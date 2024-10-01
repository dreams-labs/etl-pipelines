-- Confirms that there are no 0 prices
select *
from {{ ref('coin_market_data') }}
where price = 0