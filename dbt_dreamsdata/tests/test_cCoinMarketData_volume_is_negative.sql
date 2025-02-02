select *
from {{ ref('coin_market_data') }}
where volume < 0
