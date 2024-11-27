
select *
from {{ ref('coin_wallet_transfers') }} cwt
where balance < -0.1
