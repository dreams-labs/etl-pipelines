-- tests/no_token_address_overlap.sql
with ethereum_transfers_cohorts as (
    select token_address
    from etl_pipelines.ethereum_net_transfers
    group by 1
),
coin_wallet_net_transfers as (
    select token_address
    from etl_pipelines.coin_wallet_net_transfers
    where chain_text_source = 'ethereum'
    group by 1
)
select token_address
from ethereum_transfers_cohorts
where token_address in (select token_address from coin_wallet_net_transfers)
