-- generates the list of token-level exclusions that will be applied to both the ethereum \
-- tables and core.coin_wallet_transfers

create or replace view etl_pipelines.ethereum_transfers_exclusions as (

select c.coin_id
,c.coingecko_id
from core.coins c
join (
    select coingecko_id
    ,array_agg(category)
    from `etl_pipelines.coin_coingecko_categories` cgc
    where category in (
        -- stablecoins
        'Algorithmic Stablecoin'
        ,'Crypto-backed Stablecoin'
        ,'EUR Stablecoin'
        ,'Fiat-backed Stablecoin'
        ,'Stablecoin Protocol'
        ,'Stablecoins'
        ,'USD Stablecoin'

        -- bridged, wrapped, or staked large cap tokens
        ,'Bridged DAI'
        ,'Bridged USDC'
        ,'Bridged USDT'
        ,'Bridged WBTC'
        ,'Bridged WETH'
        ,'Wrapped-Tokens'
        ,'cToken'
        ,'Eth 2.0 Staking'
        ,'Liquid Restaked SOL'
        ,'Liquid Restaking Tokens'
        ,'Liquid Staked ETH'
        ,'Liquid Staked SOL'
        ,'Liquid Staking Tokens'
        ,'Tokenized BTC'
        ,'Tokenized Gold'
        ,'Tokenized Treasury Bonds (T-Bonds)'

        -- abnormal token types
        ,'Centralized Exchange (CEX)'
    )
    or coingecko_id in (
        -- major projects that are likely to have lots of transfers \
        -- and behave differently than smallcaps
        'safe'
        ,'api3'
        ,'1inch'
        ,'snx'
        ,'inst'
        ,'lqty'
        ,'zro'
        ,'ssv'
        ,'gfi'
        ,'ohm'
        ,'cvx'
        ,'yfi'
        ,'gno'
        ,'uma'
        ,'nexo'
        ,'bal'
        ,'pzeth'
        ,'eth+'
        ,'unieth'
        ,'ethdydx'
        ,'ethfi'
        ,'rpl'
        ,'ageth'
        ,'nxra'
        ,'WZANO'
        ,'GNUS'
        ,'btse'
        ,'wexo'
        ,'ampl'
        ,'ens'
        ,'mana'
        ,'compound-governance-token'
        ,'shiba-inu'
        ,'curve-dao-token'
    )
    group by 1
) exclusions on exclusions.coingecko_id = c.coingecko_id
group by 1,2

)