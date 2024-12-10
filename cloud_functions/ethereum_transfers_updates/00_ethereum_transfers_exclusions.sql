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

        -- Stablecoins and pegged assets
        ,'crvusd'
        ,'eurc'
        ,'lusd'
        ,'pyusd'
        ,'tusd'
        ,'usd0'

        -- Wrapped tokens
        ,'wbeth'
        ,'cbeth'
        ,'sweth'
        ,'steth'
        ,'reth'
        ,'cbbtc'
        ,'wbtc'
        ,'tbtc'
        ,'wtlos'

        -- Liquid staking derivatives
        ,'rseth'
        ,'sfrxeth'
        ,'stfx'
        ,'ankr'
        ,'lseth'

        -- Governance tokens of major protocols
        ,'aave'
        ,'comp'
        ,'mkr'
        ,'ldo'
        ,'crv'
        ,'bal'
        ,'uni'
        ,'1inch'

        -- Exchange tokens
        ,'ht'
        ,'ftx'
        ,'okb'
        ,'kcs'

        -- Tokens with unusual mechanisms
        ,'ampl'

        -- Large cap or well-established projects
        ,'link'
        ,'grt'
        ,'matic'
        ,'ftm'

        -- Synthetic assets (including specific ones found in the list)
        ,'seth'
        ,'sbtc'

        -- Others
        ,'xaut'
        ,'paxg'
        ,'ceth'
        ,'cdai'
        ,'crvusd'

        -- Additional tokens from the provided list that fit exclusion criteria
        ,'meveth'
        ,'compound-chainlink-token'
        ,'compound-basic-attention-token'
        ,'compound-uniswap'
        ,'compound-usd-coin'
        ,'compound-wrapped-btc'
        ,'compound-0x'
        ,'eth-2x-flexible-leverage-index'
        ,'btc-2x-flexible-leverage-index'
        ,'kyber-network'
        ,'omisego'
        ,'republic-protocol'
        ,'funfair'
        ,'pundi-x'
        ,'ethlend'
    )
    group by 1
) exclusions on exclusions.coingecko_id = c.coingecko_id
group by 1,2