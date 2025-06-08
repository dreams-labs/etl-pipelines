/*
THIS TEST DOES NOT COVER PROFITS CALULATIONS IN THE IMPUTED COIN_WALLET_PROFITS ROWS.
These records are imputed on the first price date when the first transfer happens before
price data is available. As these records exist in coin_wallet_profits but not
coin_wallet_transfers, they will be removed by the cwt to cwp join.

Tests alignment of USD balance calculations with profits and transfers. Logic flow:
1. Samples 300 random coins to limit compute
2. Establishes data freshness cutoff (10 days from latest data) to avoid imputed prices
3. Joins core tables and calculates lagged values for comparison
4. Computes expected vs actual values for:
   - Profits change: (prev_balance * price_change)
   - USD balance: (prev_balance + transfers + profits)
5. Flags mismatches where differences exceed both $1 and 1% threshold
*/

-- select random coins to test
with coin_sample as (
    select coin_id
    from {{ ref('coins') }}
    ORDER BY farm_fingerprint(concat('seed42', coin_id))
    limit 300 -- limit determines the sample size
)

-- select data that is at least 10 days old. if coin_market_data has been updated since
-- coin_wallet_profits was last calculated, profits will appear to be wrong because they
-- were built using imputed prices. this filters out the most recent 10 days of records
-- from the check to avoid including these rows
,freshness_state as (
    select least(most_recent_transfer,most_recent_price) - interval 10 day as freshness_date
    from (
        select max(date) as most_recent_transfer
        from {{ ref('coin_wallet_transfers') }}
    ) cwt
    cross join (
        select max(date) as most_recent_price
        from {{ ref('coin_market_data') }}
        where days_imputed is null
    ) cmd
)

-- extract values from the core table and add offsets
,recalc_base as (
    select cwp.coin_id
    ,cwp.wallet_address
    ,cwp.date
    ,cwt.net_transfers as token_net_transfers
    ,cwt.balance as token_balance
    ,cmd.price
    ,round(cwp.usd_net_transfers,2) as usd_net_transfers
    ,round(cwp.usd_balance,2) as usd_balance
    ,round(cwp.profits_change,2) as profits_change
    ,lag(cwt.balance,1) over (partition by cwp.coin_id,cwp.wallet_address order by cwp.date) as prev_token_balance
    ,round(lag(cwp.usd_balance,1) over (partition by cwp.coin_id,cwp.wallet_address order by cwp.date),2) as prev_usd_balance
    ,lag(cmd.price,1) over (partition by cmd.coin_id,cwp.wallet_address order by cmd.date) as prev_price

    -- only check values for the coin sample to limit memory and compute usage
    from coin_sample
    join {{ ref('coin_wallet_profits') }} cwp on cwp.coin_id = coin_sample.coin_id
    join {{ ref('coin_wallet_transfers') }} cwt on cwt.coin_id = cwp.coin_id
        and cwt.date = cwp.date
        and cwt.wallet_address = cwp.wallet_address
    join {{ ref('coin_market_data') }} cmd on cmd.coin_id = cwp.coin_id
        and cmd.date = cwp.date

    -- only check records older than 10 days to avoid data freshness discrepancies
    join freshness_state on freshness_state.freshness_date > cmd.date
    order by cwp.coin_id,cwp.wallet_address,cwp.date
)

-- recalculate expected profits-change and usd_balance
,recalc_diff_values as (
    select coin_id
    ,wallet_address
    ,date
    ,token_net_transfers
    ,token_balance
    ,prev_token_balance
    ,price
    ,prev_price
    ,profits_change

    -- (previous token balance) * (change in token price)
    ,round(prev_token_balance * (price - prev_price),2) as expected_profits_change
    ,round(profits_change - (prev_token_balance * (price - prev_price)),2) as profits_change_diff

    ,usd_balance
    ,usd_net_transfers
    ,prev_usd_balance

    -- (previous usd balance) + (new usd transfers) + (change in profits due to price movement)
    ,round(prev_usd_balance + usd_net_transfers + profits_change,2) as expected_usd_balance
    ,round(usd_balance - (prev_usd_balance + usd_net_transfers + profits_change),2) as usd_balance_diff

    from recalc_base
)


,recalc_diff_pcts as (
    select *
    ,round(case when abs(profits_change) > 0 then (profits_change_diff / profits_change) end, 4) as profits_change_diff_pct

    -- balance diff is compared based on the previous balance
    -- example scenario:
    --     a wallet had $10M and sold all at a $2M loss. expected_profits_calc is $2M+$1
    --     diff calc is $1/$10M (prev balance) rather than $1/$0 (current balance)
    ,round(case when prev_usd_balance > 0 then (usd_balance_diff / prev_usd_balance) end, 4) as usd_balance_diff_pct

    from recalc_diff_values
)

select *
from recalc_diff_pcts
where (
    ( -- select rows where usd calculations were off by more than $1 and 1%
        abs(usd_balance_diff) > 1
        AND (abs(usd_balance_diff / greatest(expected_usd_balance,prev_usd_balance)) > 0.01)
    )
OR
    case when profits_change != 0 then
        ( -- select rows where profits_change was off by more than $1 and 1%
            abs(profits_change_diff) > 1
            AND (abs(profits_change_diff / profits_change) > 0.01)
        )
    else False end
)