-- Confirms that each coin has a record for each date. We know that each record is unique
-- on coin_id-date because of the schema test on unique_combination_of_columns
with coin_data as (
    select
        coin_id,
        date
    from {{ ref('coin_market_data') }}
),

coin_dates as (
    select
        coin_id,
        datetime_diff(max(date), min(date), day)+1 as expected_days,
        count(date) as actual_days
    from coin_data
    group by coin_id
)

select *
from coin_dates
where expected_days <> actual_days
