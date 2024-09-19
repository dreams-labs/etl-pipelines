-- tests/test_coin_market_data_coin_ids.sql
SELECT
    coin_id
FROM
    {{ ref('coin_market_data') }}
WHERE
    coin_id NOT IN (
        SELECT coin_id
        FROM {{ ref('coins') }}
    )