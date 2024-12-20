create or replace table reference.dev_coins as (
    with ranked_coins as (
        select
            c.coin_id,
            c.has_wallet_transfer_data,
            -- Generate a reproducible random hash using a seed
            row_number() over (
                partition by has_wallet_transfer_data
                order by farm_fingerprint(concat('seed_42', c.coin_id))
            ) as rn
        from
            core.coins c
    )
    select
        current_datetime('UTC') as cohort_created_at, -- Timestamp for cohort creation
        c.*
    from
        core.coins c
    join
        ranked_coins r
        on r.coin_id = c.coin_id
    where
        (
            (c.has_wallet_transfer_data = True and r.rn <= 200) -- Select 200 with transfer data
            or
            (c.has_wallet_transfer_data = False and r.rn <= 50) -- Select 50 without transfer data
        )
);
