{{ config(severity='error') }}

with ordered_transfers as (
    select
        coin_id,
        wallet_address,
        date,
        transfer_sequence,
        lag(transfer_sequence) over (
            partition by coin_id, wallet_address
            order by date
        ) as prev_transfer_sequence
    from {{ ref('coin_wallet_transfers') }}
),

validation_check as (
    select *
    from ordered_transfers
    where transfer_sequence <= prev_transfer_sequence
        and prev_transfer_sequence is not null
)

select *
from validation_check