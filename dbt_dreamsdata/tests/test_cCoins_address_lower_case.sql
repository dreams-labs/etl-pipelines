-- tests/test_address_case_sensitivity.sql
-- retrieves any coins with a non-case sensitive blockchain that have \
-- uppercase characters in their address. this could potentially cause
-- duplicate coin_ids to be made and should not happen.

SELECT c.coin_id
FROM {{ ref('coins') }} c
JOIN {{ ref('chains') }} ch ON ch.chain_id = c.chain_id
WHERE ch.is_case_sensitive = False
AND c.address <> LOWER(c.address)
