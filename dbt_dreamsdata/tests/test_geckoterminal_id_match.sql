-- tests/test_geckoterminal_id_match.sql
SELECT
    geckoterminal_id
FROM
    {{ ref('coin_geckoterminal_metadata') }}
WHERE
    geckoterminal_id NOT IN (
        SELECT geckoterminal_id
        FROM {{ ref('coin_geckoterminal_ids') }}
    )