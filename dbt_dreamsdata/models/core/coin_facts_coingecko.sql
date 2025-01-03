{{ config(schema=var('target_schema', 'core')) }}

SELECT *
FROM `western-verve-411004.{{ var('target_schema', 'core') }}.coin_facts_coingecko`