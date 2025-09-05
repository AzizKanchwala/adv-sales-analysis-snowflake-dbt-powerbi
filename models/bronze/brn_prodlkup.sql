{{
config(
materialized = 'ephemeral'
)
}}
WITH SOURCE AS (

    SELECT *
    FROM {{ source('analytics', 'productlookup') }}

),

TRANSFORMED AS (

    SELECT
        *,
        CASE 
            WHEN REGEXP_COUNT(PRODUCTSKU, '-') >= 2 
                THEN REGEXP_SUBSTR(PRODUCTSKU, '^[^-]+-[^-]+')
            ELSE PRODUCTSKU
        END AS SKUTYPE,

        PRODUCTPRICE * 0.9 AS DISCOUNTPRICE

    FROM SOURCE

)
SELECT * 
FROM TRANSFORMED
