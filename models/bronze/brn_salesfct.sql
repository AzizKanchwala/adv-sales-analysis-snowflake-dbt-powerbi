{{
config(
materialized = 'ephemeral'
)
}}
WITH SOURCE AS (

    SELECT
        TRY_TO_DATE(ORDERDATE, 'YYYY-MM-DD') AS ORDERDATE,
        TRY_TO_DATE(STOCKDATE, 'YYYY-MM-DD') AS STOCKDATE,
        ORDERNUMBER,
        PRODUCTKEY,
        CUSTOMERKEY,
        TERRITORYKEY,
        ORDERLINEITEM,
        ORDERQUANTITY
    FROM {{ source('analytics', 'salesfact') }}

),

TRANSFORMED AS (

    SELECT
        *,
        'Adv Works Sales Data ' 
            || TO_VARCHAR(YEAR(TO_DATE(ORDERDATE))) 
            || '.csv' AS SOURCENAME
    FROM SOURCE

)

SELECT *
FROM TRANSFORMED