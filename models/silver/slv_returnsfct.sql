{{
config(
materialized = 'incremental',
on_schema_change='fail'
)
}}
WITH base AS (
    SELECT
        TRY_TO_DATE(RETURNDATE, 'YYYY-MM-DD') AS RETURNDATE,
        TERRITORYKEY,
        PRODUCTKEY,
        RETURNQUANTITY
    FROM {{ source('analytics', 'returnsfact') }}
)
select * from base b
{% if is_incremental() %}
WHERE b.returndate > (select max(returndate) from {{ this }})
{% endif %}