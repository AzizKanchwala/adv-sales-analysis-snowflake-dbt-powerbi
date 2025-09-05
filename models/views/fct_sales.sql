{{
config(
materialized = 'view'
)
}}
WITH base AS (
    SELECT *
    FROM {{ ref('slv_salesfct') }}
)
select * from base