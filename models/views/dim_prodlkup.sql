{{
config(
materialized = 'view'
)
}}
WITH base AS (
    SELECT *
    FROM {{ ref('slv_prodlkup') }}
)
select * from base