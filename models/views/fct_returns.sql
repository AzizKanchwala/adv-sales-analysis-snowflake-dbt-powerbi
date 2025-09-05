{{
config(
materialized = 'view'
)
}}
WITH base AS (
    SELECT *
    FROM {{ ref('slv_returnsfct') }}
)
select * from base