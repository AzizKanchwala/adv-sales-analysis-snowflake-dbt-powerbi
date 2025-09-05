{{
config(
materialized = 'view'
)
}}
WITH base AS (
    SELECT *
    FROM {{ ref('slv_calendarlkup') }}
)
select * from base