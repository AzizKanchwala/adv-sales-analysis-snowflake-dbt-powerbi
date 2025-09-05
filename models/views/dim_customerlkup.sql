{{
config(
materialized = 'view'
)
}}
WITH base AS (
    SELECT *
    FROM {{ ref('slv_customerlkup') }}
)
select * from base