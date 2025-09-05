{{
config(
materialized = 'view'
)
}}
WITH base AS (
    SELECT *
    FROM {{ source('analytics','productcategorylookup') }}
)
select * from base