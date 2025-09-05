{{
config(
materialized = 'view'
)
}}
WITH base AS (
    SELECT *
    FROM {{ source('analytics','productsubcategorylookup') }}
)
select * from base