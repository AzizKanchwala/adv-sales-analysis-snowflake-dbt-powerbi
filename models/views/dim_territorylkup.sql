{{
config(
materialized = 'view'
)
}}
WITH base AS (
    SELECT *
    FROM {{ source('analytics','territorylookup') }}
)
select * from base