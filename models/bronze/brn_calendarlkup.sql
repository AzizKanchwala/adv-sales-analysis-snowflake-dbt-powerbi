{{
config(
materialized = 'ephemeral'
)
}}
WITH SOURCE AS (

    SELECT 
        DATE AS DATE_STR
    FROM {{ source('analytics', 'calendarlookup') }}

),

TRANSFORMED AS (

    SELECT
        -- Convert from string (YYYY-MM-DD) to DATE
        TO_DATE(DATE_STR) AS DATE,

        -- Day name (e.g. MONDAY)
        DAYNAME(TO_DATE(DATE_STR)) AS DAYNAME,

        -- Start of week (Monday first day)
        DATEADD(
            DAY,
            1 - DAYOFWEEKISO(TO_DATE(DATE_STR)),
            TO_DATE(DATE_STR)
        ) AS STARTOFWEEK,

        -- Start of month
        DATE_TRUNC(MONTH, TO_DATE(DATE_STR)) AS STARTOFMONTH,

        -- Start of quarter
        DATE_TRUNC(QUARTER, TO_DATE(DATE_STR)) AS STARTOFQUARTER,

        -- Month name (e.g. JANUARY)
        MONTHNAME(TO_DATE(DATE_STR)) AS MONTHNAME,

        -- Month number (1–12)
        MONTH(TO_DATE(DATE_STR)) AS MONTHNUMBER,

        -- Start of year
        DATE_TRUNC(YEAR, TO_DATE(DATE_STR)) AS STARTOFYEAR,

        -- Year number (e.g. 2025)
        YEAR(TO_DATE(DATE_STR)) AS YEAR

    FROM SOURCE

)

SELECT * 
FROM TRANSFORMED
