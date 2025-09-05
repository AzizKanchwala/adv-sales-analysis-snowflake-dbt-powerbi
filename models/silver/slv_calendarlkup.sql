WITH base AS (
    -- Pull everything from the bronze table
    SELECT *
    FROM {{ ref('brn_calendarlkup') }}
)

SELECT
    *,
    -- 1. First 3 characters of MonthName in uppercase
    UPPER(LEFT(MonthName, 3)) AS MonthShort,

    -- 2. Day of week number (Monday = 1)
    DAYOFWEEKISO(Date) AS DayOfWeek,

    -- 3. Weekend or Weekday
    CASE 
        WHEN DAYOFWEEKISO(Date) IN (6, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS Weekend
FROM base