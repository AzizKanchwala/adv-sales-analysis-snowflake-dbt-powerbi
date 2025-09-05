{{
config(
materialized = 'ephemeral'
)
}}
WITH SOURCE AS (
    SELECT *
    FROM {{ source('analytics', 'customerlookup') }}
),

CLEANED AS (
    SELECT
        *,
        -- Convert BirthDate to DATE safely using the correct format
        TRY_TO_DATE(BIRTHDATE, 'DD/MM/YY') AS BIRTHDATE_DT,

        -- Convert CUSTOMERKEY to NUMBER safely, remove invalid rows later
        TRY_TO_NUMBER(CUSTOMERKEY) AS CUSTOMERKEY_NUM
    FROM SOURCE
),

FILTERED AS (
    SELECT *
    FROM CLEANED
    -- Remove rows where CUSTOMERKEY conversion failed or null/blank
    WHERE CUSTOMERKEY_NUM IS NOT NULL
),

TRANSFORMED AS (
    SELECT
        -- Overwrite CUSTOMERKEY with numeric version
        CUSTOMERKEY_NUM AS CUSTOMERKEY,
        -- Capitalize first letter only
        INITCAP(FIRSTNAME) AS FIRSTNAME,
        INITCAP(LASTNAME) AS LASTNAME,
        -- Use converted BIRTHDATE
        BIRTHDATE_DT AS BIRTHDATE,
        MARITALSTATUS,
        GENDER,
        EMAILADDRESS,
        ANNUALINCOME,
        TOTALCHILDREN,
        EDUCATIONLEVEL,
        OCCUPATION,
        HOMEOWNER,
        -- Create FULLNAME: PREFIX + FIRSTNAME + LASTNAME (PREFIX may be null)
        TRIM(
            CONCAT(
                IFNULL(PREFIX, ''), 
                CASE WHEN IFNULL(PREFIX, '') != '' THEN ' ' ELSE '' END,
                INITCAP(FIRSTNAME),
                ' ',
                INITCAP(LASTNAME)
            )
        ) AS FULLNAME,
        -- Extract domain name from EmailAddress, capitalize first letter of each word, replace '-' with space
        TRIM(
            REPLACE(
                INITCAP(
                    REGEXP_SUBSTR(
                        EMAILADDRESS, 
                        '@([^\\.]+)\\.', 1, 1, 'e', 1
                    )
                ), '-', ' '
            )
        ) AS DOMAINNAME
    FROM FILTERED
)

SELECT *
FROM TRANSFORMED