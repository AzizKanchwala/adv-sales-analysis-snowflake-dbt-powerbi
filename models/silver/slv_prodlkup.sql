WITH base AS (
    SELECT *
    FROM {{ ref('brn_prodlkup') }}
)

SELECT
    *,
    
    -- 1. PricePoint based on ProductPrice
    CASE
        WHEN ProductPrice > 500 THEN 'High'
        WHEN ProductPrice > 100 THEN 'Mid-Range'
        ELSE 'Low'
    END AS PricePoint,
    
    -- 2. FirstPartBeforeHyphen from ProductSKU
    CASE
        WHEN POSITION('-' IN ProductSKU) > 0 THEN LEFT(ProductSKU, POSITION('-' IN ProductSKU) - 1)
        ELSE ProductSKU
    END AS FirstPartBeforeHyphen

FROM base