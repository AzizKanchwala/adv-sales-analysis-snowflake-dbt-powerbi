{{
config(
materialized = 'incremental',
on_schema_change='fail'
)
}}
WITH base AS (
    SELECT *
    FROM {{ ref('brn_salesfct') }}
),

prod AS (
    -- Get Product info from silver product lookup
    SELECT
        ProductKey,
        ProductPrice as RetailPrice
    FROM {{ ref('slv_prodlkup') }}
)

SELECT
    s.*,
    
    -- 1. QuantityType
    CASE
        WHEN s.OrderQuantity > 1 THEN 'Multiple Items'
        ELSE 'Single Item'
    END AS QuantityType,
    
    -- 2. RetailPrice from silver product lookup
    p.RetailPrice,
    
    -- 3. Revenue = RetailPrice * OrderQuantity
    p.RetailPrice * s.OrderQuantity AS Revenue

FROM base s
LEFT JOIN prod p ON s.ProductKey = p.ProductKey
{% if is_incremental() %}
WHERE s.orderdate > (select max(orderdate) from {{ this }})
{% endif %}