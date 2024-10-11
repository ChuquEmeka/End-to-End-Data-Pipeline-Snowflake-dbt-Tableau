{{ config(
    materialized='incremental',
    unique_key='SaleID'
) }}

WITH sales_data AS (
    -- Get the necessary fields from the raw sales data
    SELECT
        SaleID,
        Quantity,
        SaleDate,
        LocationID,
        CustomerID,
        ProductID,
        PromotionID,
        ShippingID,
        ReviewID
    FROM {{ source('emeka_market_data', 'SALES_DATA') }}
    {% if is_incremental() %}
        -- Only get new or updated sales based on SaleID
        WHERE SaleID > (SELECT MAX(SaleID) FROM {{ this }})
    {% endif %}
),

product_data AS (
    -- Get the necessary fields from the product dimension table
    SELECT
        p.ProductID,
        p.UnitCost,
        p.UnitPrice
    FROM {{ ref('dim_product') }} AS p
)

-- Join the sales data with the product data to calculate total cost, revenue, and profit
SELECT 
    s.SaleID,
    s.Quantity,
    s.SaleDate,
    s.LocationID,
    s.CustomerID,
    s.ProductID,
    s.PromotionID,
    s.ShippingID,
    s.ReviewID,
    (s.Quantity * p.UnitPrice) AS total_revenue,
    (s.Quantity * p.UnitCost) AS total_cost,
    ((s.Quantity * p.UnitPrice) - (s.Quantity * p.UnitCost)) AS total_profit
FROM sales_data s
JOIN product_data p
    ON s.ProductID = p.ProductID
