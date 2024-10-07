{{ config(
    materialized='table'
) }}

WITH sales_data AS (
    SELECT
        SaleID,
        Quantity,
        SaleDate,  -- Avoid keyword conflict by renaming
        LocationID,
        CustomerID,
        ProductID,
        PromotionID,
        ShippingID,
        ReviewID
    FROM {{ source('emeka_market_data', 'SALES_DATA') }}
)

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
FROM sales_data
