{{ config(
    materialized='incremental',
    unique_key='ProductID'
) }}

WITH product_data AS (
    SELECT
        p.value:ProductID::int AS ProductID,
        p.value:ProductName::string AS ProductName,
        p.value:Category::string AS Category,
        p.value:UnitCost::float AS UnitCost,
        p.value:UnitPrice::float AS UnitPrice
    FROM {{ source('emeka_market_data', 'SALES_DATA') }},
    LATERAL FLATTEN(input => Product) AS p
    {% if is_incremental() %}
        -- Only get new or updated products based on ProductID
        WHERE p.value:ProductID::int > (SELECT MAX(ProductID) FROM {{ this }})
    {% endif %}
)

-- Deduplicating by ProductID
SELECT DISTINCT
    ProductID, 
    ProductName, 
    Category, 
    UnitCost, 
    UnitPrice
FROM product_data
