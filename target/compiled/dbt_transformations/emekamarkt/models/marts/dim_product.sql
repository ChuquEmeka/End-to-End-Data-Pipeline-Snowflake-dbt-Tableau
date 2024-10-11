

WITH product_data AS (
    SELECT
        p.value:ProductID::int AS ProductID,
        p.value:ProductName::string AS ProductName,
        p.value:Category::string AS Category,
        p.value:UnitCost::float AS UnitCost,
        p.value:UnitPrice::float AS UnitPrice
    FROM EMEKA_MARKET_DATA.RAW_SALES_DATA.SALES_DATA,
    LATERAL FLATTEN(input => Product) AS p
    
        -- Only get new or updated products based on ProductID
        WHERE p.value:ProductID::int > (SELECT MAX(ProductID) FROM EMEKA_MARKET_DATA.RAW_SALES_DATA.dim_product)
    
)

-- Deduplicating by ProductID
SELECT DISTINCT
    ProductID, 
    ProductName, 
    Category, 
    UnitCost, 
    UnitPrice
FROM product_data