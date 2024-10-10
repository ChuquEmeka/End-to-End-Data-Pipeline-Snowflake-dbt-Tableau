

WITH sales_data AS (
    -- Get the necessary fields from the sales fact table
    SELECT
        ProductID,
        SUM(Quantity) AS total_quantity
    FROM EMEKA_MARKET_DATA.RAW_SALES_DATA.fact_sales
    GROUP BY ProductID
),

product_data AS (
    -- Get the necessary fields from the product dimension table
    SELECT
        ProductID,
        UnitCost,
        UnitPrice
    FROM EMEKA_MARKET_DATA.RAW_SALES_DATA.dim_product
)

-- Join the sales data with the product data to calculate total cost, revenue, and profit
SELECT 
    p.ProductID,
    s.total_quantity,
    (s.total_quantity * p.UnitPrice) AS total_revenue,
    (s.total_quantity * p.UnitCost) AS total_cost,
    ((s.total_quantity * p.UnitPrice) - (s.total_quantity * p.UnitCost)) AS total_profit
FROM sales_data s
JOIN product_data p
    ON s.ProductID = p.ProductID