

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
    FROM EMEKA_MARKET_DATA.RAW_SALES_DATA.SALES_DATA
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