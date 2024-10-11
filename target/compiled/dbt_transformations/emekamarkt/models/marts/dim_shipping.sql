

WITH shipping_data AS (
    SELECT
        s.value:ShippingID::int AS ShippingID,
        s.value:Method::string AS Method,
        s.value:Cost::float AS Cost
    FROM EMEKA_MARKET_DATA.RAW_SALES_DATA.SALES_DATA,
    LATERAL FLATTEN(input => Shipping) AS s
    
        -- Only get new or updated shipping data based on ShippingID
        WHERE s.value:ShippingID::int > (SELECT MAX(ShippingID) FROM EMEKA_MARKET_DATA.RAW_SALES_DATA.dim_shipping)
    
)

-- Deduplicating by ShippingID
SELECT DISTINCT
    ShippingID, 
    Method, 
    Cost
FROM shipping_data