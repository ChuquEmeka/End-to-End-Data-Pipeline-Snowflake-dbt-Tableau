
  
    

        create or replace transient table EMEKA_MARKET_DATA.RAW_SALES_DATA.dim_shipping
         as
        (

WITH shipping_data AS (
    SELECT
        s.value:ShippingID::int AS ShippingID,
        s.value:Method::string AS Method,
        s.value:Cost::float AS Cost
    FROM EMEKA_MARKET_DATA.RAW_SALES_DATA.SALES_DATA,
    LATERAL FLATTEN(input => Shipping) AS s
)
-- Deduplicating by ShippingID
SELECT DISTINCT(ShippingID) 
    ShippingID, 
    Method, 
    Cost
FROM shipping_data
        );
      
  