{{ config(
    materialized='table'
) }}

WITH shipping_data AS (
    SELECT
        s.value:ShippingID::int AS ShippingID,
        s.value:Method::string AS Method,
        s.value:Cost::float AS Cost
    FROM {{ source('emeka_market_data', 'SALES_DATA') }},
    LATERAL FLATTEN(input => Shipping) AS s
)
-- Deduplicating by ShippingID
SELECT DISTINCT(ShippingID) 
    ShippingID, 
    Method, 
    Cost
FROM shipping_data
