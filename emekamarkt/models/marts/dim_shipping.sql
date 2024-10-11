{{ config(
    materialized='incremental',
    unique_key='ShippingID'
) }}

WITH shipping_data AS (
    SELECT
        s.value:ShippingID::int AS ShippingID,
        s.value:Method::string AS Method,
        s.value:Cost::float AS Cost
    FROM {{ source('emeka_market_data', 'SALES_DATA') }},
    LATERAL FLATTEN(input => Shipping) AS s
    {% if is_incremental() %}
        -- Only get new or updated shipping data based on ShippingID
        WHERE s.value:ShippingID::int > (SELECT MAX(ShippingID) FROM {{ this }})
    {% endif %}
)

-- Deduplicating by ShippingID
SELECT DISTINCT
    ShippingID, 
    Method, 
    Cost
FROM shipping_data
