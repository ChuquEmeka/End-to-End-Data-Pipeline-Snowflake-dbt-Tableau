{{ config(
    materialized='incremental',
    unique_key='PromotionID'
) }}

WITH promotion_data AS (
    SELECT
        p.value:PromotionID::int AS PromotionID,
        p.value:PromotionName::string AS PromotionName,
        p.value:DiscountRate::float AS DiscountRate
    FROM {{ source('emeka_market_data', 'SALES_DATA') }},
    LATERAL FLATTEN(input => Promotion) AS p
    {% if is_incremental() %}
        -- Only get new or updated promotions based on PromotionID
        WHERE p.value:PromotionID::int > (SELECT MAX(PromotionID) FROM {{ this }})
    {% endif %}
)

-- Deduplicating by PromotionID
SELECT DISTINCT
    PromotionID, 
    PromotionName, 
    DiscountRate
FROM promotion_data
