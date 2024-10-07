

WITH promotion_data AS (
    SELECT
        p.value:PromotionID::int AS PromotionID,
        p.value:PromotionName::string AS PromotionName,
        p.value:DiscountRate::float AS DiscountRate
    FROM EMEKA_MARKET_DATA.RAW_SALES_DATA.SALES_DATA,
    LATERAL FLATTEN(input => Promotion) AS p
)
-- Deduplicating by PromotionID
SELECT DISTINCT (PromotionID) 
    PromotionID, 
    PromotionName, 
    DiscountRate
FROM promotion_data