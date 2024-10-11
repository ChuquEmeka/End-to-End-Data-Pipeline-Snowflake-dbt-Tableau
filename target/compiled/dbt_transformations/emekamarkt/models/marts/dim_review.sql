

WITH review_data AS (
    SELECT
        r.value:ReviewID::int AS ReviewID,
        r.value:ProductID::int AS ProductID,
        r.value:CustomerID::int AS CustomerID,
        r.value:Rating::int AS Rating,
        r.value:Comment::string AS Comment
    FROM EMEKA_MARKET_DATA.RAW_SALES_DATA.SALES_DATA,
    LATERAL FLATTEN(input => Review) AS r
    
        -- Only get new or updated reviews based on ReviewID
        WHERE r.value:ReviewID::int > (SELECT MAX(ReviewID) FROM EMEKA_MARKET_DATA.RAW_SALES_DATA.dim_review)
    
)

-- Deduplicating by ReviewID
SELECT DISTINCT
    ReviewID, 
    ProductID, 
    CustomerID, 
    Rating, 
    Comment
FROM review_data