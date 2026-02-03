{{ config(
    materialized='incremental',
    unique_key='ReviewID'
) }}

WITH review_data AS (
    SELECT
        r.value:ReviewID::int AS ReviewID,
        r.value:ProductID::int AS ProductID,
        r.value:CustomerID::int AS CustomerID,
        r.value:Rating::int AS Rating,
        r.value:Comment::string AS Comment
    FROM {{ source('emeka_market_data', 'SALES_DATA') }},
    LATERAL FLATTEN(input => Review) AS r
    {% if is_incremental() %}
        -- Only get new or updated reviews based on ReviewID
        WHERE r.value:ReviewID::int > (SELECT MAX(ReviewID) FROM {{ this }})
    {% endif %}
)

-- Deduplicating by ReviewID
SELECT DISTINCT
    ReviewID, 
    ProductID, 
    CustomerID, 
    Rating, 
    Comment
FROM review_data
