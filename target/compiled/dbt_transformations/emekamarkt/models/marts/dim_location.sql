

WITH location_data AS (
    SELECT
        l.value:LocationID::int AS LocationID,
        l.value:Country::string AS Country,
        l.value:City::string AS City,
        l.value:PostalCode::string AS PostalCode,
        l.value:Region::string AS Region
    FROM EMEKA_MARKET_DATA.RAW_SALES_DATA.SALES_DATA,
    LATERAL FLATTEN(input => Location) AS l
    
        -- Only get new or updated locations based on LocationID
        WHERE l.value:LocationID::int > (SELECT MAX(LocationID) FROM EMEKA_MARKET_DATA.RAW_SALES_DATA.dim_location)
    
)

-- Deduplicating by LocationID
SELECT DISTINCT
    LocationID, 
    Country, 
    City, 
    PostalCode, 
    Region
FROM location_data