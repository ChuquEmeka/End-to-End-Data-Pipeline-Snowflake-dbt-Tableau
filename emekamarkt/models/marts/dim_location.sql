{{ config(
    materialized='table'
) }}

WITH location_data AS (
    SELECT
        l.value:LocationID::int AS LocationID,
        l.value:Country::string AS Country,
        l.value:City::string AS City,
        l.value:PostalCode::string AS PostalCode,
        l.value:Region::string AS Region
    FROM {{ source('emeka_market_data', 'SALES_DATA') }},
    LATERAL FLATTEN(input => Location) AS l
)
-- Deduplicating by LocationID
SELECT DISTINCT (LocationID) 
    LocationID, 
    Country, 
    City, 
    PostalCode, 
    Region
FROM location_data
