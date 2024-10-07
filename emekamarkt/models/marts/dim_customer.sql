{{ config(
    materialized='table'
) }}

WITH customer_data AS (
    SELECT
        c.value:CustomerID::int AS CustomerID,
        c.value:CustomerName::string AS CustomerName,
        c.value:Email::string AS Email,
        c.value:PhoneNumber::string AS PhoneNumber,
        c.value:LoyaltyStatus::string AS LoyaltyStatus
    FROM {{ source('emeka_market_data', 'SALES_DATA') }},
    LATERAL FLATTEN(input => Customer) AS c
)

SELECT DISTINCT (CustomerID)  
    CustomerName, 
    Email, 
    PhoneNumber, 
    LoyaltyStatus
FROM customer_data
