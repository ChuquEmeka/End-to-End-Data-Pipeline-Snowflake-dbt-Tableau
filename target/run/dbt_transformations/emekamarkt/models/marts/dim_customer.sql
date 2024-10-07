
  
    

        create or replace transient table EMEKA_MARKET_DATA.RAW_SALES_DATA.dim_customer
         as
        (

WITH customer_data AS (
    SELECT
        c.value:CustomerID::int AS CustomerID,
        c.value:CustomerName::string AS CustomerName,
        c.value:Email::string AS Email,
        c.value:PhoneNumber::string AS PhoneNumber,
        c.value:LoyaltyStatus::string AS LoyaltyStatus
    FROM EMEKA_MARKET_DATA.RAW_SALES_DATA.SALES_DATA,
    LATERAL FLATTEN(input => Customer) AS c
)

SELECT DISTINCT (CustomerID)  
    CustomerName, 
    Email, 
    PhoneNumber, 
    LoyaltyStatus
FROM customer_data
        );
      
  