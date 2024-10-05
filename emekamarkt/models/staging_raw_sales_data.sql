-- models/staging_raw_sales_data.sql

{{ config(
    materialized='table'  -- You can also use 'view' if preferred
) }}

WITH raw_data AS (
    SELECT 
        *
    FROM 
        external_table('s3://emeka-market-raw-sales-data/raw_sales_data.csv',
        'arn:aws:iam::863518444424:role/service-role/AmazonRedshift-CommandsAccessRole-20241004T144721',  -- Replace with your IAM Role ARN
        'CSV',
        'IGNOREHEADER 1',
        'DELIMITER ','',
        'REGION eu-north-1'  -- Adjust if necessary
    )
)

SELECT
    SaleID,
    ProductID,
    CustomerID,
    Quantity,
    Date,
    LocationID,
    PaymentID,
    ShippingID,
    PromotionID,
    ReviewID,
    JSON_EXTRACT_PATH_TEXT(Product, 'ProductID')::INT AS Product_ProductID,
    JSON_EXTRACT_PATH_TEXT(Product, 'ProductName') AS Product_ProductName,
    JSON_EXTRACT_PATH_TEXT(Product, 'Category') AS Product_Category,
    JSON_EXTRACT_PATH_TEXT(Product, 'UnitCost')::FLOAT AS Product_UnitCost,
    JSON_EXTRACT_PATH_TEXT(Product, 'UnitPrice')::FLOAT AS Product_UnitPrice,
    JSON_EXTRACT_PATH_TEXT(Customer, 'CustomerID')::INT AS Customer_CustomerID,
    JSON_EXTRACT_PATH_TEXT(Customer, 'CustomerName') AS Customer_CustomerName,
    JSON_EXTRACT_PATH_TEXT(Customer, 'Email') AS Customer_Email,
    JSON_EXTRACT_PATH_TEXT(Customer, 'PhoneNumber') AS Customer_PhoneNumber,
    JSON_EXTRACT_PATH_TEXT(Customer, 'LoyaltyStatus') AS Customer_LoyaltyStatus,
    JSON_EXTRACT_PATH_TEXT(Location, 'LocationID')::INT AS Location_LocationID,
    JSON_EXTRACT_PATH_TEXT(Location, 'Country') AS Location_Country,
    JSON_EXTRACT_PATH_TEXT(Location, 'State') AS Location_State,
    JSON_EXTRACT_PATH_TEXT(Location, 'City') AS Location_City,
    JSON_EXTRACT_PATH_TEXT(Location, 'PostalCode')::INT AS Location_PostalCode,
    JSON_EXTRACT_PATH_TEXT(Location, 'Region') AS Location_Region,
    JSON_EXTRACT_PATH_TEXT(Promotion, 'PromotionID')::INT AS Promotion_PromotionID,
    JSON_EXTRACT_PATH_TEXT(Promotion, 'PromotionName') AS Promotion_PromotionName,
    JSON_EXTRACT_PATH_TEXT(Promotion, 'DiscountRate')::FLOAT AS Promotion_DiscountRate,
    JSON_EXTRACT_PATH_TEXT(Shipping, 'ShippingID')::INT AS Shipping_ShippingID,
    JSON_EXTRACT_PATH_TEXT(Shipping, 'Method') AS Shipping_Method,
    JSON_EXTRACT_PATH_TEXT(Shipping, 'Cost')::FLOAT AS Shipping_Cost,
    JSON_EXTRACT_PATH_TEXT(Review, 'ReviewID')::INT AS Review_ReviewID,
    JSON_EXTRACT_PATH_TEXT(Review, 'ProductID')::INT AS Review_ProductID,
    JSON_EXTRACT_PATH_TEXT(Review, 'CustomerID')::INT AS Review_CustomerID,
    JSON_EXTRACT_PATH_TEXT(Review, 'Rating')::INT AS Review_Rating,
    JSON_EXTRACT_PATH_TEXT(Review, 'Comment') AS Review_Comment,
    JSON_EXTRACT_PATH_TEXT(MarketingChannel, 'ChannelID')::INT AS MarketingChannel_ChannelID,
    JSON_EXTRACT_PATH_TEXT(MarketingChannel, 'ChannelName') AS MarketingChannel_ChannelName
FROM raw_data
