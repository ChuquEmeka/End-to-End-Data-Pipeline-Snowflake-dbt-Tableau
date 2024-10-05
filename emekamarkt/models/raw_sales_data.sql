-- models/raw_sales_data.sql
{{ config(materialized='table') }}

WITH expanded_product_data AS (
    SELECT * FROM (
        VALUES
            (1, 'Phone', 'Electronics', 300, 450),
            (2, 'Clock', 'Home Appliances', 20, 30),
            (3, 'Laptop', 'Computers', 800, 1200),
            (4, 'Keyboard', 'Accessories', 30, 45),
            (5, 'Pot', 'Kitchenware', 15, 25),
            (6, 'Plates', 'Kitchenware', 10, 15),
            (7, 'Spoon', 'Kitchenware', 5, 10),
            (8, 'Tablet', 'Electronics', 200, 300),
            (9, 'Monitor', 'Electronics', 150, 225),
            (10, 'Headphones', 'Electronics', 50, 75),
            (11, 'Television', 'Electronics', 400, 600),
            (12, 'Blender', 'Home Appliances', 40, 60),
            (13, 'Microwave', 'Home Appliances', 100, 150),
            (14, 'Couch', 'Furniture', 500, 750),
            (15, 'Desk', 'Furniture', 150, 225),
            (16, 'Chair', 'Furniture', 100, 150),
            (17, 'Camera', 'Cameras', 600, 900),
            (18, 'Refrigerator', 'Home Appliances', 800, 1200),
            (19, 'Oven', 'Home Appliances', 400, 600),
            (20, 'Printer', 'Office Supplies', 150, 225)
            -- Add more products as needed...
    ) AS t (ProductID, ProductName, Category, UnitCost, UnitPrice)
),

customer_data AS (
    SELECT * FROM (
        VALUES
            (1, 'Emma Watson', 'emma.watson@example.com', '+49-1234567890', 'Silver'),
            (2, 'Dwayne Johnson', 'dwayne.johnson@example.com', '+49-0987654321', 'Gold'),
            (3, 'Angelina Jolie', 'angelina.jolie@example.com', '+49-2345678901', 'Bronze'),
            (4, 'Chris Hemsworth', 'chris.hemsworth@example.com', '+49-3456789012', 'Silver'),
            (5, 'Adele', 'adele@example.com', '+49-4567890123', 'Gold'),
            (6, 'Leonardo DiCaprio', 'leonardo.dicaprio@example.com', '+49-5678901234', 'Silver'),
            (7, 'Taylor Swift', 'taylor.swift@example.com', '+49-6789012345', 'Bronze'),
            (8, 'Shakira', 'shakira@example.com', '+49-7890123456', 'Gold'),
            (9, 'Ed Sheeran', 'ed.sheeran@example.com', '+49-8901234567', 'Silver'),
            (10, 'Selena Gomez', 'selena.gomez@example.com', '+49-9012345678', 'Gold')
            -- Add more customers as needed...
    ) AS t (CustomerID, CustomerName, Email, PhoneNumber, LoyaltyStatus)
),

country_city_mapping AS (
    SELECT * FROM (
        VALUES
            ('Germany', 'Berlin'),
            ('Germany', 'Munich'),
            ('Germany', 'Hamburg'),
            ('Germany', 'Frankfurt'),
            ('Germany', 'Cologne'),
            ('USA', 'New York'),
            ('USA', 'Los Angeles'),
            ('USA', 'Chicago'),
            ('USA', 'Houston'),
            ('USA', 'Phoenix'),
            ('UK', 'London'),
            ('UK', 'Birmingham'),
            ('UK', 'Manchester'),
            ('UK', 'Glasgow'),
            ('UK', 'Liverpool'),
            ('France', 'Paris'),
            ('France', 'Marseille'),
            ('France', 'Lyon'),
            ('France', 'Toulouse'),
            ('France', 'Nice'),
            ('Canada', 'Toronto'),
            ('Canada', 'Vancouver'),
            ('Canada', 'Montreal'),
            ('Canada', 'Calgary'),
            ('Canada', 'Ottawa'),
            ('Italy', 'Rome'),
            ('Italy', 'Milan'),
            ('Italy', 'Naples'),
            ('Italy', 'Turin'),
            ('Italy', 'Palermo'),
            ('Spain', 'Madrid'),
            ('Spain', 'Barcelona'),
            ('Spain', 'Valencia'),
            ('Spain', 'Seville'),
            ('Spain', 'Zaragoza'),
            ('Netherlands', 'Amsterdam'),
            ('Netherlands', 'Rotterdam'),
            ('Netherlands', 'The Hague'),
            ('Netherlands', 'Utrecht'),
            ('Netherlands', 'Eindhoven'),
            ('China', 'Beijing'),
            ('China', 'Shanghai'),
            ('China', 'Guangzhou'),
            ('China', 'Shenzhen'),
            ('China', 'Chengdu'),
            ('Japan', 'Tokyo'),
            ('Japan', 'Osaka'),
            ('Japan', 'Yokohama'),
            ('Japan', 'Nagoya'),
            ('Japan', 'Sapporo')
            -- Add more cities as needed...
    ) AS t (Country, City)
),

unit_cost_data AS (
    SELECT * FROM (
        VALUES
            (1, 'Phone', 300),
            (2, 'Clock', 20),
            (3, 'Laptop', 800),
            (4, 'Keyboard', 30),
            (5, 'Pot', 15),
            (6, 'Plates', 10),
            (7, 'Spoon', 5),
            (8, 'Tablet', 200),
            (9, 'Monitor', 150),
            (10, 'Headphones', 50),
            (11, 'Television', 400),
            (12, 'Blender', 40),
            (13, 'Microwave', 100),
            (14, 'Couch', 500),
            (15, 'Desk', 150),
            (16, 'Chair', 100),
            (17, 'Camera', 600),
            (18, 'Refrigerator', 800),
            (19, 'Oven', 400),
            (20, 'Printer', 150)
            -- Add more products as needed...
    ) AS t (ProductID, ProductName, UnitCost)
),

sales_data AS (
    SELECT
        ROW_NUMBER() OVER () AS SaleID,
        (SELECT ProductID FROM expanded_product_data ORDER BY RANDOM() LIMIT 1) AS ProductID,
        (SELECT CustomerID FROM customer_data ORDER BY RANDOM() LIMIT 1) AS CustomerID,
        CAST(FLOOR(RANDOM() * 10 + 1) AS INT) AS Quantity,
        CURRENT_TIMESTAMP AS Date,
        (SELECT City FROM country_city_mapping ORDER BY RANDOM() LIMIT 1) AS City,
        (SELECT Country FROM country_city_mapping ORDER BY RANDOM() LIMIT 1) AS Country
    FROM 
        generate_series(1, 150000)  -- Generate 150,000 records
)

SELECT
    s.SaleID,
    s.ProductID,
    s.CustomerID,
    s.Quantity,
    s.Date,
    JSON_BUILD_OBJECT(
        'Location', (SELECT JSON_AGG(c) FROM country_city_mapping c WHERE c.City = s.City),
        'Promotion', JSON_BUILD_OBJECT('PromotionID', 1, 'PromotionName', 'Promo 1', 'DiscountRate', 0.10),
        'Shipping', JSON_BUILD_OBJECT('ShippingID', 1, 'Method', 'Standard', 'Cost', 10.0),
        'Review', JSON_BUILD_OBJECT('ReviewID', 1, 'Rating', 5, 'Comment', 'Excellent!'),
        'MarketingChannel', JSON_BUILD_OBJECT('ChannelID', 1, 'ChannelName', 'Email'),
        'Product', (SELECT JSON_AGG(pd) FROM expanded_product_data pd WHERE pd.ProductID = s.ProductID)
    ) AS Dimensions
FROM 
    sales_data s;
