-- tests/check_positive_quantity.sql

SELECT *
FROM {{ ref('fact_sales') }}
WHERE Quantity < 0
