-- models/staging_raw_sales_data.sql



WITH raw_data AS (
    SELECT *
    FROM my_external_schema."external_table"(  -- Note the quotes around the function name
        's3://emeka-market-raw-sales-data/raw_sales_data.csv',
        'arn:aws:iam::863518444424:role/service-role/AmazonRedshift-CommandsAccessRole-20241004T144721',
        'CSV',
        'IGNOREHEADER 1',
        'DELIMITER '',''',
        'REGION ''eu-north-1'''
    )
)

SELECT * FROM raw_data;