select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select CUSTOMERID
from EMEKA_MARKET_DATA.RAW_SALES_DATA.dim_customer
where CUSTOMERID is null



      
    ) dbt_internal_test