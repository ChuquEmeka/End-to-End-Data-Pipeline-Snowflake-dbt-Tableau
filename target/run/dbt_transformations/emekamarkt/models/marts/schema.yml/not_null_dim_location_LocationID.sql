select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select LocationID
from EMEKA_MARKET_DATA.RAW_SALES_DATA.dim_location
where LocationID is null



      
    ) dbt_internal_test