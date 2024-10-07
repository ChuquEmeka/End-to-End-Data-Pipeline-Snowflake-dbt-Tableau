select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select ProductID
from EMEKA_MARKET_DATA.RAW_SALES_DATA.dim_product
where ProductID is null



      
    ) dbt_internal_test