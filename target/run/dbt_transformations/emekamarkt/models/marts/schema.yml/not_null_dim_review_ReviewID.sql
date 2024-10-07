select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select ReviewID
from EMEKA_MARKET_DATA.RAW_SALES_DATA.dim_review
where ReviewID is null



      
    ) dbt_internal_test