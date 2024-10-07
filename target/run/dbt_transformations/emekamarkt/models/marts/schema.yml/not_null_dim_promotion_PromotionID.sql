select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select PromotionID
from EMEKA_MARKET_DATA.RAW_SALES_DATA.dim_promotion
where PromotionID is null



      
    ) dbt_internal_test