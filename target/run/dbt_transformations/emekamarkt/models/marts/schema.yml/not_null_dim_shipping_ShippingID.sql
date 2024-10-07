select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select ShippingID
from EMEKA_MARKET_DATA.RAW_SALES_DATA.dim_shipping
where ShippingID is null



      
    ) dbt_internal_test