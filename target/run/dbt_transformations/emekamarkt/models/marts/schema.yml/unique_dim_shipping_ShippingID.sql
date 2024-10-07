select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    ShippingID as unique_field,
    count(*) as n_records

from EMEKA_MARKET_DATA.RAW_SALES_DATA.dim_shipping
where ShippingID is not null
group by ShippingID
having count(*) > 1



      
    ) dbt_internal_test