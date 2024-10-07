
    
    

select
    ShippingID as unique_field,
    count(*) as n_records

from EMEKA_MARKET_DATA.RAW_SALES_DATA.dim_shipping
where ShippingID is not null
group by ShippingID
having count(*) > 1


