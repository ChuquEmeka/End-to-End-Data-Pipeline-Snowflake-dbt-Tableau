
    
    

select
    ProductID as unique_field,
    count(*) as n_records

from EMEKA_MARKET_DATA.RAW_SALES_DATA.dim_product
where ProductID is not null
group by ProductID
having count(*) > 1


