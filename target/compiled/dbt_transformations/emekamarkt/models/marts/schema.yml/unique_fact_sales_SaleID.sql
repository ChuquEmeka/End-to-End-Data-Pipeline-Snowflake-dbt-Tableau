
    
    

select
    SaleID as unique_field,
    count(*) as n_records

from EMEKA_MARKET_DATA.RAW_SALES_DATA.fact_sales
where SaleID is not null
group by SaleID
having count(*) > 1


