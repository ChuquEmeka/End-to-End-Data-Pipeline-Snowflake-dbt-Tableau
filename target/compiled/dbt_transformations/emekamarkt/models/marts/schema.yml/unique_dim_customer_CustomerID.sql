
    
    

select
    CUSTOMERID as unique_field,
    count(*) as n_records

from EMEKA_MARKET_DATA.RAW_SALES_DATA.dim_customer
where CUSTOMERID is not null
group by CUSTOMERID
having count(*) > 1


