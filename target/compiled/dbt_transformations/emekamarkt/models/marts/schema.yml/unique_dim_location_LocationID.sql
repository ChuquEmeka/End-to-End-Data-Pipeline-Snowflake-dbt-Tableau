
    
    

select
    LocationID as unique_field,
    count(*) as n_records

from EMEKA_MARKET_DATA.RAW_SALES_DATA.dim_location
where LocationID is not null
group by LocationID
having count(*) > 1


