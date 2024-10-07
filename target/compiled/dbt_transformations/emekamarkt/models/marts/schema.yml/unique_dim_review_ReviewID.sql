
    
    

select
    ReviewID as unique_field,
    count(*) as n_records

from EMEKA_MARKET_DATA.RAW_SALES_DATA.dim_review
where ReviewID is not null
group by ReviewID
having count(*) > 1


