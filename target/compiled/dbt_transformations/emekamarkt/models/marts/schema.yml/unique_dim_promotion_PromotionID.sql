
    
    

select
    PromotionID as unique_field,
    count(*) as n_records

from EMEKA_MARKET_DATA.RAW_SALES_DATA.dim_promotion
where PromotionID is not null
group by PromotionID
having count(*) > 1


