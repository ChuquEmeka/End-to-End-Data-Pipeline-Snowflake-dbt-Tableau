-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into EMEKA_MARKET_DATA.RAW_SALES_DATA.dim_promotion as DBT_INTERNAL_DEST
        using EMEKA_MARKET_DATA.RAW_SALES_DATA.dim_promotion__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.PromotionID = DBT_INTERNAL_DEST.PromotionID
            )

    
    when matched then update set
        "PROMOTIONID" = DBT_INTERNAL_SOURCE."PROMOTIONID","PROMOTIONNAME" = DBT_INTERNAL_SOURCE."PROMOTIONNAME","DISCOUNTRATE" = DBT_INTERNAL_SOURCE."DISCOUNTRATE"
    

    when not matched then insert
        ("PROMOTIONID", "PROMOTIONNAME", "DISCOUNTRATE")
    values
        ("PROMOTIONID", "PROMOTIONNAME", "DISCOUNTRATE")

;
    commit;