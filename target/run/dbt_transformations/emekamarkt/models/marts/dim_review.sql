-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into EMEKA_MARKET_DATA.RAW_SALES_DATA.dim_review as DBT_INTERNAL_DEST
        using EMEKA_MARKET_DATA.RAW_SALES_DATA.dim_review__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.ReviewID = DBT_INTERNAL_DEST.ReviewID
            )

    
    when matched then update set
        "REVIEWID" = DBT_INTERNAL_SOURCE."REVIEWID","PRODUCTID" = DBT_INTERNAL_SOURCE."PRODUCTID","CUSTOMERID" = DBT_INTERNAL_SOURCE."CUSTOMERID","RATING" = DBT_INTERNAL_SOURCE."RATING","COMMENT" = DBT_INTERNAL_SOURCE."COMMENT"
    

    when not matched then insert
        ("REVIEWID", "PRODUCTID", "CUSTOMERID", "RATING", "COMMENT")
    values
        ("REVIEWID", "PRODUCTID", "CUSTOMERID", "RATING", "COMMENT")

;
    commit;