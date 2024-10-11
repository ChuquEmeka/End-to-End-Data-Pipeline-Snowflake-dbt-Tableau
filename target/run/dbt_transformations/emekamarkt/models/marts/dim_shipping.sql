-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into EMEKA_MARKET_DATA.RAW_SALES_DATA.dim_shipping as DBT_INTERNAL_DEST
        using EMEKA_MARKET_DATA.RAW_SALES_DATA.dim_shipping__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.ShippingID = DBT_INTERNAL_DEST.ShippingID
            )

    
    when matched then update set
        "SHIPPINGID" = DBT_INTERNAL_SOURCE."SHIPPINGID","METHOD" = DBT_INTERNAL_SOURCE."METHOD","COST" = DBT_INTERNAL_SOURCE."COST"
    

    when not matched then insert
        ("SHIPPINGID", "METHOD", "COST")
    values
        ("SHIPPINGID", "METHOD", "COST")

;
    commit;