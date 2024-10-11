-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into EMEKA_MARKET_DATA.RAW_SALES_DATA.dim_location as DBT_INTERNAL_DEST
        using EMEKA_MARKET_DATA.RAW_SALES_DATA.dim_location__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.LocationID = DBT_INTERNAL_DEST.LocationID
            )

    
    when matched then update set
        "LOCATIONID" = DBT_INTERNAL_SOURCE."LOCATIONID","COUNTRY" = DBT_INTERNAL_SOURCE."COUNTRY","CITY" = DBT_INTERNAL_SOURCE."CITY","POSTALCODE" = DBT_INTERNAL_SOURCE."POSTALCODE","REGION" = DBT_INTERNAL_SOURCE."REGION"
    

    when not matched then insert
        ("LOCATIONID", "COUNTRY", "CITY", "POSTALCODE", "REGION")
    values
        ("LOCATIONID", "COUNTRY", "CITY", "POSTALCODE", "REGION")

;
    commit;