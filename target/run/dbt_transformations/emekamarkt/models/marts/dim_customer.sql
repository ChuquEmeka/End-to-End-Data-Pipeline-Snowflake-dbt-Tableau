-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into EMEKA_MARKET_DATA.RAW_SALES_DATA.dim_customer as DBT_INTERNAL_DEST
        using EMEKA_MARKET_DATA.RAW_SALES_DATA.dim_customer__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.CustomerID = DBT_INTERNAL_DEST.CustomerID
            )

    
    when matched then update set
        "CUSTOMERID" = DBT_INTERNAL_SOURCE."CUSTOMERID","CUSTOMERNAME" = DBT_INTERNAL_SOURCE."CUSTOMERNAME","EMAIL" = DBT_INTERNAL_SOURCE."EMAIL","PHONENUMBER" = DBT_INTERNAL_SOURCE."PHONENUMBER","LOYALTYSTATUS" = DBT_INTERNAL_SOURCE."LOYALTYSTATUS"
    

    when not matched then insert
        ("CUSTOMERID", "CUSTOMERNAME", "EMAIL", "PHONENUMBER", "LOYALTYSTATUS")
    values
        ("CUSTOMERID", "CUSTOMERNAME", "EMAIL", "PHONENUMBER", "LOYALTYSTATUS")

;
    commit;