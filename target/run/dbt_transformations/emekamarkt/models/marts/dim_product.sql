-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into EMEKA_MARKET_DATA.RAW_SALES_DATA.dim_product as DBT_INTERNAL_DEST
        using EMEKA_MARKET_DATA.RAW_SALES_DATA.dim_product__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.ProductID = DBT_INTERNAL_DEST.ProductID
            )

    
    when matched then update set
        "PRODUCTID" = DBT_INTERNAL_SOURCE."PRODUCTID","PRODUCTNAME" = DBT_INTERNAL_SOURCE."PRODUCTNAME","CATEGORY" = DBT_INTERNAL_SOURCE."CATEGORY","UNITCOST" = DBT_INTERNAL_SOURCE."UNITCOST","UNITPRICE" = DBT_INTERNAL_SOURCE."UNITPRICE"
    

    when not matched then insert
        ("PRODUCTID", "PRODUCTNAME", "CATEGORY", "UNITCOST", "UNITPRICE")
    values
        ("PRODUCTID", "PRODUCTNAME", "CATEGORY", "UNITCOST", "UNITPRICE")

;
    commit;