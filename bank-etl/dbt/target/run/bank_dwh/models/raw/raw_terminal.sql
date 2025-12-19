
      
        
            delete from "bank_dwh"."public_raw"."raw_terminal"
            using "raw_terminal__dbt_tmp230007386619"
            where (
                
                    "raw_terminal__dbt_tmp230007386619".terminal_id = "bank_dwh"."public_raw"."raw_terminal".terminal_id
                    and 
                
                    "raw_terminal__dbt_tmp230007386619".batch_id = "bank_dwh"."public_raw"."raw_terminal".batch_id
                    
                
                
            );
        
    

    insert into "bank_dwh"."public_raw"."raw_terminal" ("terminal_id", "terminal_code", "city", "country", "latitude", "longitude", "created_at", "updated_at", "is_working", "is_deleted", "batch_id", "etl_upload_dttm")
    (
        select "terminal_id", "terminal_code", "city", "country", "latitude", "longitude", "created_at", "updated_at", "is_working", "is_deleted", "batch_id", "etl_upload_dttm"
        from "raw_terminal__dbt_tmp230007386619"
    )
  