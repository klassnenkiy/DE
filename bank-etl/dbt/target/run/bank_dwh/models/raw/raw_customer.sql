
      
        
            delete from "bank_dwh"."public_raw"."raw_customer"
            using "raw_customer__dbt_tmp230007416847"
            where (
                
                    "raw_customer__dbt_tmp230007416847".customer_id = "bank_dwh"."public_raw"."raw_customer".customer_id
                    and 
                
                    "raw_customer__dbt_tmp230007416847".batch_id = "bank_dwh"."public_raw"."raw_customer".batch_id
                    
                
                
            );
        
    

    insert into "bank_dwh"."public_raw"."raw_customer" ("customer_id", "customer_uuid", "full_name", "birth_date", "email", "phone", "city", "created_at", "updated_at", "is_deleted", "batch_id", "etl_upload_dttm")
    (
        select "customer_id", "customer_uuid", "full_name", "birth_date", "email", "phone", "city", "created_at", "updated_at", "is_deleted", "batch_id", "etl_upload_dttm"
        from "raw_customer__dbt_tmp230007416847"
    )
  