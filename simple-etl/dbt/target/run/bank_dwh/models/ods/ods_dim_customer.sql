
  
    
    
    
        
         


        insert into `bank_dwh`.`ods_dim_customer__dbt_backup`
        ("customer_id", "customer_uuid", "full_name", "birth_date", "email", "phone", "city", "is_deleted", "loaded_at")

SELECT
    customer_id,
    customer_uuid,
    full_name,
    birth_date,
    email,
    phone,
    city,
    is_deleted,
    now() AS loaded_at
FROM `bank_dwh`.`raw_customer`
  