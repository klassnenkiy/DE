
  
    
    
    
        
         


        insert into `bank_dwh`.`ods_dim_terminal__dbt_backup`
        ("terminal_id", "terminal_code", "city", "country", "latitude", "longitude", "is_working", "is_deleted", "loaded_at")

SELECT
    terminal_id,
    terminal_code,
    city,
    country,
    latitude,
    longitude,
    is_working,
    is_deleted,
    now() AS loaded_at
FROM `bank_dwh`.`raw_terminal`
  