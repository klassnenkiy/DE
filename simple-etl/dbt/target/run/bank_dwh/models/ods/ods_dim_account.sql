
  
    
    
    
        
         


        insert into `bank_dwh`.`ods_dim_account__dbt_backup`
        ("account_id", "account_number", "customer_id", "currency_code", "opened_at", "closed_at", "status", "daily_transfer_limit", "is_deleted", "loaded_at")

SELECT
    account_id,
    account_number,
    customer_id,
    currency_code,
    opened_at,
    closed_at,
    status,
    daily_transfer_limit,
    is_deleted,
    now() AS loaded_at
FROM `bank_dwh`.`raw_account`
  