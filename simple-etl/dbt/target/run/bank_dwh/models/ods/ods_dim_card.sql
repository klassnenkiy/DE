
  
    
    
    
        
         


        insert into `bank_dwh`.`ods_dim_card__dbt_backup`
        ("card_id", "card_number", "account_id", "status", "opened_at", "closed_at", "is_deleted", "loaded_at")

SELECT
    card_id,
    card_number,
    account_id,
    status,
    opened_at,
    closed_at,
    is_deleted,
    now() AS loaded_at
FROM `bank_dwh`.`raw_card`
  