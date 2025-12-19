
      
        
            delete from "bank_dwh"."public_raw"."raw_account"
            where (
                account_id) in (
                select (account_id)
                from "raw_account__dbt_tmp230007380557"
            );

        
    

    insert into "bank_dwh"."public_raw"."raw_account" ("account_id", "account_number", "customer_id", "currency_code", "opened_at", "closed_at", "status", "daily_transfer_limit", "created_at", "updated_at", "is_deleted", "src_event_id", "src_event_ts")
    (
        select "account_id", "account_number", "customer_id", "currency_code", "opened_at", "closed_at", "status", "daily_transfer_limit", "created_at", "updated_at", "is_deleted", "src_event_id", "src_event_ts"
        from "raw_account__dbt_tmp230007380557"
    )
  