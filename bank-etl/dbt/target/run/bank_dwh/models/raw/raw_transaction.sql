
      
        
            delete from "bank_dwh"."public_raw"."raw_transaction"
            where (
                txn_id) in (
                select (txn_id)
                from "raw_transaction__dbt_tmp230007704299"
            );

        
    

    insert into "bank_dwh"."public_raw"."raw_transaction" ("txn_id", "card_id", "terminal_id", "txn_ts", "amount", "currency_code", "txn_type", "status", "etl_upload_dttm")
    (
        select "txn_id", "card_id", "terminal_id", "txn_ts", "amount", "currency_code", "txn_type", "status", "etl_upload_dttm"
        from "raw_transaction__dbt_tmp230007704299"
    )
  