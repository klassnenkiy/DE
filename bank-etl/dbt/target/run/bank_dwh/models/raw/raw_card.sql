
      
        
            delete from "bank_dwh"."public_raw"."raw_card"
            where (
                card_id) in (
                select (card_id)
                from "raw_card__dbt_tmp230007420877"
            );

        
    

    insert into "bank_dwh"."public_raw"."raw_card" ("card_id", "card_number", "account_id", "status", "opened_at", "closed_at", "created_at", "updated_at", "is_deleted", "src_event_id", "src_event_ts")
    (
        select "card_id", "card_number", "account_id", "status", "opened_at", "closed_at", "created_at", "updated_at", "is_deleted", "src_event_id", "src_event_ts"
        from "raw_card__dbt_tmp230007420877"
    )
  