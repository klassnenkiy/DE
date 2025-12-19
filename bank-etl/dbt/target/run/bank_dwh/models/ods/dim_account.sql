
  
    

  create  table "bank_dwh"."public_ods"."dim_account__dbt_tmp"
  
  
    as
  
  (
    

select
  md5(account_id::text) as account_sk,
  account_id,
  account_number,
  customer_id,
  currency_code,
  opened_at,
  closed_at,
  status,
  daily_transfer_limit,
  created_at,
  updated_at,
  is_deleted,
  src_event_id,
  src_event_ts
from "bank_dwh"."public_raw"."raw_account"
  );
  