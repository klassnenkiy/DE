

select
  txn_id,
  card_id,
  terminal_id,
  txn_ts,
  amount,
  currency_code,
  txn_type,
  status,
  etl_upload_dttm
from "bank_dwh"."stg"."transaction_increment"