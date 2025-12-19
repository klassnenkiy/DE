{{ config(materialized='incremental', unique_key='txn_id', incremental_strategy='delete+insert') }}

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
from {{ source('stg','transaction_increment') }}
