{{ config(materialized='incremental', unique_key=['terminal_id','batch_id'], incremental_strategy='delete+insert') }}

select
  terminal_id,
  terminal_code,
  city,
  country,
  latitude,
  longitude,
  created_at,
  updated_at,
  is_working,
  is_deleted,
  batch_id,
  etl_upload_dttm
from {{ source('stg','terminal_snapshot') }}
