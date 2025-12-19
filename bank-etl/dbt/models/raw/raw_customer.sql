{{ config(materialized='incremental', unique_key=['customer_id','batch_id'], incremental_strategy='delete+insert') }}

select
  customer_id,
  customer_uuid,
  full_name,
  birth_date,
  email,
  phone,
  city,
  created_at,
  updated_at,
  is_deleted,
  batch_id,
  etl_upload_dttm
from {{ source('stg','customer_snapshot') }}
