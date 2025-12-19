

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
from "bank_dwh"."stg"."terminal_snapshot"