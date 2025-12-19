

with x as (
  select
    terminal_id,
    terminal_code,
    city,
    country,
    latitude,
    longitude,
    is_working,
    is_deleted,
    etl_upload_dttm,
    row_number() over (partition by terminal_id order by etl_upload_dttm desc) as rn
  from "bank_dwh"."public_raw"."raw_terminal"
)
select
  md5(terminal_id::text) as terminal_sk,
  terminal_id,
  terminal_code,
  city,
  country,
  latitude,
  longitude,
  is_working,
  is_deleted,
  etl_upload_dttm as asof_dttm
from x
where rn = 1