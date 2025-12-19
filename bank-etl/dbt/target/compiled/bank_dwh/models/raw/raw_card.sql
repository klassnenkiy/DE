

with ev as (
  select
    (pk->>'card_id')::bigint as card_id,
    op,
    event_id,
    event_ts,
    row_data
  from "bank_dwh"."stg"."cdc_events"
  where table_name = 'card'
  
    and event_id > (select coalesce(max(src_event_id), 0) from "bank_dwh"."public_raw"."raw_card")
  
),
latest as (
  select distinct on (card_id)
    card_id,
    op,
    event_id as src_event_id,
    event_ts as src_event_ts,
    row_data
  from ev
  order by card_id, event_id desc
)
select
  card_id,
  row_data->>'card_number' as card_number,
  (row_data->>'account_id')::bigint as account_id,
  row_data->>'status' as status,
  (row_data->>'opened_at')::timestamptz as opened_at,
  (row_data->>'closed_at')::timestamptz as closed_at,
  (row_data->>'created_at')::timestamptz as created_at,
  (row_data->>'updated_at')::timestamptz as updated_at,
  case when op = 'd' then true else coalesce((row_data->>'is_deleted')::boolean, false) end as is_deleted,
  src_event_id,
  src_event_ts
from latest