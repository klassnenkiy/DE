{{ config(materialized='incremental', unique_key='account_id', incremental_strategy='delete+insert') }}

with ev as (
  select
    (pk->>'account_id')::bigint as account_id,
    op,
    event_id,
    event_ts,
    row_data
  from {{ source('stg','cdc_events') }}
  where table_name = 'account'
  {% if is_incremental() %}
    and event_id > (select coalesce(max(src_event_id), 0) from {{ this }})
  {% endif %}
),
latest as (
  select distinct on (account_id)
    account_id,
    op,
    event_id as src_event_id,
    event_ts as src_event_ts,
    row_data
  from ev
  order by account_id, event_id desc
)
select
  account_id,
  row_data->>'account_number' as account_number,
  (row_data->>'customer_id')::bigint as customer_id,
  row_data->>'currency_code' as currency_code,
  (row_data->>'opened_at')::timestamptz as opened_at,
  (row_data->>'closed_at')::timestamptz as closed_at,
  row_data->>'status' as status,
  (row_data->>'daily_transfer_limit')::numeric as daily_transfer_limit,
  (row_data->>'created_at')::timestamptz as created_at,
  (row_data->>'updated_at')::timestamptz as updated_at,
  case when op = 'd' then true else coalesce((row_data->>'is_deleted')::boolean, false) end as is_deleted,
  src_event_id,
  src_event_ts
from latest
