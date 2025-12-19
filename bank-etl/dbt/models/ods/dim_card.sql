{{ config(materialized='table') }}

select
  md5(card_id::text) as card_sk,
  card_id,
  card_number,
  account_id,
  status,
  opened_at,
  closed_at,
  created_at,
  updated_at,
  is_deleted,
  src_event_id,
  src_event_ts
from {{ ref('raw_card') }}
