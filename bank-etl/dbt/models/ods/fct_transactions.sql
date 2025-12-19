{{ config(materialized='table') }}

with t as (
  select
    txn_id,
    card_id,
    terminal_id,
    txn_ts,
    amount,
    currency_code,
    txn_type,
    status
  from {{ ref('raw_transaction') }}
),
c as (
  select card_id, md5(card_id::text) as card_sk, account_id
  from {{ ref('raw_card') }}
  where is_deleted = false
),
a as (
  select account_id, md5(account_id::text) as account_sk, customer_id
  from {{ ref('raw_account') }}
  where is_deleted = false
),
cust as (
  select customer_id, customer_sk
  from {{ ref('dim_customer_scd2') }}
  where is_current = true
),
term as (
  select terminal_id, terminal_sk
  from {{ ref('dim_terminal') }}
)
select
  t.txn_id,
  t.txn_ts,
  t.amount,
  t.currency_code,
  t.txn_type,
  t.status,
  c.card_sk,
  a.account_sk,
  cust.customer_sk,
  term.terminal_sk
from t
left join c on c.card_id = t.card_id
left join a on a.account_id = c.account_id
left join cust on cust.customer_id = a.customer_id
left join term on term.terminal_id = t.terminal_id
