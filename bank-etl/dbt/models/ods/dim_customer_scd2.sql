{{ config(materialized='table') }}

with base as (
  select
    customer_id,
    customer_uuid,
    full_name,
    birth_date,
    email,
    phone,
    city,
    is_deleted,
    etl_upload_dttm,
    md5(
      coalesce(customer_uuid::text,'') || '|' ||
      coalesce(full_name,'') || '|' ||
      coalesce(birth_date::text,'') || '|' ||
      coalesce(email,'') || '|' ||
      coalesce(phone,'') || '|' ||
      coalesce(city,'') || '|' ||
      coalesce(is_deleted::text,'')
    ) as diff
  from {{ ref('raw_customer') }}
),
chg as (
  select
    *,
    lag(diff) over (partition by customer_id order by etl_upload_dttm) as prev_diff
  from base
),
filt as (
  select
    *
  from chg
  where prev_diff is distinct from diff
),
scd as (
  select
    md5(customer_id::text || '|' || etl_upload_dttm::text) as customer_sk,
    customer_id,
    customer_uuid,
    full_name,
    birth_date,
    email,
    phone,
    city,
    is_deleted,
    etl_upload_dttm as valid_from,
    coalesce(lead(etl_upload_dttm) over (partition by customer_id order by etl_upload_dttm) - interval '1 second', timestamptz '9999-12-31') as valid_to,
    case when lead(etl_upload_dttm) over (partition by customer_id order by etl_upload_dttm) is null then true else false end as is_current
  from filt
)
select * from scd
