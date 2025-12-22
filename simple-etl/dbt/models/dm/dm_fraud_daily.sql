{{ config(materialized='table') }}

select
    fraud_date as day,
    fraud_type,
    count() as fraud_case_cnt,
    sum(fraud_txn_cnt) as fraud_txn_cnt,
    sum(fraud_amount_sum) as fraud_amount_sum
from {{ ref('cdm_fraud_cases') }}
group by
    day,
    fraud_type

