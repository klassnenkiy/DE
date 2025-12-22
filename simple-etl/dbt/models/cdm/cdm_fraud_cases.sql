{{ config(materialized='table') }}

with
tx_base as (
    select
        txn_id,
        txn_ts,
        toDateTime64(txn_ts, 3) as txn_ts64,
        toDate(txn_ts) as txn_date,
        amount,
        card_id,
        account_id,
        terminal_id,
        lowerUTF8(txn_type) as txn_type_l,
        lowerUTF8(status) as status_l,
        terminal_city
    from {{ ref('ods_fact_atm_transaction') }}
    where txn_ts is not null
),
cards_scd2 as (
    select
        card_id,
        lowerUTF8(status) as card_status_l,
        closed_at,
        valid_from,
        valid_to
    from {{ ref('ods_dim_card_scd2') }}
),
terminals as (
    select
        terminal_id,
        city as dim_city,
        country as dim_country
    from {{ ref('ods_dim_terminal') }}
),
tx as (
    select
        t.txn_id,
        t.txn_ts,
        t.txn_ts64,
        t.txn_date,
        t.amount,
        t.card_id,
        t.account_id,
        t.terminal_id,
        t.txn_type_l,
        t.status_l,
        t.terminal_city,
        c.card_status_l,
        c.closed_at as card_closed_at,
        coalesce(t.terminal_city, tm.dim_city) as city,
        tm.dim_country as country,
        intDiv(toUnixTimestamp(t.txn_ts), 600) as w10m
    from tx_base as t
    left join cards_scd2 as c
        on t.card_id = c.card_id
       and t.txn_ts64 >= c.valid_from
       and t.txn_ts64 < c.valid_to
    left join terminals as tm
        on t.terminal_id = tm.terminal_id
),

amount_probing as (
    select
        t.card_id as card_id,
        t.w10m as w10m,
        minIf(t.txn_ts, t.status_l in ('declined','rejected','failed','error')) as fraud_start_ts,
        argMinIf(t.txn_id, t.txn_ts, t.status_l in ('declined','rejected','failed','error')) as txn_id_first,
        any(t.account_id) as account_id,
        argMin(t.terminal_id, t.txn_ts) as terminal_id_first,
        countIf(t.status_l in ('declined','rejected','failed','error')) as declined_cnt,
        uniqExactIf(t.amount, t.status_l in ('declined','rejected','failed','error')) as declined_amounts_uniq,
        count() as tx_cnt,
        sum(t.amount) as amount_sum
    from tx as t
    where t.txn_type_l in ('withdrawal','cash_withdrawal','atm_withdrawal','cash','withdraw')
    group by t.card_id, t.w10m
    having declined_cnt >= 3 and declined_amounts_uniq >= 3
),


geo_rapid as (
    select
        t.card_id as card_id,
        t.w10m as w10m,
        min(t.txn_ts) as fraud_start_ts,
        argMin(t.txn_id, t.txn_ts) as txn_id_first,
        any(t.account_id) as account_id,
        argMin(t.terminal_id, t.txn_ts) as terminal_id_first,
        uniqExact(t.terminal_id) as terminals_uniq,
        uniqExact(t.city) as cities_uniq,
        count() as tx_cnt,
        sum(t.amount) as amount_sum
    from tx as t
    where t.txn_type_l in ('withdrawal','cash_withdrawal','atm_withdrawal','cash','withdraw')
    group by t.card_id, t.w10m
    having tx_cnt >= 2 and (terminals_uniq >= 2 or cities_uniq >= 2)
),


after_close as (
    select
        t.card_id as card_id,
        t.txn_date as txn_date,
        min(t.txn_ts) as fraud_start_ts,
        argMin(t.txn_id, t.txn_ts) as txn_id_first,
        any(t.account_id) as account_id,
        argMin(t.terminal_id, t.txn_ts) as terminal_id_first,
        count() as tx_cnt,
        sum(t.amount) as amount_sum
    from tx as t
    where t.txn_type_l in ('withdrawal','cash_withdrawal','atm_withdrawal','cash','withdraw')
      and (
            t.card_status_l in ('closed','blocked')
            or (t.card_closed_at is not null and t.txn_ts > t.card_closed_at)
          )
    group by t.card_id, t.txn_date
),


unioned as (
    select
        cityHash64('amount_probing', toString(card_id), toString(w10m), toString(txn_id_first)) as fraud_case_id,
        'amount_probing' as fraud_type,
        fraud_start_ts,
        toDate(fraud_start_ts) as fraud_date,
        card_id,
        account_id,
        terminal_id_first,
        txn_id_first,
        tx_cnt as fraud_txn_cnt,
        amount_sum as fraud_amount_sum
    from amount_probing

    union all

    select
        cityHash64('geo_rapid', toString(card_id), toString(w10m), toString(txn_id_first)) as fraud_case_id,
        'geo_rapid' as fraud_type,
        fraud_start_ts,
        toDate(fraud_start_ts) as fraud_date,
        card_id,
        account_id,
        terminal_id_first,
        txn_id_first,
        tx_cnt as fraud_txn_cnt,
        amount_sum as fraud_amount_sum
    from geo_rapid

    union all

    select
        cityHash64('after_close', toString(card_id), toString(fraud_start_ts), toString(txn_id_first)) as fraud_case_id,
        'after_close' as fraud_type,
        fraud_start_ts,
        toDate(fraud_start_ts) as fraud_date,
        card_id,
        account_id,
        terminal_id_first,
        txn_id_first,
        tx_cnt as fraud_txn_cnt,
        amount_sum as fraud_amount_sum
    from after_close
)

select
    fraud_case_id,
    fraud_type,
    fraud_start_ts,
    fraud_date,
    card_id,
    account_id,
    terminal_id_first,
    txn_id_first,
    fraud_txn_cnt,
    fraud_amount_sum
from unioned
