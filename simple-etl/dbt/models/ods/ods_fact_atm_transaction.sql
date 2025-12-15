{{ config(materialized='table') }}

WITH tx AS (
    SELECT *
    FROM {{ ref('raw_transaction') }}
),

card_dim AS (
    SELECT *
    FROM {{ ref('raw_card') }}
),

acc_dim AS (
    SELECT *
    FROM {{ ref('raw_account') }}
),

cust_dim AS (
    SELECT *
    FROM {{ ref('raw_customer') }}
),

term_dim AS (
    SELECT *
    FROM {{ ref('raw_terminal') }}
)

SELECT
    tx.txn_id,
    tx.txn_ts,
    toFloat64(tx.amount)           AS amount,
    tx.currency_code,

    card_dim.card_id,
    card_dim.card_number,

    acc_dim.account_id,
    acc_dim.account_number,
    cust_dim.customer_id,
    cust_dim.city                  AS customer_city,

    term_dim.terminal_id,
    term_dim.city                  AS terminal_city,

    tx.txn_type,
    tx.status,

    now() AS loaded_at
FROM tx
LEFT JOIN card_dim
    ON card_dim.card_id = tx.card_id
LEFT JOIN acc_dim
    ON acc_dim.account_id = card_dim.account_id
LEFT JOIN cust_dim
    ON cust_dim.customer_id = acc_dim.customer_id
LEFT JOIN term_dim
    ON term_dim.terminal_id = tx.terminal_id
