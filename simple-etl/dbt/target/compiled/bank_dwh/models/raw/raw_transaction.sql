

WITH src AS (
    SELECT
        txn_id,
        card_id,
        terminal_id,
        txn_ts,
        amount,
        currency_code,
        txn_type,
        status
    FROM `bank_dwh`.`stg_transaction`
)

SELECT
    txn_id,
    card_id,
    terminal_id,
    txn_ts,
    amount,
    currency_code,
    txn_type,
    status,
    now() AS loaded_at
FROM src


WHERE txn_id > (SELECT max(txn_id) FROM `bank_dwh`.`raw_transaction`)
