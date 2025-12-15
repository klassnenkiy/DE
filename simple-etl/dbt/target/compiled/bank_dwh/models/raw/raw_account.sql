

WITH src AS (
    SELECT
        account_id,
        account_number,
        customer_id,
        currency_code,
        opened_at,
        closed_at,
        status,
        daily_transfer_limit,
        created_at,
        updated_at,
        is_deleted
    FROM `bank_dwh`.`stg_account`
)

SELECT
    account_id,
    account_number,
    customer_id,
    currency_code,
    opened_at,
    closed_at,
    status,
    daily_transfer_limit,
    created_at,
    updated_at,
    is_deleted,
    now() AS loaded_at
FROM src


WHERE updated_at > (SELECT max(updated_at) FROM `bank_dwh`.`raw_account`)
