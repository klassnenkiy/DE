

SELECT
    account_id,
    account_number,
    customer_id,
    currency_code,
    opened_at,
    closed_at,
    status,
    daily_transfer_limit,
    is_deleted,
    now() AS loaded_at
FROM `bank_dwh`.`raw_account`