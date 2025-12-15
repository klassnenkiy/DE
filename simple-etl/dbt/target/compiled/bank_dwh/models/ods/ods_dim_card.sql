

SELECT
    card_id,
    card_number,
    account_id,
    status,
    opened_at,
    closed_at,
    is_deleted,
    now() AS loaded_at
FROM `bank_dwh`.`raw_card`