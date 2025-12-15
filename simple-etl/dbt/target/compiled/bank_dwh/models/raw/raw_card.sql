

WITH src AS (
    SELECT
        card_id,
        card_number,
        account_id,
        status,
        opened_at,
        closed_at,
        created_at,
        updated_at,
        is_deleted
    FROM `bank_dwh`.`stg_card`
)

SELECT
    card_id,
    card_number,
    account_id,
    status,
    opened_at,
    closed_at,
    created_at,
    updated_at,
    is_deleted,
    now() AS loaded_at
FROM src


WHERE updated_at > (SELECT max(updated_at) FROM `bank_dwh`.`raw_card`)
