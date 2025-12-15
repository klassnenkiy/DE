

WITH src AS (
    SELECT
        terminal_id,
        terminal_code,
        city,
        country,
        latitude,
        longitude,
        created_at,
        updated_at,
        is_working,
        is_deleted
    FROM `bank_dwh`.`stg_terminal`
)

SELECT
    terminal_id,
    terminal_code,
    city,
    country,
    latitude,
    longitude,
    created_at,
    updated_at,
    is_working,
    is_deleted,
    now() AS loaded_at
FROM src


WHERE updated_at > (SELECT max(updated_at) FROM `bank_dwh`.`raw_terminal`)
