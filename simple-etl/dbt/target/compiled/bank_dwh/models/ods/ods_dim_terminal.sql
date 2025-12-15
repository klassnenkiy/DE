

SELECT
    terminal_id,
    terminal_code,
    city,
    country,
    latitude,
    longitude,
    is_working,
    is_deleted,
    now() AS loaded_at
FROM `bank_dwh`.`raw_terminal`