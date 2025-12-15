

WITH src AS (
    SELECT
        customer_id,
        customer_uuid,
        full_name,
        birth_date,
        email,
        phone,
        city,
        created_at,
        updated_at,
        is_deleted
    FROM `bank_dwh`.`stg_customer`
)

SELECT
    customer_id,
    customer_uuid,
    full_name,
    birth_date,
    email,
    phone,
    city,
    created_at,
    updated_at,
    is_deleted,
    now() AS loaded_at
FROM src


WHERE updated_at > (SELECT max(updated_at) FROM `bank_dwh`.`raw_customer`)
