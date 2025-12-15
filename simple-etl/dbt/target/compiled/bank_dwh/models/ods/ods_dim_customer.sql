

SELECT
    customer_id,
    customer_uuid,
    full_name,
    birth_date,
    email,
    phone,
    city,
    is_deleted,
    now() AS loaded_at
FROM `bank_dwh`.`raw_customer`