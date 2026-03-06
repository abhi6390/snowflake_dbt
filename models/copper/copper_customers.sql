with copper_customers as (
    select * FROM {{ ref('bronze_customers') }}
)

SELECT
    CAST(customer_id AS VARCHAR(255)) AS customer_id,
    CONCAT(first_name,' ',last_name) AS full_name,

    COALESCE(
    TRY_TO_DATE(dob, 'YYYY-MM-DD'),
    TRY_TO_DATE(dob, 'DD/MM/YYYY'),
    TRY_TO_DATE(dob, 'MM/DD/YYYY'),
    TRY_TO_DATE(dob, 'DD-MM-YYYY')
    ) AS dob,

    INITCAP(city) AS city,
    CASE
        WHEN UPPER(country) IN ('IND','IN','INDIA', 'BHARAT') THEN 'INDIA'
        WHEN UPPER(country) IN ('US','USA', 'U.S.') THEN 'USA'
        WHEN UPPER(country) IN ('GB','UK') THEN 'UK'
        ELSE 'OTHER'
    END AS country,
    CASE
        WHEN LOWER(status) IN ('active','ac','a') THEN 'ACTIVE'
        WHEN LOWER(status) IN ('closed','cl','clos') THEN 'CLOSED'
        WHEN LOWER(status) IN ('inactive','inac','in') THEN 'INACTIVE'
        ELSE 'UNKNOWN'
    END AS status,

    COALESCE(
    TRY_TO_TIMESTAMP(updated_at),
    TRY_TO_TIMESTAMP(updated_at, 'DD/MM/YYYY'),
    TRY_TO_TIMESTAMP(updated_at, 'YYYY/MM/DD'),
    TRY_TO_TIMESTAMP(updated_at, 'YYYY-MM-DD'),
    TRY_TO_TIMESTAMP(updated_at, 'DD-MM-YYYY')
    ) AS updated_at,

    CURRENT_TIMESTAMP AS pipeline_updated_at

from copper_customers