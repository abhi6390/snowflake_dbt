with copper_accounts as (
    select * FROM {{ ref('bronze_accounts') }}
)

SELECT
    CAST(account_id AS VARCHAR(255)) AS account_id,
    CAST(customer_id AS VARCHAR(255)) AS customer_id,

    CASE
        WHEN UPPER(account_type) IN ('FD','F') THEN 'FD'
        WHEN UPPER(account_type) IN ('SAV','S','SAVING','SAVINGS') THEN 'SAVINGS'
        WHEN UPPER(account_type) IN ('CUR', 'C', 'CURR', 'CURRENT') then 'CURRENT'
        WHEN UPPER(account_type) IN ('LOAN', 'L') THEN 'LOAN'
        ELSE 'OTHER'
    END AS account_type,

    COALESCE(
    TRY_TO_DATE(open_date, 'YYYY-MM-DD'),
    TRY_TO_DATE(open_date, 'DD-MM-YYYY'),
    TRY_TO_DATE(open_date, 'YYYY/MM/DD'),
    TRY_TO_DATE(open_date, 'DD/MM/YYYY'),
    TRY_TO_TIMESTAMP(open_date, 'YYYY-MM-DD HH24:MI:SS')
    ) AS open_date,

    CASE
        WHEN LOWER(status) IN ('active','ac','a') THEN 'ACTIVE'
        WHEN LOWER(status) IN ('closed','cl','clos') THEN 'CLOSED'
        WHEN LOWER(status) IN ('inactive','inac','in') THEN 'INACTIVE'
        ELSE 'UNKNOWN'
    END AS status,

    COALESCE(branch_code, 'OTHER') AS branch_code,

    CURRENT_TIMESTAMP AS pipeline_updated_at

from copper_accounts