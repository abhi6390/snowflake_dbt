SELECT 
    CAST(account_id as VARCHAR(255)) as account_id,

    COALESCE(
    TRY_TO_DATE(balance_date, 'YYYY-MM-DD'),
    TRY_TO_DATE(balance_date, 'DD-MM-YYYY'),
    TRY_TO_DATE(balance_date, 'YYYY/MM/DD'),
    TRY_TO_DATE(balance_date, 'DD/MM/YYYY'),
    TRY_TO_TIMESTAMP(balance_date, 'YYYY-MM-DD HH24:MI:SS')
    ) AS balance_date,

    CAST(
    REGEXP_REPLACE(closing_balance, '[^0-9.-]', '')
    AS NUMBER(18,2)
    ) AS closing_balance,

    CASE
        WHEN UPPER(currency) IN ('₹','INR','IN') THEN 'INR'
        WHEN UPPER(currency) IN ('$','USD') THEN 'USD'
        ELSE 'OTHER'
    END AS currency

from {{ source('banking','raw_daily_balances') }}