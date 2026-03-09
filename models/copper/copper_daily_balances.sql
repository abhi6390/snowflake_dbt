with copper_daily_balances as (
    select * FROM {{ ref('bronze_daily_balances') }}
)

SELECT 
    CAST(account_id as VARCHAR(255)) as account_id,

    {{ date_macro('balance_date') }} AS balance_date,

    CAST(
    REGEXP_REPLACE(closing_balance, '[^0-9.-]', '')
    AS NUMBER(18,2)
    ) AS closing_balance,

    CASE
        WHEN UPPER(currency) IN ('₹','INR','IN') THEN 'INR'
        WHEN UPPER(currency) IN ('$','USD') THEN 'USD'
        ELSE 'OTHER'
    END AS currency,
    CURRENT_TIMESTAMP AS pipeline_updated_at

from copper_daily_balances