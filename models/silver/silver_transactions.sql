SELECT
    CAST(transaction_id AS VARCHAR(255)) AS transaction_id,
    CAST(account_id as VARCHAR(255)) as account_id,

    COALESCE(
    TRY_TO_DATE(txn_date, 'YYYY-MM-DD'),
    TRY_TO_DATE(txn_date, 'DD-MM-YYYY'),
    TRY_TO_DATE(txn_date, 'YYYY/MM/DD'),
    TRY_TO_DATE(txn_date, 'DD/MM/YYYY'),
    TRY_TO_TIMESTAMP(txn_date, 'YYYY-MM-DD HH24:MI:SS')
    ) AS txn_date,

    -- CAST(
    -- REGEXP_REPLACE(amount, '[^0-9.-]', '')
    -- AS NUMBER(18,2)
    -- ) AS amount,

    ABS(CAST(
    REGEXP_REPLACE(amount, '[^0-9.-]', '')
    AS NUMBER(18,2)
    )) AS amount,

    CASE
        WHEN UPPER(txn_type) IN ('CR', 'CREDIT') THEN 'CREDIT'
        WHEN UPPER(txn_type) IN ('DR', 'DEBIT') THEN 'DEBIT'
        ELSE 'OTHER'
    END AS txn_type,

    UPPER(channel) AS channel,

    COALESCE(UPPER(merchant), 'OTHER') as merchant

from {{ source('banking','raw_transactions') }}