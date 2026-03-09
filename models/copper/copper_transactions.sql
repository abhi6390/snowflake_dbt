with copper_transactions as (
    select * FROM {{ ref('bronze_transactions') }}
)

SELECT
    CAST(transaction_id AS VARCHAR(255)) AS transaction_id,
    CAST(account_id as VARCHAR(255)) as account_id,

    {{ date_macro('txn_date') }} AS txn_date,

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

    COALESCE(UPPER(merchant), 'OTHER') as merchant,

    CURRENT_TIMESTAMP AS pipeline_updated_at


from copper_transactions