{{ config(materialized='view') }}

WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY trade_ref
            ORDER BY ingested_at DESC
        ) AS rn
    FROM {{ ref('stg_trade_blotter_raw') }}
)

SELECT
    trade_ref,
    trade_date,
    value_date,
    currency,
    side,
    amount_local,
    rate,
    usd_equivalent,
    counterparty,
    trader,
    status,
    notes,
    ingested_at
FROM ranked
WHERE rn = 1

