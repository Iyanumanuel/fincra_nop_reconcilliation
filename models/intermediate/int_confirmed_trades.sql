{{ config(materialized='view') }}

SELECT
    trade_ref,
    trade_date,
    currency,
    side,
    usd_equivalent,
    status,
    ingested_at
FROM {{ ref('stg_trade_blotter_deduped') }}
WHERE status = 'confirmed'