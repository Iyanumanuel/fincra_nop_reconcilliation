{{ config(materialized='view') }}

SELECT
    trade_ref,
    CAST(trade_date AS DATE) AS trade_date,
    CAST(value_date AS DATE) AS value_date,
    currency,
    UPPER(side) AS side,
    amount_local,
    rate,
    amount_local / NULLIF(rate, 0) AS usd_equivalent,
    counterparty,
    trader,
    status,
    notes,
    CAST(ingested_at AS TIMESTAMP) AS ingested_at
FROM {{ source('raw', 'trade_blotter') }}
