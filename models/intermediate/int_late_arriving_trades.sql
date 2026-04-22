{{ config(materialized='view') }}

SELECT
    trade_ref,
    trade_date,
    currency,
    side,
    usd_equivalent,
    ingested_at,
    DATE_DIFF('day', trade_date, CAST(ingested_at AS DATE)) AS days_late
FROM {{ ref('mart_confirmed_trades') }}
WHERE DATE_DIFF('day', trade_date, CAST(ingested_at AS DATE)) > 3