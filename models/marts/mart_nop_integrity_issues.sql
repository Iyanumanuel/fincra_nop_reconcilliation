{{ config(materialized='view') }}

SELECT
    c1.snapshot_date AS day_n,
    c1.currency,
    c1.opening_position_usd AS opening_today,
    c0.closing_position_usd AS closing_yesterday,
    (c1.opening_position_usd - c0.closing_position_usd) AS break_amount
FROM {{ ref('mart_computed_nop') }} c1
JOIN {{ ref('mart_computed_nop') }} c0
  ON c1.currency = c0.currency
 AND c1.snapshot_date = c0.snapshot_date + INTERVAL 1 DAY
WHERE c1.opening_position_usd != c0.closing_position_usd