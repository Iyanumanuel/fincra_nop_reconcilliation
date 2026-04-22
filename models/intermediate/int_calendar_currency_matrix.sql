{{ config(materialized='view') }}

WITH dates AS (
    SELECT DISTINCT snapshot_date
    FROM {{ source('raw', 'daily_nop_snapshot') }}
),
currencies AS (
    SELECT DISTINCT currency
    FROM {{ source('raw', 'daily_nop_snapshot') }}
)

SELECT
    d.snapshot_date,
    c.currency
FROM dates d
CROSS JOIN currencies c