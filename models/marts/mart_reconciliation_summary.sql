-- models/marts/reconciliation_summary.sql
{{ config(materialized='table') }}

WITH divergences AS (
    SELECT *
    FROM {{ ref('mart_nop_reconciliation') }}
),

-- mark first break per currency as a "root" break
root_breaks AS (
    SELECT
        currency,
        snapshot_date AS break_date,
        divergence_usd,
        computed_closing AS expected_position,
        snapshot_closing AS actual_snapshot_position,
        ROW_NUMBER() OVER (
            PARTITION BY currency
            ORDER BY snapshot_date
        ) AS rn
    FROM divergences
),

-- for each break, count how many subsequent days remain divergent
chains AS (
    SELECT
        d.currency,
        d.snapshot_date AS break_date,
        d.divergence_usd,
        d.computed_closing AS expected_position,
        d.snapshot_closing AS actual_snapshot_position,
        COUNT(*) OVER (
            PARTITION BY d.currency
            ORDER BY d.snapshot_date
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        ) AS days_affected
    FROM divergences d
)

SELECT
    currency,
    break_date,
    expected_position,
    actual_snapshot_position,
    divergence_usd AS divergence_amount,
    days_affected
FROM chains
ORDER BY currency, break_date