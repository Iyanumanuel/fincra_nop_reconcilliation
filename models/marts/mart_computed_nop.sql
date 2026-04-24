{{ config(materialized='table') }}

WITH calendar AS (
    SELECT
        snapshot_date,
        currency,
        is_trading_day
    FROM {{ ref('stg_daily_nop_snapshot') }}
),

flows AS (
    SELECT
        trade_date AS snapshot_date,
        currency,
        net_buys_usd,
        net_sells_usd,
        confirmed_trade_count
    FROM {{ ref('int_daily_trade_flows') }}
),

joined AS (
    SELECT
        c.snapshot_date,
        c.currency,
        c.is_trading_day,
        COALESCE(f.net_buys_usd, 0) AS net_buys_usd,
        COALESCE(f.net_sells_usd, 0) AS net_sells_usd,
        COALESCE(f.confirmed_trade_count, 0) AS confirmed_trade_count
    FROM calendar c
    LEFT JOIN flows f
      ON c.snapshot_date = f.snapshot_date
     AND c.currency = f.currency
),

with_opening AS (
    SELECT
        j.*,
        CASE
            WHEN j.snapshot_date = (
                SELECT MIN(snapshot_date)
                FROM calendar c2
                WHERE c2.currency = j.currency
            )
            THEN (
                SELECT opening_position_usd
                FROM {{ ref('stg_daily_nop_snapshot') }} s
                WHERE s.snapshot_date = j.snapshot_date
                  AND s.currency = j.currency
            )
            ELSE NULL
        END AS opening_position_usd
    FROM joined j
),

ordered AS (
    SELECT
        *,
        LAG(opening_position_usd) OVER (
            PARTITION BY currency ORDER BY snapshot_date
        ) AS prev_opening,
        LAG(net_buys_usd) OVER (
            PARTITION BY currency ORDER BY snapshot_date
        ) AS prev_buys,
        LAG(net_sells_usd) OVER (
            PARTITION BY currency ORDER BY snapshot_date
        ) AS prev_sells
    FROM with_opening
),

opening_filled AS (
    SELECT
        snapshot_date,
        currency,
        is_trading_day,
        net_buys_usd,
        net_sells_usd,
        confirmed_trade_count,

        CASE
            WHEN opening_position_usd IS NOT NULL THEN opening_position_usd
            ELSE (
                -- previous closing = previous opening + previous flows
                prev_opening
                + COALESCE(prev_buys, 0)
                - COALESCE(prev_sells, 0)
            )
        END AS opening_position_usd
    FROM ordered
),

final AS (
    SELECT
        snapshot_date,
        currency,
        round(opening_position_usd,2) AS opening_position_usd,
        round(net_buys_usd,2) AS net_buys_usd,
        round(net_sells_usd,2) AS net_sells_usd,

        CASE
            WHEN is_trading_day IS TRUE THEN
                round(opening_position_usd + net_buys_usd - net_sells_usd,2)
            ELSE
                round(opening_position_usd,2)  -- weekend carry-forward
        END AS closing_position_usd,

        confirmed_trade_count
    FROM opening_filled
)

SELECT *
FROM final
ORDER BY currency, snapshot_date