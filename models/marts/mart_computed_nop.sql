

WITH base AS (
    SELECT
        m.snapshot_date,
        m.currency,
        s.is_trading_day,
        COALESCE(f.net_buys_usd, 0) AS net_buys_usd,
        COALESCE(f.net_sells_usd, 0) AS net_sells_usd,
        f.confirmed_trade_count
    FROM {{ ref('int_calendar_currency_matrix') }} m
    LEFT JOIN {{ ref('int_daily_trade_flows') }} f
      ON m.snapshot_date = f.trade_date
     AND m.currency = f.currency
    LEFT JOIN {{ source('raw', 'daily_nop_snapshot') }} s
      ON m.snapshot_date = s.snapshot_date
     AND m.currency = s.currency
),

with_opening AS (
    SELECT
        b.*,
        CASE
            WHEN b.snapshot_date = (SELECT MIN(snapshot_date) FROM base)
            THEN (
                SELECT opening_position_usd
                FROM {{ source('raw', 'daily_nop_snapshot') }} s
                WHERE s.snapshot_date = b.snapshot_date
                  AND s.currency = b.currency
            )
        END AS opening_position_usd
        ,NULL AS closing_position_usd
    FROM base b
),

ordered AS (
    SELECT
        *,
        LAG(closing_position_usd) OVER (
            PARTITION BY currency ORDER BY snapshot_date
        ) AS prev_closing
    FROM with_opening
)

SELECT
    snapshot_date,
    currency,
    CASE
        WHEN opening_position_usd IS NOT NULL THEN opening_position_usd
        ELSE prev_closing
    END AS opening_position_usd,
    CASE
        WHEN is_trading_day THEN net_buys_usd ELSE 0 END AS net_buys_usd,
    CASE
        WHEN is_trading_day THEN net_sells_usd ELSE 0 END AS net_sells_usd,
    CASE
        WHEN is_trading_day
        THEN (
            COALESCE(opening_position_usd, prev_closing)
            + net_buys_usd
            - net_sells_usd
        )
        ELSE prev_closing
    END AS closing_position_usd,
    confirmed_trade_count
FROM ordered
ORDER BY currency, snapshot_date
