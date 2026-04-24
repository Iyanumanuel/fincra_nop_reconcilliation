{{ config(materialized='view') }}

WITH late_trades AS (
    SELECT
        trade_ref,
        trade_date,
        currency,
        side,
        usd_equivalent,
        ingested_at,
        DATE_DIFF('day', trade_date, CAST(ingested_at AS DATE)) AS days_late
    FROM {{ ref('int_confirmed_trades') }}
    WHERE DATE_DIFF('day', trade_date, CAST(ingested_at AS DATE)) > 3
),

impact AS (
    SELECT
        trade_ref,
        trade_date,
        currency,
        side,
        usd_equivalent,
        ingested_at,
        SUM(CASE WHEN side = 'BUY' THEN usd_equivalent ELSE 0 END) AS missed_net_buys_usd,
        SUM(CASE WHEN side = 'SELL' THEN usd_equivalent ELSE 0 END) AS missed_net_sells_usd,
        SUM(
            CASE WHEN side = 'BUY' THEN usd_equivalent ELSE -usd_equivalent END
        ) AS net_nop_impact_usd,
        COUNT(*) AS late_trade_count,
        DATE(ingested_at) - DATE(trade_date) AS days_late
    FROM late_trades
    GROUP BY all
)

SELECT *
FROM impact
ORDER BY currency, trade_date