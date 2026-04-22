{{ config(materialized='table') }}

SELECT
    trade_date,
    currency,
    SUM(CASE WHEN side = 'BUY' THEN usd_equivalent ELSE 0 END) AS missed_net_buys_usd,
    SUM(CASE WHEN side = 'SELL' THEN usd_equivalent ELSE 0 END) AS missed_net_sells_usd,
    SUM(
        CASE WHEN side = 'BUY' THEN usd_equivalent ELSE -usd_equivalent END
    ) AS net_nop_impact_usd,
    COUNT(*) AS late_trade_count
FROM {{ ref('int_late_arriving_trades') }}
GROUP BY trade_date, currency
ORDER BY currency, trade_date