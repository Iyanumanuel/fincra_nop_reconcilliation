{{ config(materialized='view') }}

SELECT
    trade_date,
    currency,
    SUM(CASE WHEN side = 'BUY' THEN usd_equivalent ELSE 0 END) AS net_buys_usd,
    SUM(CASE WHEN side = 'SELL' THEN usd_equivalent ELSE 0 END) AS net_sells_usd,
    COUNT(*) AS confirmed_trade_count
FROM {{ ref('int_confirmed_trades') }}
GROUP BY 1, 2