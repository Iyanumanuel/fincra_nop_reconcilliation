{{config (materialized = 'view')}}

SELECT
  CAST(snapshot_date AS DATE) snapshot_date,
  currency,
  is_trading_day,
  opening_position_usd,
  net_buys_usd,
  net_sells_usd,
  closing_position_usd,
  confirmed_trade_count
FROM  {{source ('raw', 'daily_nop_snapshot')}}