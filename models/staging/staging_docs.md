# Trade Blotter Staging Models

## stg_trade_blotter_raw
Raw ingestion of the trade blotter CSV. Performs basic type casting and recomputes `usd_equivalent`.

## stg_trade_blotter_deduped
Deduplicates trades by keeping only the latest `ingested_at` per `trade_ref`.  
This ensures the final status (confirmed/pending/reversed) is authoritative.

## int_confirmed_trades
Filters to confirmed trades only.  
These are the only trades that contribute to NOP.
