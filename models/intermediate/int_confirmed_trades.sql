{{
    config(
        materialized='incremental',
        unique_key='trade_ref',
        incremental_strategy='merge',
        partition_by={'trade_date':'date', 'currency':'string'}
    )
}}

WITH base AS (
    SELECT
        trade_ref,
        trade_date,
        currency,
        side,
        amount_local,
        rate,
        status,
        usd_equivalent,
        ingested_at
        -- status
    FROM {{ ref('int_trade_blotter_deduped') }}
    WHERE status = 'confirmed'
),

filtered AS (
    SELECT *
    FROM base
    {% if is_incremental() %}
      WHERE ingested_at >= (
          SELECT
              MAX(ingested_at) - INTERVAL '7 day'
          FROM {{ this }}
      )
    {% endif %}
)

SELECT *
FROM filtered
