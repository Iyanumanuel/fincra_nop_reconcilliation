{{ config(
    materialized='incremental',
    unique_key='trade_ref',
    incremental_strategy='merge'
) }}

WITH src AS (
    SELECT *
    FROM {{ ref('int_confirmed_trades') }}

    {% if is_incremental() %}
        WHERE ingested_at >= (
            SELECT max(ingested_at) - INTERVAL '3 days'
            FROM {{ this }}
        )
    {% endif %}
)

SELECT *
FROM src