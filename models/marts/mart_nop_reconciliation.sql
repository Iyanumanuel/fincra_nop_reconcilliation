{{ config(materialized='table') }}

WITH joined AS (
    SELECT
        c.snapshot_date,
        c.currency,
        c.opening_position_usd AS computed_opening,
        s.opening_position_usd AS snapshot_opening,
        c.net_buys_usd AS computed_net_buys,
        s.net_buys_usd AS snapshot_net_buys,
        c.net_sells_usd AS computed_net_sells,
        s.net_sells_usd AS snapshot_net_sells,
        c.closing_position_usd AS computed_closing,
        s.closing_position_usd AS snapshot_closing
    FROM {{ ref('mart_computed_nop') }} c
    JOIN {{ source('raw', 'daily_nop_snapshot') }} s
      ON c.snapshot_date = s.snapshot_date
     AND c.currency = s.currency
),

divergences AS (
    SELECT
        *,
        (computed_closing - snapshot_closing) AS divergence_usd,
        CASE
            WHEN computed_opening != snapshot_opening
             AND (computed_net_buys != snapshot_net_buys
               OR computed_net_sells != snapshot_net_sells)
                THEN 'opening_and_flows'
            WHEN computed_opening != snapshot_opening
                THEN 'opening_only'
            WHEN computed_net_buys != snapshot_net_buys
              OR computed_net_sells != snapshot_net_sells
                THEN 'flows_only'
            ELSE 'unknown'
        END AS break_type
    FROM joined
    WHERE computed_closing != snapshot_closing
)

SELECT *
FROM divergences
ORDER BY currency, snapshot_date
