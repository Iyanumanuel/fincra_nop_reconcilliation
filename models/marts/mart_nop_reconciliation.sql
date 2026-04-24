{{ config(materialized='table') }}


WITH snapshot AS (
    SELECT
        snapshot_date,
        currency,
        opening_position_usd AS snapshot_opening,
        closing_position_usd AS snapshot_closing,
        net_buys_usd AS snapshot_net_buys,
        net_sells_usd AS snapshot_net_sells
    FROM {{ ref('stg_daily_nop_snapshot') }}
),

joined AS (
    SELECT
        c.snapshot_date,
        c.currency,
        c.opening_position_usd AS computed_opening,
        s.snapshot_opening,
        c.net_buys_usd AS computed_net_buys,
        s.snapshot_net_buys,
        c.net_sells_usd AS computed_net_sells,
        s.snapshot_net_sells,
        c.closing_position_usd AS computed_closing,
        s.snapshot_closing
    FROM {{ ref('mart_computed_nop') }} c
    LEFT JOIN snapshot s
      ON c.snapshot_date = s.snapshot_date
     AND c.currency = s.currency
),

snapshot_breaks AS (
    SELECT
        snapshot_date,
        currency,
        computed_opening,
        snapshot_opening,
        computed_net_buys,
        snapshot_net_buys,
        computed_net_sells,
        snapshot_net_sells,
        computed_closing,
        snapshot_closing,
        (computed_closing - snapshot_closing) AS divergence_usd,
        CASE
            WHEN computed_opening != snapshot_opening
             AND (computed_net_buys != snapshot_net_buys
               OR computed_net_sells != snapshot_net_sells)
                THEN 'opening_and_flows_break'
            WHEN computed_closing != snapshot_closing
             AND (computed_net_buys != snapshot_net_buys
               OR computed_net_sells != snapshot_net_sells)
                THEN 'closing_and_flows_break'
            WHEN computed_net_buys != snapshot_net_buys
              OR computed_net_sells != snapshot_net_sells
                THEN 'flows_break'
            WHEN computed_opening != snapshot_opening
                THEN 'opening_break'
            WHEN computed_closing != snapshot_closing
                THEN 'closing_break'
            ELSE 'snapshot_unknown'
        END AS break_type
    FROM joined
    WHERE computed_closing != snapshot_closing
),

integrity_breaks AS (
    SELECT
        c.snapshot_date,
        c.currency,
        c.opening_position_usd AS computed_opening,
        NULL AS snapshot_opening,
        c.net_buys_usd AS computed_net_buys,
        NULL AS snapshot_net_buys,
        c.net_sells_usd AS computed_net_sells,
        NULL AS snapshot_net_sells,
        c.closing_position_usd AS computed_closing,
        NULL AS snapshot_closing,
        (c.opening_position_usd - LAG(c.closing_position_usd) OVER (
            PARTITION BY c.currency ORDER BY c.snapshot_date
        )) AS divergence_usd,
        'integrity_chain_break' AS break_type
    FROM {{ ref('mart_computed_nop') }} c
),

integrity_filtered AS (
    SELECT *
    FROM integrity_breaks
    WHERE divergence_usd != 0
)

SELECT *
FROM snapshot_breaks

UNION ALL

SELECT *
FROM integrity_filtered

ORDER BY currency, snapshot_date