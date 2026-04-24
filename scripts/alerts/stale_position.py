from utils.cooldown import cooldown_active, update_cooldown
from utils.slack import send_slack
from utils.logging_utils import log
from utils.config import ALERT_STALE
from utils.alert_writer import write_alert_event

from templates.stale_position_template import format_stale_alert


def check_stale_position(con, now):
    log("Checking stale positions...")

    stale = con.execute(
        """
        WITH ordered AS (
            SELECT
                snapshot_date,
                currency,
                confirmed_trade_count,
                CASE WHEN confirmed_trade_count = 0 THEN 1 ELSE 0 END AS is_zero_trade,
                ROW_NUMBER() OVER (
                    PARTITION BY currency
                    ORDER BY snapshot_date
                ) AS rn
            FROM mart_computed_nop
            WHERE snapshot_date <= CURRENT_DATE
              AND snapshot_date >= CURRENT_DATE - INTERVAL 15 DAY
        ),
        streak_groups AS (
            SELECT
                *,
                SUM(CASE WHEN is_zero_trade = 0 THEN 1 ELSE 0 END)
                    OVER (PARTITION BY currency ORDER BY rn) AS group_id
            FROM ordered
        ),
        streak_lengths AS (
            SELECT
                currency,
                group_id,
                COUNT(*) AS streak_length,
                MIN(snapshot_date) AS streak_start,
                MAX(snapshot_date) AS streak_end
            FROM streak_groups
            WHERE is_zero_trade = 1
            GROUP BY currency, group_id
        )
        SELECT currency, streak_length, streak_start, streak_end
        FROM streak_lengths
        WHERE streak_length >= 5
        """
    ).fetchdf()

    if stale.empty:
        log("No stale positions found.")
        return

    for _, row in stale.iterrows():
        currency = row["currency"]

        if cooldown_active(con, ALERT_STALE, currency, now):
            log(f"Cooldown active for {ALERT_STALE} / {currency}, skipping.")
            continue

        msg = format_stale_alert(
            currency=row["currency"],
            days=row["streak_length"],
            start=row["streak_start"],
            end=row["streak_end"]
        )

        payload = {
            "currency": currency,
            "streak_length": int(row["streak_length"]),
            "streak_start": str(row["streak_start"]),
            "streak_end": str(row["streak_end"])
        }

        send_slack(msg)

        write_alert_event(
            con=con,
            alert_type=ALERT_STALE,
            currency=currency,
            payload_dict=payload,
            now=now
        )

        update_cooldown(con, ALERT_STALE, currency, now)

        log(f"Stale position alert sent for {currency}.")
