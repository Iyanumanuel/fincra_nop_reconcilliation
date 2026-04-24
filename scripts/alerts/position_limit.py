from utils.cooldown import cooldown_active, update_cooldown
from utils.slack import send_slack
from utils.logging_utils import log
from utils.config import ALERT_POSITION
from utils.alert_writer import write_alert_event

from templates.position_limit_template import format_position_alert


def check_position_limit(con, now):
    log("Checking position limit breaches...")

    breaches = con.execute(
        """
        SELECT snapshot_date, currency, closing_position_usd,
               CASE
                   WHEN closing_position_usd > 2000000 THEN closing_position_usd - 2000000
                   WHEN closing_position_usd < -1500000 THEN closing_position_usd + 1500000
               END AS breach_amount
        FROM mart_computed_nop
        WHERE snapshot_date = CURRENT_DATE
          AND (closing_position_usd > 2000000 OR closing_position_usd < -1500000)
        """
    ).fetchdf()

    if breaches.empty:
        log("No position limit breaches found.")
        return

    for _, row in breaches.iterrows():
        currency = row["currency"]

        if cooldown_active(con, ALERT_POSITION, currency, now):
            log(f"Cooldown active for {ALERT_POSITION} / {currency}, skipping.")
            continue

        trades = con.execute(
            """
            SELECT trade_ref, trade_date, side, usd_equivalent
            FROM mart_confirmed_trades
            WHERE currency = ?
              AND trade_date <= ?
            ORDER BY trade_date DESC, ingested_at DESC
            LIMIT 3
            """,
            [currency, row["snapshot_date"]],
        ).fetchdf().to_dict("records")

        msg = format_position_alert(row, trades)

        # JSON payload
        payload = {
            "currency": currency,
            "current_position": float(row["closing_position_usd"]),
            "breach_amount": float(row["breach_amount"]),
            "last_3_trades": trades,
            "snapshot_date": str(row["snapshot_date"])
        }

        send_slack(msg)

        write_alert_event(
            con=con,
            alert_type=ALERT_POSITION,
            currency=currency,
            payload_dict=payload,
            now=now
        )

        update_cooldown(con, ALERT_POSITION, currency, now)

        log(f"Position limit alert sent for {currency}.")
