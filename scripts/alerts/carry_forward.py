from utils.cooldown import cooldown_active, update_cooldown
from utils.slack import send_slack
from utils.logging_utils import log
from utils.config import ALERT_CARRY
from utils.alert_writer import write_alert_event

from templates.carry_forward_template import format_carry_break_alert


def check_carry_forward_break(con, now):
    log("Checking carry-forward breaks...")

    breaks = con.execute(
        """
        SELECT
            c1.snapshot_date AS break_date,
            c1.currency,
            c1.opening_position_usd AS opening_today,
            c0.closing_position_usd AS closing_yesterday,
            (c1.opening_position_usd - c0.closing_position_usd) AS break_amount
        FROM mart_computed_nop c1
        JOIN mart_computed_nop c0
          ON c1.currency = c0.currency
         AND c1.snapshot_date = c0.snapshot_date + INTERVAL 1 DAY
        WHERE c1.opening_position_usd != c0.closing_position_usd
        """
    ).fetchdf()

    if breaks.empty:
        log("No carry-forward breaks found.")
        return

    for _, row in breaks.iterrows():
        currency = row["currency"]

        if cooldown_active(con, ALERT_CARRY, currency, now):
            log(f"Cooldown active for {ALERT_CARRY} / {currency}, skipping.")
            continue

        msg = format_carry_break_alert(row)

        payload = {
            "currency": currency,
            "break_amount": float(row["break_amount"]),
            "break_date": str(row["break_date"]),
            "opening_today": float(row["opening_today"]),
            "closing_yesterday": float(row["closing_yesterday"])
        }

        send_slack(msg)

        write_alert_event(
            con=con,
            alert_type=ALERT_CARRY,
            currency=currency,
            payload_dict=payload,
            now=now
        )

        update_cooldown(con, ALERT_CARRY, currency, now)

        log(f"Carry-forward break alert sent for {currency}.")
