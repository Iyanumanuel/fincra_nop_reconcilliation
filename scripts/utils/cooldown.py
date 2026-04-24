import datetime as dt
from utils.config import COOLDOWN_MINUTES

def cooldown_active(con, alert_type, currency, now):
    row = con.execute(
        """
        SELECT last_triggered_at
        FROM alert_state
        WHERE alert_type = ? AND currency = ?
        """,
        [alert_type, currency],
    ).fetchone()

    if not row:
        return False

    last_triggered_at = row[0]
    return (now - last_triggered_at) < dt.timedelta(minutes=COOLDOWN_MINUTES)


def update_cooldown(con, alert_type, currency, now):
    con.execute(
        """
        INSERT INTO alert_state (alert_type, currency, last_triggered_at)
        VALUES (?, ?, ?)
        ON CONFLICT (alert_type, currency)
        DO UPDATE SET last_triggered_at = EXCLUDED.last_triggered_at
        """,
        [alert_type, currency, now],
    )
