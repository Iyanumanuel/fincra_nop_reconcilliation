from utils.db import get_connection
from utils.logging_utils import log
import datetime as dt

from alerts.position_limit import check_position_limit
from alerts.carry_forward import check_carry_forward_break
from alerts.stale_position import check_stale_position


def main():
    log("Starting alerting system...")
    now = dt.datetime.now()
    con = get_connection()

    # Ensure cooldown table exists
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS alert_events (
            triggered_at TIMESTAMP,
            alert_type TEXT,
            currency TEXT,
            payload JSON,
            PRIMARY KEY (triggered_at, alert_type, currency)
        );
        """
    )


    # Run all alert types
    check_position_limit(con, now)
    check_carry_forward_break(con, now)
    check_stale_position(con, now)

    log("Alerting run completed.")


if __name__ == "__main__":
    main()
