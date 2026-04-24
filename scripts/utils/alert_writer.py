import json
from utils.logging_utils import log

def write_alert_event(con, alert_type, currency, payload_dict, now):
    con.execute(
        """
        INSERT INTO alert_events (
            triggered_at,
            alert_type,
            currency,
            payload
        )
        VALUES (?, ?, ?, ?)
        """,
        [now, alert_type, currency, json.dumps(payload_dict)],
    )
    log(f"Alert event written to table: {alert_type} / {currency}")
