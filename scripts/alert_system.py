import datetime as dt
import duckdb
import requests
import os

# -----------------------------
# CONFIG
# -----------------------------
# Get path relative to project root
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, "../../dbt_project/fincra_dev.duckdb")
SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/XXX/YYY/ZZZ"

ALERT_POSITION = "position_limit_breach"
ALERT_CARRY = "carry_forward_break"
ALERT_STALE = "stale_position"


COOLDOWN_MINUTES = 30
DRY_RUN = True  # set to False to actually send Slack messages


# -----------------------------
# LOGGING
# -----------------------------
def log(msg: str):
    now = dt.datetime.now().isoformat(timespec="seconds")
    print(f"[{now}] [ALERTING] {msg}")


# -----------------------------
# DB CONNECTION
# -----------------------------
def get_connection():
    return duckdb.connect(DB_PATH)


# -----------------------------
# COOLDOWN LOGIC
# -----------------------------
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


# -----------------------------
# SLACK SENDER
# -----------------------------
def send_slack(message: str):
    if DRY_RUN:
        log("DRY RUN: would send Slack message:")
        print("\n--- SLACK MESSAGE ---")
        print(message)
        print("--- END MESSAGE ---\n")
        return

    resp = requests.post(SLACK_WEBHOOK_URL, json={"text": message})
    if resp.status_code != 200:
        log(f"Slack error: {resp.status_code} {resp.text}")


# -----------------------------
# MESSAGE TEMPLATES
# -----------------------------
def format_position_alert(row, trades):
    lines = [
        f"*🚨 POSITION LIMIT BREACH — {row['currency']}*",
        f"Date: {row['snapshot_date']}",
        f"Current NOP: `{row['closing_position_usd']:.2f} USD`",
        "",
        "*Last 3 trades:*" if trades else "*No recent trades found.*",
    ]
    for t in trades:
        lines.append(
            f"- `{t['trade_date']}` — {t['side']} `{t['usd_equivalent']:.2f}` USD (ref `{t['trade_ref']}`)"
        )
    return "\n".join(lines)


def format_carry_break_alert(row):
    return (
        f"*⚠️ CARRY-FORWARD BREAK — {row['currency']}*\n"
        f"Date: {row['break_date']}\n"
        f"Opening today: `{row['opening_today']:.2f}`\n"
        f"Closing yesterday: `{row['closing_yesterday']:.2f}`\n"
        f"Break amount: `{row['break_amount']:.2f}`"
    )


def format_stale_alert(currency, days, start, end):
    return (
        f"*ℹ️ STALE POSITION — {currency}*\n"
        f"No confirmed trades for `{days}` consecutive business days.\n"
        f"Streak: `{start}` → `{end}`\n"
        f"The desk may be carrying a static open position."
    )


# -----------------------------
# ALERT TYPE 1 — POSITION LIMIT
# -----------------------------
def check_position_limit(con, now):
    log("Checking position limit breaches...")
    breaches = con.execute(
        """
        SELECT snapshot_date, currency, closing_position_usd
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
        send_slack(msg)
        update_cooldown(con, ALERT_POSITION, currency, now)
        log(f"Position limit alert sent for {currency}.")


# -----------------------------
# ALERT TYPE 2 — CARRY-FORWARD BREAK
# -----------------------------
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
        send_slack(msg)
        update_cooldown(con, ALERT_CARRY, currency, now)
        log(f"Carry-forward break alert sent for {currency} on {row['break_date']}.")


# -----------------------------
# ALERT TYPE 3 — STALE POSITION
# -----------------------------
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
            end=row["streak_end"])
        send_slack(msg)
        update_cooldown(con, ALERT_STALE, currency, now)
        log(f"Stale position alert sent for {currency}.")


# -----------------------------
# MAIN
# -----------------------------
def main():
    log("Starting alerting system...")
    now = dt.datetime.now()
    con = get_connection()

    # Ensure cooldown table exists
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS alert_state (
            alert_type TEXT,
            currency TEXT,
            last_triggered_at TIMESTAMP,
            PRIMARY KEY (alert_type, currency)
        );
        """
    )

    check_position_limit(con, now)
    check_carry_forward_break(con, now)
    check_stale_position(con, now)

    log("Alerting run completed.")


if __name__ == "__main__":
    main()
