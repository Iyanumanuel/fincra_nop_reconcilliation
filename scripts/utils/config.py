import os

# Base directory of the project
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, "../../database/fincra_dev.duckdb")

# Slack webhook
SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/XXX/YYY/ZZZ"

# Alert types
ALERT_POSITION = "position_limit_breach"
ALERT_CARRY = "carry_forward_break"
ALERT_STALE = "stale_position"

# Cooldown settings
COOLDOWN_MINUTES = 30

# Dry run mode (no Slack messages sent)
DRY_RUN = True
