import requests
from utils.config import SLACK_WEBHOOK_URL, DRY_RUN
from utils.logging_utils import log

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
