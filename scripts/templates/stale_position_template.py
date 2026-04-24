def format_stale_alert(currency, days, start, end):
    return (
        f"*ℹ️ STALE POSITION — {currency}*\n"
        f"No confirmed trades for `{days}` consecutive business days.\n"
        f"Streak: `{start}` → `{end}`\n"
        f"The desk may be carrying a static open position."
    )
