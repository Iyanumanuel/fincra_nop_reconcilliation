def format_carry_break_alert(row):
    return (
        f"*⚠️ CARRY-FORWARD BREAK — {row['currency']}*\n"
        f"Date: {row['break_date']}\n"
        f"Opening today: `{row['opening_today']:.2f}`\n"
        f"Closing yesterday: `{row['closing_yesterday']:.2f}`\n"
        f"Break amount: `{row['break_amount']:.2f}`"
    )
