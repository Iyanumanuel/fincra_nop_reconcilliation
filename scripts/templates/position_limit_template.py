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
