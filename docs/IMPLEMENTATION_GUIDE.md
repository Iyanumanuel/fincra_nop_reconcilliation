# Fincra NOP Reconciliation System - Comprehensive Implementation Guide

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [System Architecture Overview](#system-architecture-overview)
3. [Part 1: Data Cleaning and Ingestion](#part-1-data-cleaning-and-ingestion)
4. [Part 2: NOP Computation and Reconciliation](#part-2-nop-computation-and-CarryForward-reconciliation)
5. [Part 3: NOP Alerting System](#part-3-nop-alerting-system)
6. [Part 4: Production-Ready Architecture](#part-4-production-ready-architecture)
7. [Key Findings & Insights](#key-findings-and-insights)

---

## Executive Summary

This document provides a comprehensive explanation of the **Net Open Position (NOP) Computation, Reconciliation, and Alerting System**.


## System Architecture Overview

### What This System Does

The system performs four critical functions:

1. **Ingests & Cleans** trade blotter data with deduplication and normalization
2. **Computes NOP** deterministically using a daily reconciliation framework
3. **Reconciles** computed NOP against official snapshots with break classification
4. **Alerts** on position limits, carries forward breaks, and stale positions


### Technology Stack

| Component | Purpose | Technology |
|-----------|---------|-----------|
| **Data Warehouse** | Central repository | DuckDB (dev), BigQuery (prod) |
| **Ingestion** | Loading data from source | Python |
| **Transformation** | ELT framework | dbt (data build tool) |
| **Alerting** | notifications | Python + Slack |



## Part 1: Data Cleaning and Ingestion

### 1.1 Context & Requirements

The Fincra trade system generates a **trade blotter** (raw transaction log) and a **daily NOP snapshot** (authoritative position report). These are the two source datasets for our system.

**Key Challenge:** Raw data contains inconsistencies, duplicates, and quality issues that must be handled systematically before downstream consumption.

### 1.2 Data Quality Issues Discovered

#### Issue 1: Numeric Overflow

**Problem:**
- The `amount_local` column exceeded 32-bit integer range
- System-level type casting failures in some ETL pipelines

**Decision:**
- Used `NUMERIC` for column type
- Implement explicit type validation in staging layer

---


#### Issue 2: Rounding Noise & Precision

**Problem:**
- Computing `USD_equivalent = amount_local / rate` produces values that don't exactly match stored values in the `usd_equivalent` field
- At scale (1000s of trades), cent-level errors sum to dollars

**Decision:** Rounded to 2 decimal points to ensure consistency usd_equivalent consistency

#### Issue 3: NULL Status Values

**Problem:**
- 13 records in the trade blotter have `NULL` status

**Decision:**
- Filtered out all records with `NULL` status from progressing past staging staging layer
- Created dbt test to ensure records with NULL values don't get past stagiing layer


#### Issue 4: Duplicate Trade Records

**Problem:**
- The trade blotter contains multiple versions of the same trade
- Each version has a different `ingested_at` timestamp
- No deduplication flag in source system

**Decision:**
- **Deduplication strategy:** For each `trade_ref`, I kept only the record with the **latest `ingested_at`**
- Rationale: Assumes system corrections are always subsequent to the original record
- Trade-off: If earlier records represent corrections, this approach will miss them (but trade blotter architecture makes this unlikely)


---

### 1.4 Ingestion Workflow

**Steps:**
1. Load `trade_blotter.csv` and `daily_nop_snapshot.csv` into the database (DuckDb) via python script `load_data.py`
2. Run `stg_trade_blotter_raw` → Type casting & validation
3. Run `stg_trade_blotter_deduped` → Deduplication
4. Run `int_confirmed_trades` → Status filter
5. Manual validation: Check row counts match expectations
6. Proceed to NOP computation

---

## Part 2: NOP Computation and CarryForward Reconciliation

### NOP Fundamentals

**Definition:**
> **Net Open Position (NOP)** = The aggregate currency position of the firm across all counterparties at a given point in time.

**Fundamental Equation:**
```
Closing NOP = Opening NOP + Net Purchases - Net Sales
```

For each currency, each day.

**Key Principle:**
- NOP is **deterministic** given confirmed trades and opening position
- If correctly computed, NOP forms an unbroken chain across days
- Any break indicates either data quality issues or manual adjustments

The **Core Computation** Aggregate confirmed trades into daily flows by currency 

**Key Rules:**

1. **Day 1 Opening:** From `daily_nop_snapshot`
2. **Subsequent Openings:** From previous day's computed closing
3. **Non-Trading Days:** 
   - `opening = closing = previous business day closing`
   - `net_buys = 0`, `net_sells = 0`
4. **All Days:** `closing = opening + net_buys - net_sells`

### Break Classification
- **flows_break** — Flow  (net_buys/sells computation) divergence only
- **opening_break** — Opening divergence only
- **opening_and_flows_break** — Both opening and flows diverge
- **closing_break** — Closing divergence only
- **closing_and_flows_break** — Both closing and flows diverge

### 2.1 From `stg_trade_blotter_raw` → `int_trade_blotter_deduped`

The staging model (stg_trade_blotter_raw) performs:
- Type casting
- Basic cleaning

The in `int_trade_blotter_deduped`, the following actions happened

- Deduplicate trades by trade_ref
- `NULL` status values were excluded
- Keep the latest version based on ingested_at
- Ensure each trade has a single canonical record
- This gives us a clean, version‑corrected trade table.

### 2.2 From `int_trade_blotter_deduped` → `int_confirmed_trades` (Incremental Model)
`int_confirmed_trades` is an incremental model with a 7‑day lookback window (this covers for the 3 days period as well). It:
- Filters to status = 'CONFIRMED'
- Reprocesses only recent data to capture late-arriving updates
- Ensures historical correctness without recomputing the entire dataset
- This model forms the authoritative source of confirmed trades for NOP.


### 2.3 From `int_confirmed_trades` → `int_daily_trade_flows`
NOP is driven by daily net flows per currency. `int_daily_trade_flows` aggregates confirmed trades into:
- net_buys_usd
- net_sells_usd
- confirmed_trade_count

Each trade contributes:
- +USD equivalent for BUY
- –USD equivalent for SELL

This model produces the daily flow matrix that powers the NOP computation.


### 2.4 From `int_daily_trade_flows` → `mart_computed_nop`
`mart_computed_nop`computes the deterministic NOP chain using:

**Core Rules**
1. Day 1 opening comes from the official snapshot (daily_nop_snapshot).
2. Subsequent openings come from the previous day’s computed closing.
3. Non‑trading days carry forward the previous business day’s closing
4. Daily closing is always:
`closing = opening + net_buys - net_sells`

The output is a fully computed NOP chain that reflects only trade activity, independent of the snapshot.

### 2.5 From `mart_computed_nop` → `mart_nop_reconciliation`
`mart_nop_reconciliation` compares:
- Computed NOP (from trades)
- Snapshot NOP (official positions)
- For each currency and date, it identifies divergences between computed and snapshot data

This model surfaces all divergences between the trade‑driven computation and the official published numbers.


### 2.6 Carry-forward Chain and Late‑Arrival Impact (`int_late_arriving_trade_impact`)
Late-arriving trades are identified by comparing:
> days_late = DATE(ingested_at) - DATE(trade_date)
A trade is considered late if:
> days_late > 3 

The model quantifies:
- Missed net buys
- Missed net sells
- Net NOP impact
- Count of late trades
- Number of days late
This provides visibility into how ingestion delays distort NOP reporting. It shows “what NOP should have been” if those trades had arrived on time, and how much reported NOP was understated or overstated due to ingestion delays.




## Part 3: NOP Alerting System

### 3.1 Architecture Overview

The alerting system is implemented in Python (`alert_system.py`) with three distinct alert types:

- `Position Limit`
- `Carry-Forward`
- `Stale Position`

**Design Principles:**

1. **Actionable Only:** Alerts must drive action; no noise
2. **Cooldown Mechanism:** Prevent alert fatigue with per-(alert_type, currency) cooldowns
3. **Self-Contained:** Each alert is a complete message with context
4. **Durable State:** Alert history stored for audit trail and recovery

---

### 3.2 Alert Type 1: Position Limit Breach (`/scripts/alerts/position_limit.py`)

 ```sql
SELECT trade_ref, trade_date, side, usd_equivalent
FROM mart_confirmed_trades
WHERE currency = ?
    AND trade_date <= ?
ORDER BY trade_date DESC, ingested_at DESC
LIMIT 3
```


---

### 3.3 Alert Type 2: Carry-Forward Break  (`/scripts/alerts/carry_forward.py`)

```sql
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
```

### 3.4 Alert Type 3: Stale Position (`/scripts/alerts/stale_position.py`)

```sql
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

```

### 3.5 Cooldown Mechanism  (`/scripts/utils/cooldown.py`)


```python
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
```

**State Storage:**

Cooldown state is stored in `alert_state` table for durability:

```sql
CREATE TABLE alert_state (
  alert_type STRING,
  currency STRING,
  reference_date DATE,
  last_alert_datetime TIMESTAMP,
  PRIMARY KEY (alert_type, currency, reference_date)
)
```

---

### 3.6 Slack Integration 

**Message Templates:**
- Position limit breach  (`/scripts/template/position_limit_template.py`)
- Carry-forward break (`/scripts/template/carry_forward_template.py`)
- Stale position (`/scripts/template/stale_position_template.py`)



---

## Part 4: Production-Ready Architecture

See [ARCHITECTURE.md#system-overview](../ARCHITECTURE.md#system-overview) for the full breakdown.

## Key Findings and Insights

### 1. TZS and UGX dominate the extreme short‑position exposures
TZS (540 days) and UGX (518 days) appear most frequently with closing positions below –1.5M USD.
This indicates:
- Persistent, large directional short exposure
- Structural imbalance in trading flows
- Potentially intentional hedging strategy or chronic liquidity mismatch
- These two currencies should be top priority for monitoring.

### 2. NGN and XAF dominate the large long‑position exposures
```
NGN: 534 days
XAF: 517 days
```
These two currencies are almost always above +2M USD.

This means:
- They carry persistent, structural long exposure
- They are the highest‑risk currencies in terms of directional imbalance

### 3. TSX and UGX have never had a closing position above 2m
TSX and UGX have never had a closing position above +2M USD, indicating:
- They are predominantly short currencies for this desk
- The desk may have a structural bias or hedging strategy that limits long exposure in these currencies 

### 4. UGX has the highest number of stale days
UGX has the highest number (194) of stale days with the most consecutive being 4 days.

### 5. Overall pattern: The desk frequently carries large short and long positions
The distribution shows a systemic pattern:
- Multiple currencies regularly exceed the –1.5M USD threshold for short positions and 2M USD for long positions
- This is not an isolated event — it’s a daily operational reality
- The desk is structurally short across several African currencies

This is a strong justification for:
- Automated position‑limit alerts
- Daily monitoring
- Escalation thresholds


**Implications:**

| Implication | Impact |
|-----------|--------|
| **Operational Risk** | Large directional exposure held static signals hedging needs |
| **Data Quality Risk** | Stale positions can hide missing trades or late arrivals |

## Conclusion

This system demonstrates **production-grade data engineering**:

✅ Clean, deterministic computation  
✅ Transparent reconciliation and break handling  
✅ Actionable alerting with operational context  
✅ Scalable architecture for production deployment  
✅ Comprehensive data quality management  
