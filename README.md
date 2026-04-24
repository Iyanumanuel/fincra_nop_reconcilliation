# Fincra – NOP Computation, Reconciliation & Alerting System

**Author:** Iyanuloluwa  
**Role:** Senior Data Engineer Candidate

---

## 📋 Overview

This project implements a full **Net Open Position (NOP)** computation pipeline for Fincra, including:

- ✅ Clean ingestion & deduplication of trade data
- ✅ Deterministic NOP computation from scratch
- ✅ Full reconciliation against the official snapshot
- ✅ Break classification & carry-forward chain analysis
- ✅ Late-arrival impact quantification
- ✅ Real-time alerting system with Slack integration
- ✅ Proposed real-time architecture for production

**Goal:** Demonstrate **data engineering judgment**, not just SQL. Every model, assumption, and decision is explicitly documented.

---

## 📌 Assumptions

| Assumption | Details |
|-----------|---------|
| **Trade blotter is source of truth** | The snapshot is authoritative for reporting, but trades determine what should have happened |
| **Latest ingested_at wins** | If multiple versions of a trade exist, the most recent ingestion timestamp is authoritative |
| **NOP attribution is by trade_date** | Not by ingestion date. Late arrivals must be retroactively applied |
| **Snapshot may contain manual adjustments** | Reconciliation must surface discrepancies rather than overwrite them |
| **Non-trading days carry forward** | `opening = closing = previous business day closing` |
| **All flows in USD equivalent** | Computed as `amount_local / rate` |

---

## 🏗️ Part 1 — Data Cleaning & Ingestion

### Data Quality Issues
#### Trade Blotter

- **Numeric overflow** - `amount_local` exceeded integer range
- **NULL Status Values** - 13 Records had `NULL` status
- **Rounding issues** - Incosistent rounding difference in `usd_equivalent` against `amount_local/ rate` accumulate over time leading to inconsistent total sum
- **Snapshot inconsistencies** - Some rows violate the fundamental NOP equation
- **Carry-forward breaks** - Some `opening[N] != closing[N-1]`
- **Confirmed trade counts inconsistent with flows** - Snapshot counts don't match blotter-derived flows

### Key Models

| Model | Purpose |
|-------|---------|
| `stg_trade_blotter_raw` | Type casting, normalization | 
| `stg_trade_blotter_deduped` | Dedup by trade_ref using latest ingested_at |
| `int_confirmed_trades` | Filter to confirmed trades only |
| `mart_confirmed_trades` | Incremental 3-day lookback mart |

### Reasoning

- **Deduplication is essential** — the trade blotter contains multiple versions of the same trade
- **Confirmed trades only** — the only ones that contribute to NOP
- **3-day lookback strategy** — captures late arrivals without recomputing the entire dataset

---

## 🧮 Part 2 — NOP Computation & Reconciliation

This is the **core** of the assignment.

### Requirement Mapping (Requirements 7–13)

| Req | Description | Model(s) |
|-----|-------------|----------|
| 7 | Compute opening, flows, closing | `int_daily_trade_flows`, `mart_computed_nop` |
| 8 | Weekend carry-forward logic | `mart_computed_nop` |
| 9 | Day-over-day integrity check | `mart_nop_integrity_issues` |
| 10 | Divergence detection | `mart_nop_reconciliation` |
| 11 | Break classification | `mart_nop_reconciliation` |
| 12 | Carry-forward chain length | `reconciliation_summary` |
| 13 | Late-arrival impact | `int_late_arriving_trades`, `mart_late_arrival_impact` |

### 2.1 NOP Computation Logic

NOP is computed per currency per day using this fundamental equation:

```sql
closing = opening + net_buys - net_sells
```

**Key rules:**

- **Day 1 opening** comes from `daily_nop_snapshot`
- **Subsequent openings** come from previous day's computed closing
- **On non-trading days:**
  - `net_buys = 0`
  - `net_sells = 0`
  - `opening = closing = previous business day closing`

This produces a **fully deterministic NOP series**.

### 2.2 Reconciliation Logic

#### A. Snapshot vs Computed Divergence

`mart_nop_reconciliation` surfaces rows where computed and snapshot values diverge:

```sql
computed_closing != snapshot_closing
```

#### B. Break Classification

Breaks are categorized into four types:

- **`opening_only`** — Divergence in opening position only
- **`flows_only`** — Divergence in flows only
- **`opening_and_flows`** — Both opening and flows diverge
- **`unknown`** — Cannot be classified

#### C. Carry-Forward Chain

Once a break occurs, it contaminates all subsequent days until the snapshot realigns.

The `reconciliation_summary` model computes:

- Break date
- Expected vs actual position
- Divergence amount
- Number of days affected (carry-forward chain length)

### 2.3 Late-Arriving Trades

Trades are flagged as late-arriving when:

```sql
ingested_at - trade_date > 3 days
```

These are captured in `int_late_arriving_trades` with impact analysis in `mart_late_arrival_impact`.

**Questions answered:**

- Which trades would a naive ingestion pipeline miss?
- How much NOP would be misreported?
- Which days are affected?

---

## 🚨 Part 3 — Alerting System

A real-time alerting system implemented in Python (`alerting_system.py`) with three alert types:

### Alert Types

#### 1. **Position Limit Breach**
- Triggers when position exceeds:
  - **+$2M long**
  - **< –$1.5M short**
- Includes the last 3 trades contributing to the position

#### 2. **Carry-Forward Break**
- Alerts when: `opening[N] != closing[N-1]`
- Indicates system anomalies or manual adjustments

#### 3. **Stale Position**
- Triggers on 5 consecutive business days with zero confirmed trades
- Uses a proper consecutive-day streak detector

### Additional Features

| Feature | Purpose |
|---------|---------|
| **30-minute cooldown** | Per (alert_type, currency) pair to prevent alert spam |
| **Slack integration** | Self-contained messages for immediate visibility |
| **Durable state** | Stored in `alert_state` table for recovery |

### Design Rationale

- **Actionable alerts** — must work without dashboards
- **Cooldown mechanism** — prevents notification fatigue
- **Stale position alerts** — large directional exposure held for days signals hedging needs

---

## 🏢 Part 4 — Real-Time Architecture

A production-ready real-time NOP system using Google Cloud Platform:

### Components

| Component | Purpose |
|-----------|---------|
| **Pub/Sub** | Trade ingestion stream |
| **Dataflow** | Streaming dedup, confirmed-only filter, NOP deltas |
| **BigQuery** | Real-time & batch storage |
| **Cloud Run** | Alerting system (serverless) |
| **dbt** | Authoritative batch recomputation |

### Key Principles

- **Streaming is fast; batch is correct** — real-time for speed, nightly batch for truth
- **Late arrivals trigger retroactive corrections** — streaming can't wait, batch recomputes
- **Nightly reconciliation overwrites real-time values** — batch results are authoritative
- **Micro-batching (1–5 minutes)** — cost-optimal for Fincra's trading volume

---

## ⚠️ Part 5 — Data Quality Issues Found

The dataset contains deliberate issues. Here are the major ones and decisions made:

### 1. Numeric Overflow in Trade Blotter

**Issue:** `amount_local` exceeded 32-bit integer range

**Decision:** Cast to `BIGINT`/`DECIMAL` in seeds

### 2. Snapshot Arithmetic Inconsistencies

**Issue:** Some rows violate the fundamental equation:

```sql
closing = opening + net_buys - net_sells
```

**Decision:** Treat snapshot as authoritative but fallible; surface discrepancies in reconciliation

### 3. Carry-Forward Breaks

**Issue:** Some `opening[N] != closing[N-1]`

**Decision:** Surface via `mart_nop_integrity_issues` and alert on them

### 4. Confirmed Trade Counts Inconsistent with Flows

**Issue:** Some days show large flows with few trades or vice versa

**Decision:** Ignore snapshot counts; derive flows directly from trades

### 5. Rounding Noise

**Issue:** Small cent-level differences accumulate

**Decision:** Use high-precision decimals; surface differences but don't "fix" them

---

## 🔍 Part 6 — Breaks Found

Two major classes of breaks surfaced:

### 1. Day-over-Day Integrity Breaks

**Pattern:** Opening does not match previous closing

**Indication:** Manual adjustments or snapshot errors

### 2. Snapshot vs Computed NOP Breaks

**Pattern:** Computed NOP diverges from snapshot

**Common causes:**
- Late-arriving trades
- Incorrect snapshot flows
- Manual overrides

**Tracking:** Fully quantified in `reconciliation_summary`

---

## 💡 Part 7 — Surprising Insights

### The Stale Position Pattern

**Observation:** Some currencies (especially **TZS** and **ZMW**) carry multi-million USD short positions for long stretches—unchanged across weekends and sometimes weeks.

**Implications:**

| Type | Impact |
|------|--------|
| **Operational Risk** | Large directional exposure held static for days signals hedging needs |
| **Data Quality Risk** | Stale positions can hide missing trades or late arrivals |

**Result:** This made the stale-position alert especially meaningful for hedging operations.

---

## 📝 Part 8 — Final Thoughts

### What This Project Demonstrates

✅ Clean, reproducible data engineering  
✅ Strong reasoning around correctness vs latency trade-offs  
✅ Clear separation of batch vs streaming responsibilities  
✅ Practical alerting and operational awareness  
✅ Transparent handling of data quality issues  

### Next Steps for Production

If this were deployed as a real production system:

- [ ] Add CI/CD for dbt + alerting system
- [ ] Add data contracts for trade ingestion
- [ ] Add dashboards for NOP trends & hedging signals
- [ ] Implement automated break resolution workflows
- [ ] Add SLA monitoring for alert delivery
- [ ] Build audit logging for position corrections

---

## 📂 Project Structure

```
fincra_nop_reconcilliation/
├── README.md                          # This file
├── dbt_project.yml                   # dbt configuration
├── requirements.txt                  # Python dependencies
│
├── models/
│   ├── staging/                      # Data cleaning & normalization
│   │   ├── stg_trade_blotter_raw.sql
│   │   ├── stg_trade_blotter_deduped.sql
│   │   └── sources.yml
│   │
│   ├── intermediate/                 # Business logic layer
│   │   ├── int_confirmed_trades.sql
│   │   ├── int_daily_trade_flows.sql
│   │   ├── int_late_arriving_trades.sql
│   │   └── int_calendar_currency_matrix.sql
│   │
│   └── marts/                        # Final reporting layer
│       ├── mart_confirmed_trades.sql
│       ├── mart_computed_nop.sql
│       ├── mart_nop_reconciliation.sql
│       ├── mart_nop_integrity_issues.sql
│       ├── mart_late_arrival_impact.sql
│       └── mart_reconciliation_summary.sql
│
├── seeds/                            # Source data
│   ├── trade_blotter.csv
│   └── daily_nop_snapshot.csv
│
├── scripts/                          # Utility scripts
│   ├── alert_system.py               # Real-time alerting
│   └── alert_system.ipynb            # Alert system notebook
│
├── database/
│   └── fincra_dev.duckdb             # Local development database
│
└── target/                           # Compiled artifacts (generated)
    ├── compiled/
    ├── run/
    └── manifest.json
```

---

## 🚀 Getting Started

### Prerequisites

- Python 3.9+
- dbt 1.0+
- DuckDB (or appropriate data warehouse)

### Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Initialize dbt project
dbt deps

# Run dbt models
dbt run

# Run tests
dbt test
```

---

## 📞 Questions & Support

For questions about the implementation or assumptions, refer to the inline documentation in each model file.

