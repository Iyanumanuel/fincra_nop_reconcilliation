# SDE Assignment — Iyannuloluwa Emmnauel


## 📋 Overview

This project implements a comprehensive **Net Open Position (NOP)** computation and reconciliation system, including:

- ✅ Data cleaning & deduplication of trade records
- ✅ Deterministic NOP computation by currency and day
- ✅ Reconciliation against official snapshots with break classification
- ✅ Late-arrival trade impact analysis
- ✅ Alerting system (position limits, breaks, stale positions)
- ✅ Production-ready architecture for Google Cloud Platform

---

## 📂 Quick Navigation

### For Detailed Implementation Information
👉 **See [docs/IMPLEMENTATION_GUIDE.md](docs/IMPLEMENTATION_GUIDE.md)** for comprehensive documentation

### Key Sections in Implementation Guide:
1. **[System Architecture Overview](docs/IMPLEMENTATION_GUIDE.md#system-architecture-overview)** — High-level data flow
2. **[Data Cleaning & Ingestion](docs/IMPLEMENTATION_GUIDE.md#part-1-data-cleaning-and-ingestion)** — 6 data quality issues + fixes
3. **[NOP Computation & Reconciliation](docs/IMPLEMENTATION_GUIDE.md#part-2-nop-computation-and-CarryForward-reconciliation)** — Core logic & models
4. **[Real-Time Alerting System](docs/IMPLEMENTATION_GUIDE.md#part-3-nop-alerting-system)** — 3 alert types with examples
5. **[Production Architecture](docs/IMPLEMENTATION_GUIDE.md#part-4--production-ready-architecture)** — GCP deployment guide
6. **[Key Findings & Insights](docs/IMPLEMENTATION_GUIDE.md#key-findings--insights)** — Stale positions, breaks, late arrivals
7. **[Deployment & Operations](docs/IMPLEMENTATION_GUIDE.md#deployment--operations)** — Runbooks & monitoring

---

## 🚀 Getting Started

### Prerequisites
- Python 3.9+
- dbt 1.0+
- DuckDB (development) or BigQuery (production)

### Quick Setup

```bash
# Create a virtual environment
python -m venv .venv and activate it 

# Install dependencies
pip install -r requirements.txt

# Initialize dbt
dbt deps

# Run all models
dbt run

# Run tests
dbt test
```
---

## 🎯 Core Concepts

### NOP Fundamental Equation
```
closing_position = opening_position + Net Purchases - Net Sales
```

### Key Assumptions
| Assumption | Rationale |
|-----------|-----------|
| Trade blotter = source of truth | Snapshot authoritative for reporting but trades determine correct NOP |
| Latest `ingested_at` wins | Handles duplicate records from retries/corrections |
| NOP by `trade_date` | Late arrivals applied retroactively via batch recomputation |
| Non-trading days carry forward | `opening = closing = previous business day closing` |

---

## 🔍 Key Findings
 1. TZS and UGX dominate the extreme short‑position exposures
 2. NGN and XAF dominate the large long‑position exposures
 3. TSX and UGX have never had a closing position above 2m
 4. UGX has the highest number of stale days
 5. The desk frequently carries large short and long positions

See [IMPLEMENTATION_GUIDE.md - Key Findings and Insights](docs/IMPLEMENTATION_GUIDE.md#key-findings-and-insights) for details.

---

## ⚠️ Data Quality Issues Fixed

- Issue 1: Numeric Overflow
- Issue 2: Rounding Noise & Precision
- Issue 3: NULL Status Values
- Issue 4: Duplicate Trade Records

See [IMPLEMENTATION_GUIDE - Data Cleaning and Ingestion](docs/IMPLEMENTATION_GUIDE.md#part-1-data-cleaning-and-ingestion) for detailed analysis.

---

## 🚨 Alerting System

The system generates 3 alert types:

1. **Position Limit Breach** — Long > +2M USD or Short < -1.5M USD
2. **Carry-Forward Break** — `opening[N] != closing[N-1]`
3. **Stale Position** — 5+ consecutive business days with zero trades

See [IMPLEMENTATION_GUIDE - NOP Alerting System](docs/IMPLEMENTATION_GUIDE.md#part-3-nop-alerting-system) for alert logic & examples.

---


## 📊 Models Overview

### Staging Layer (Data Cleaning)
- `stg_trade_blotter_raw` — Type casting & validation
- `stg_trade_blotter_raw` — Type casting & validation

### Intermediate Layer (Business Logic)
- `int_confirmed_trades` — Status filter
- `int_daily_trade_flows` — Daily aggregates by currency
- `int_late_arriving_trade_impact` — >3 day latency flagging and late arrival quantification
- `int_trade_blotter_deduped` — Deduplication by trade_ref
- `int_nop_integrity_issues` — Day-over-day carry-forward checks

### Marts Layer (Reporting)
- `mart_computed_nop` — Deterministic NOP series (core model)
- `mart_nop_reconciliation` — Snapshot vs computed comparison
- `mart_reconciliation_summary` — Break classification & carry-forward chains

---

## 🏢 Production Deployment

For GCP deployment, see [IMPLEMENTATION_GUIDE - Production-Ready Architecture](docs/IMPLEMENTATION_GUIDE.md#part-4-production-ready-architecture) for:
- Pub/Sub streaming architecture
- BigQuery integration
- Cloud monitoring for alerting
- Micro-batching strategy (5 minutes)

---

## Future Work
### 1. Currency‑Specific Risk Thresholds
Current limits (+2M long, –1.5M short) are global and do not reflect the unique behavior of each currency. A more robust approach would involve:
- Percentile‑based thresholds per currency
- Volatility‑adjusted limits
- Dynamic thresholds that adapt to historical patterns
- This would reduce noise and make alerts more meaningful.


### 2. Add schema evolution handling
Automated validation with schema drift detection. This ensures reliability as the system scales.

### 3. Handle Public Holidays Properly
The `is_business_day` field uses Monday–Friday as the only non-trading day logic. In practice, the different countries observe country-specific public holidays. A better implementation would be to load a public_holidays table per country.

### 4. Lightweight Monitoring Dashboard
A simple dashboard to present the key findings and other business critical metrics

