# Fincra NOP Reconciliation System

**Quick-Start Guide**

---

## 📋 Overview

This project implements a comprehensive **Net Open Position (NOP)** computation and reconciliation system for Fincra, including:

- ✅ Data cleaning & deduplication of trade records
- ✅ Deterministic NOP computation by currency and day
- ✅ Reconciliation against official snapshots with break classification
- ✅ Late-arrival trade impact analysis
- ✅ Real-time alerting system (position limits, breaks, stale positions)
- ✅ Production-ready architecture for Google Cloud Platform

---

## 📂 Quick Navigation

### For Detailed Implementation Information
👉 **See [docs/IMPLEMENTATION_GUIDE.md](docs/IMPLEMENTATION_GUIDE.md)** for comprehensive documentation including:
- Detailed data quality issues and resolutions
- Complete NOP computation logic and formulas
- Reconciliation architecture and break classification
- Real-time alerting system design (3 alert types)
- Production deployment architecture
- Operational runbooks and SLA targets

### Key Sections in Implementation Guide:
1. **[System Architecture Overview](docs/IMPLEMENTATION_GUIDE.md#system-architecture-overview)** — High-level data flow
2. **[Data Cleaning & Ingestion](docs/IMPLEMENTATION_GUIDE.md#part-1--data-cleaning--ingestion)** — 6 data quality issues + fixes
3. **[NOP Computation & Reconciliation](docs/IMPLEMENTATION_GUIDE.md#part-2--nop-computation--reconciliation)** — Core logic & models
4. **[Real-Time Alerting System](docs/IMPLEMENTATION_GUIDE.md#part-3--real-time-alerting-system)** — 3 alert types with examples
5. **[Production Architecture](docs/IMPLEMENTATION_GUIDE.md#part-4--production-ready-architecture)** — GCP deployment guide
6. **[Data Quality Issues](docs/IMPLEMENTATION_GUIDE.md#data-quality-issues--resolution)** — Complete summary table
7. **[Key Findings & Insights](docs/IMPLEMENTATION_GUIDE.md#key-findings--insights)** — Stale positions, breaks, late arrivals
8. **[Deployment & Operations](docs/IMPLEMENTATION_GUIDE.md#deployment--operations)** — Runbooks & monitoring

---

## 🚀 Getting Started

### Prerequisites
- Python 3.9+
- dbt 1.0+
- DuckDB (development) or BigQuery (production)

### Quick Setup

```bash
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

## 📂 Project Structure

```
fincra_nop_reconcilliation/
├── README.md                     # This file (quick reference)
├── docs/
│   └── IMPLEMENTATION_GUIDE.md   # ⭐ Comprehensive documentation
├── dbt_project.yml
├── requirements.txt
│
├── models/
│   ├── staging/                  # Data cleaning & normalization
│   ├── intermediate/             # NOP computation business logic
│   └── marts/                    # Final reporting & reconciliation
│
├── seeds/                        # Source data (CSV)
│   ├── trade_blotter.csv
│   └── daily_nop_snapshot.csv
│
├── scripts/
│   ├── alert_system.py           # Real-time alerting
│
└── target/                       # Generated artifacts
```

---

## 🎯 Core Concepts

### NOP Fundamental Equation
```
Closing NOP = Opening NOP + Net Purchases - Net Sales
```

### Key Assumptions
| Assumption | Rationale |
|-----------|-----------|
| Trade blotter = source of truth | Snapshot authoritative for reporting but trades determine correct NOP |
| Latest `ingested_at` wins | Handles duplicate records from retries/corrections |
| NOP by `trade_date` | Late arrivals applied retroactively via batch recomputation |
| Non-trading days carry forward | `opening = closing = previous business day closing` |

### Break Classification
- **opening_only** — Opening divergence only
- **flows_only** — Flow divergence only
- **opening_and_flows** — Both diverge
- **match** — Perfect reconciliation

---

## ⚠️ Data Quality Issues Fixed

| Issue | Root Cause | Resolution |
|-------|-----------|-----------|
| Numeric overflow | Integer range exceeded | Cast to `DECIMAL(18,2)` |
| Duplicate trades | Retries/corrections | Deduplicate by latest `ingested_at` |
| NULL status | Data entry bugs | Filter NULL, flag for monitoring |
| Snapshot arithmetic | Manual adjustments | Reconcile & surface breaks |
| Carry-forward breaks | Manual position adjustments | Alert operations team |
| Rounding noise | Floating-point precision | Use DECIMAL types |

See [docs/IMPLEMENTATION_GUIDE.md#part-1--data-cleaning--ingestion](docs/IMPLEMENTATION_GUIDE.md#part-1--data-cleaning--ingestion) for detailed analysis.

---

## 🚨 Real-Time Alerts

The system generates 3 alert types:

1. **Position Limit Breach** — Long > +$2M or Short < -$1.5M
2. **Carry-Forward Break** — `opening[N] != closing[N-1]`
3. **Stale Position** — 5+ consecutive business days with zero trades

See [docs/IMPLEMENTATION_GUIDE.md#part-3--real-time-alerting-system](docs/IMPLEMENTATION_GUIDE.md#part-3--real-time-alerting-system) for alert logic & examples.

---

## 🔍 Key Findings

- **Stale Positions:** TZS & ZMW carry multi-million USD short positions unchanged for weeks (hedging signals)
- **Late Arrivals:** ~2% of trades arrive >3 days late, affecting 8–12% of reporting days
- **Break Patterns:** USD shows 8-day carry-forward chains; EUR sporadic 1-day blips

See [docs/IMPLEMENTATION_GUIDE.md#key-findings--insights](docs/IMPLEMENTATION_GUIDE.md#key-findings--insights) for details.

---

## 📊 Models Overview

### Staging Layer (Data Cleaning)
- `stg_trade_blotter_raw` — Type casting & validation
- `stg_trade_blotter_deduped` — Deduplication by trade_ref

### Intermediate Layer (Business Logic)
- `int_confirmed_trades` — Status filter
- `int_daily_trade_flows` — Daily aggregates by currency
- `int_late_arriving_trades` — >3 day latency flagging
- `int_calendar_currency_matrix` — All (date, currency) pairs

### Marts Layer (Reporting)
- `mart_computed_nop` — Deterministic NOP series (core model)
- `mart_nop_reconciliation` — Snapshot vs computed comparison
- `mart_nop_integrity_issues` — Day-over-day carry-forward checks
- `mart_late_arrival_impact` — Late arrival quantification
- `mart_reconciliation_summary` — Break classification & carry-forward chains

---

## 🏢 Production Deployment

For GCP deployment, see [docs/IMPLEMENTATION_GUIDE.md#part-4--production-ready-architecture](docs/IMPLEMENTATION_GUIDE.md#part-4--production-ready-architecture) for:
- Dataflow + Pub/Sub streaming architecture
- BigQuery integration
- Cloud Run alerting deployment
- Micro-batching strategy (1–5 minutes)

---

## 📝 Operational Runbooks

Quick guides available in [docs/IMPLEMENTATION_GUIDE.md#deployment--operations](docs/IMPLEMENTATION_GUIDE.md#deployment--operations):
- **Investigating new breaks** — Step-by-step SQL queries
- **Late arrival investigation** — Impact analysis workflow
- **SLA targets & monitoring** — Performance baselines

---

## 📞 Support

- **Implementation Details:** See [docs/IMPLEMENTATION_GUIDE.md](docs/IMPLEMENTATION_GUIDE.md)
- **Model-Specific Logic:** Check inline SQL comments in `models/` folder
- **Deployment Questions:** Refer to [Deployment & Operations](docs/IMPLEMENTATION_GUIDE.md#deployment--operations) section

