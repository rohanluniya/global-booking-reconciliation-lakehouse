# Engineering Case Study: Designing an Audit-Grade Multi-Currency Booking & Reconciliation Lakehouse

## Problem Statement

Global hospitality marketplaces sit at the intersection of asynchronous booking state machines, multi-currency payment processors, regional tax jurisdictions, and bank treasury settlements. Naive analytics pipelines fail finance teams in five predictable ways:

1. **Floating-point rounding drift** when converting EUR, GBP, JPY, CAD, and AUD transactions to USD using binary `float64`.
2. **Retroactive mutation of closed books** when late modifications, refunds, chargebacks, or central bank FX revisions overwrite historical rows in place.
3. **Out-of-order and duplicate CDC delivery** from transactional databases causing double-counted revenue or corrupted booking statuses.
4. **Opaque settlement gaps** where internal booking records, PSP settlement files, and treasury bank statements disagree without structured exception triage.
5. **Timezone cutoff ambiguity** where a late-night New York check-in (`2026-04-30T22:30-04:00`) falls in April locally but May in UTC.

## Key Architectural Solutions Built

- **Exact Integer Minor-Unit Core (`booking_ledger/money.py`):** Every monetary field is enforced at runtime and schema level as exact integer minor units (`BIGINT` / Python `int`), with FX rates stored as exact integer micros (`1,000,000` scale). On a 50,000-transaction multi-currency FX audit, this eliminated 24,809 per-transaction rounding mismatches and $0.71 of silent cumulative drift observed in naive `float64`.
- **Immutable Double-Entry Accounting (`booking_ledger/ledger.py`):** A 10-account Chart of Accounts enforces `SUM(debit) == SUM(credit)` per journal entry, per currency, per period, and globally. Multi-leg foreign-currency translation rounding residuals (±1 cent) are explicitly booked to `FX_GAIN_LOSS` rather than dropped. Modifications emit linked reversing + adjusting entries; late FX revisions emit reporting-only revaluation legs; post-hard-close chargebacks route as restatements into the next open period.
- **Bi-Temporal Reconstruction & Targeted SCD2 (`booking_ledger/temporal.py`):** Separating business time (`effective_at`) from system time (`recorded_at`) allows reconstructing exact booking states and as-of FX rates even after completely reversed CDC streams (`CHAOS-02`). SCD Type 2 is applied selectively to listings and tax policies with automated zero-overlap checks.
- **3-Way Reconciliation Engine (`booking_ledger/reconciliation.py`):** Matches Internal Ledger, PSP Settlements, and Bank Statements deterministically into 8 statuses, deduplicates re-delivered PSP batches, ages open exceptions (`0-1d`, `2-7d`, `8-30d`, `>30d`), and posts balanced adjustment journal entries upon ticket resolution.
- **Apache Iceberg V2 + DuckDB + dbt Dual Runtime:** All 13 bronze/silver/gold/quality tables persist as genuine Apache Iceberg V2 Parquet snapshots with time travel and compaction (`8.51×` faster scans after compacting 16 small files to 1). Real `dbt build` validates 8 SQL models and 27 schema/singular zero-sum tests in <0.5 seconds against DuckDB.

## Measured Outcomes

- **Correctness:** `0` minor-unit trial balance imbalance across all load profiles, 80+ randomized Hypothesis property cases, and 7/7 chaos drills.
- **Data Quality & dbt:** `12/12` (100%) lakehouse DQ invariants and `35/35` dbt build nodes passing.
- **Performance:** 600-booking local batch completes full broker -> Iceberg V2 -> DuckDB -> DQ in **665.14 ms**; 2,500-booking stress batch (33,764 ledger lines) completes in **2,186.41 ms** at **76,542 ledger lines/sec** and **114,696 recon checks/sec**.
