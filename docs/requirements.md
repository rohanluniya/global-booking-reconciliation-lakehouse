# System Requirements, SLOs & Consistency Contracts

## Functional Requirements

1. **Multi-Currency Booking Lifecycle & Outbox CDC:** Simulate holds, confirmations, modifications, payment retries, check-in/out, cancellations, partial/full refunds, host payouts, and chargebacks across 6 ISO-4217 currencies (`USD`, `EUR`, `GBP`, `JPY`, `CAD`, `AUD`) and 6 host timezones.
2. **Zero Floating-Point Drift:** Enforce exact integer minor units (`int` / `BIGINT`) and `Decimal` intermediate math across all monetary calculations and FX conversions.
3. **Bi-Temporal State & SCD Type 2 Dimensions:** Reconstruct booking state at any `(effective_at, recorded_at)` point in time, track host-local vs UTC accounting cutoff shifts, and maintain non-overlapping SCD Type 2 histories for listings and tax policies.
4. **Immutable Double-Entry Accounting Ledger:** Post balanced debit/credit entries across a 10-account Chart of Accounts. Never overwrite historical rows; represent modifications, refunds, late FX revisions, and post-close adjustments as immutable reversing/adjusting/restatement entries with `parent_entry_id` lineage.
5. **3-Way Financial Reconciliation & Exception Workflow:** Reconcile Internal Ledger, PSP Settlement Feeds, and Bank Statements deterministically into 8 classifications, deduplicate re-delivered settlement batches, age open exceptions (`0-1d`, `2-7d`, `8-30d`, `>30d`), and post balanced adjustment entries upon ticket resolution.
6. **Period Close & Restatement Audit:** Support `OPEN -> SOFT_CLOSE -> HARD_CLOSED`, lock trial balance snapshots in Apache Iceberg V2 with SHA-256 Sign-Off Certificates, gate `HARD_CLOSED` on 0 imbalance and 100% DQ pass, and route post-close events to the next open period as restatements.
7. **Serving, Governance & Observability:** Expose an RBAC-protected FastAPI control plane, interactive dark-mode Finance Control Tower UI, DuckDB/dbt marts, Prometheus metrics, OpenTelemetry spans, OpenLineage events, and PII-redacted JSON logs.

## Service-Level Objectives (SLOs)

| Dimension | Target SLO | Measurement Source |
|---|---|---|
| Double-Entry Zero-Sum Invariant | **0 minor units** imbalance per entry, currency, period, and globally | `finance_trial_balance_imbalance_minor` + dbt singular tests |
| Data Quality Gate | **100% pass rate** (12/12 assertions) before `HARD_CLOSE` | `quality.dq_results` + `finance_dq_pass_rate` |
| End-to-End Local Batch Latency (`600` bookings) | **< 5.0 seconds** (broker -> Iceberg V2 -> DuckDB -> DQ) | `benchmarks/run_benchmarks.py` |
| Control Plane API Latency (p95 read) | **< 250 ms** for `/v1/overview` and `/v1/ledger/trial-balance` | `finance_api_request_duration_seconds` |
| Poison Event Isolation | **100%** of schema-invalid CDC events quarantined without crashing pipeline | `bronze.quarantine_dlq` + `CHAOS-07` |
| Replay Determinism | **100%** identical trial balance across duplicate & out-of-order replays | Hypothesis property tests + `CHAOS-01`, `CHAOS-02` |

## Workload Assumptions

- **Local Demo Profile (`local`):** 600 bookings (~4,670 raw CDC events, ~8,050 ledger lines, 6 currencies, 42 listings) spanning two accounting months (`2026-04` and `2026-05`).
- **Stress Verification Profile (`stress`):** 2,500 bookings (~19,560 raw CDC events, ~33,760 ledger lines) run single-node on Apple Silicon to characterize linear scaling and memory footprint.
- **Target Production Scale (Distributed Profile):** 500,000 bookings/day (~4M CDC events/day, ~6.8M ledger legs/day) partitioned by `accounting_period` on Spark 4.1.4 + Iceberg V2 + Trino 484.
