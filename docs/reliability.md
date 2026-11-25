# Reliability, Alerting & Operator Runbooks

Every Prometheus alert rule in `infra/observability/alerts.yml` links directly to an operator runbook anchor below.

## Observability Signals

- **Metrics (`/metrics`):** `finance_trial_balance_imbalance_minor`, `finance_dq_pass_rate`, `finance_reconciliation_match_rate`, `finance_open_exceptions_count`, `finance_open_exception_exposure_usd_minor`, `finance_dlq_events_total`, `finance_ledger_entries_total`, `finance_api_requests_total`, `finance_api_request_duration_seconds`.
- **Traces (`.local_data/traces/spans.jsonl`):** OpenTelemetry spans covering `finance_medallion_pipeline`, `silver_temporal_and_scd2`, `gold_double_entry_ledger`, `gold_three_way_reconciliation`, and `iceberg_v2_commit_and_duckdb_sync`.
- **Logs (stdout JSON):** Structured logs containing `trace_id`, `run_id`, and `correlation_id` with recursive PII/credential redaction.
- **Lineage (`.local_data/lineage/openlineage_events.jsonl`):** OpenLineage v2.0.2 `RunEvent` records consumable by Marquez `0.51.1`.

---

## Runbook: Trial Balance Imbalance {#runbook-trial-balance-imbalance}

- **Alert:** `TrialBalanceImbalanceDetected` (`finance_trial_balance_imbalance_minor > 0`)
- **Severity:** Critical — automatically halts period close.
- **Immediate Triage:**
  1. Query `/v1/ledger/trial-balance` and inspect which `accounting_period` has non-zero `imbalance_minor`.
  2. Run the singular dbt assertion `assert_no_orphan_journal_entries` to isolate the exact offending `journal_entry_id`:
     ```bash
     uv run dbt test --select assert_no_orphan_journal_entries --project-dir dbt_project --profiles-dir dbt_project
     ```
  3. Inspect the offending booking via `GET /v1/ledger/booking/{booking_id}` and compare original vs reporting minor units.
- **Remediation:** Never delete or mutate committed Iceberg rows. Roll back the active Iceberg snapshot pointer if a bad batch write bypassed `build_balanced_journal_entry`, fix the posting rule, and replay idempotently from `bronze.cdc_booking_events`.

---

## Runbook: Data Quality Gate Failure {#runbook-dq-gate-failure}

- **Alert:** `DataQualityGateFailure` (`finance_dq_pass_rate < 1.0`)
- **Severity:** Critical — blocks `HARD_CLOSED` period lock.
- **Immediate Triage:**
  1. Call `GET /v1/quality/report` or inspect `results/dq/dq_report.json` to identify which of the 12 invariants failed (`scd2_listings_non_overlapping`, `booking_history_monotonic_sequence`, `fx_rates_positive_micros`, etc.).
  2. Check recent Iceberg snapshots via `GET /v1/iceberg/tables`.
- **Remediation:** Correct upstream dimension/FX source records, re-run `uv run python scripts/run_pipeline.py --export-results`, confirm `dq_pass_rate == 1.0`, and proceed with period close.

---

## Runbook: Reconciliation Match Rate Degraded {#runbook-reconciliation-degraded}

- **Alert:** `ReconciliationMatchRateDegraded` (`finance_reconciliation_match_rate < 0.80`)
- **Severity:** Warning.
- **Immediate Triage:**
  1. Call `GET /v1/reconciliation/summary` to inspect `counts_by_classification`.
  2. If `TIMING_IN_TRANSIT` spiked, check whether the daily Treasury Bank statement (`bank.statements`) arrived late.
  3. If `FEE_VARIANCE` or `FX_VARIANCE` spiked, check whether the PSP changed its basis-point fee tier or applied an unscheduled spread.
- **Remediation:** Ingest the delayed bank settlement feed and re-run reconciliation; for verified fee/FX changes, update policy parameters and resolve open exception tickets via `POST /v1/reconciliation/exceptions/{id}/resolve`.

---

## Runbook: High Open Exception Exposure {#runbook-open-exception-exposure}

- **Alert:** `HighOpenExceptionExposure` (`finance_open_exception_exposure_usd_minor > 5000000`)
- **Severity:** Warning.
- **Immediate Triage:**
  1. Query `GET /v1/reconciliation/exceptions?status=OPEN` (results are sorted descending by `variance_usd_minor`).
  2. Prioritize `HIGH` severity items (`UNMATCHED_PSP_OR_BANK`, `PARTIAL_REFUND_MISMATCH`, `CHARGEBACK_OPEN`) and items in `8-30d` / `>30d` aging buckets.
- **Remediation:** Transition tickets from `OPEN` -> `INVESTIGATING` -> `RESOLVED_ADJUSTED` with documented root cause; verify the auto-posted adjustment `journal_entry_id` maintains zero trial balance imbalance.

---

## Runbook: Poison DLQ Spike {#runbook-poison-dlq-spike}

- **Alert:** `PoisonDlqSpike` (`increase(finance_dlq_events_total[15m]) > 25`)
- **Severity:** Warning.
- **Immediate Triage:**
  1. Call `GET /v1/quality/report` and inspect `dlq_sample` (`bronze.quarantine_dlq`).
  2. Identify whether an upstream producer deployed an incompatible schema change (e.g., sending IEEE-754 floats instead of integer minor units or an unregistered enum symbol).
- **Remediation:** Coordinate a backward-compatible Avro schema bump (following the v1 -> v2 contract pattern in `schemas/avro/booking_cdc_event.v2.avsc`), then replay fixed records from the quarantine archive.
