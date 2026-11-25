# Incident Report INC-001: Controlled Multi-Fault Financial Chaos Drill

## Summary

- **Incident Type:** Planned Chaos & Fault-Injection Game Day (`scripts/run_chaos_drills.py`)
- **Scope:** 7 injected failure modes spanning broker replay, out-of-order CDC streams, duplicate payment processor settlement files, late FX revisions, post-hard-close chargebacks, broker unavailability, and schema-invalid poison records.
- **Outcome:** **7/7 drills passed** (`0` ledger imbalances, `0` lost events, `0` closed-period mutations, `100%` poison isolation). Raw evidence persisted in `results/chaos/chaos_results.json`.

## Observed Drill Timeline & Recovery Behavior

| Drill ID | Failure Mode Injected | Observed System Behavior & Recovery | Verification Metric | Duration |
|---|---|---|---|---:|
| `CHAOS-01` | Full 2× duplicate CDC replay of entire booking batch | Deterministic SHA-256 `event_id` deduplication absorbed 100% of replayed messages | First run debits `28,443,645` == 2× replay debits `28,443,645` USD cents; imbalance `0` | 642.76 ms |
| `CHAOS-02` | Complete reversal of CDC arrival stream (cancellations arriving before confirmations) | Bi-temporal reconstruction ordered transitions strictly by `(sequence_num, effective_at, source_lsn)` | `314` out-of-order events yielded identical `312`-row state history | 8.59 ms |
| `CHAOS-03` | 2× re-delivery of entire PSP settlement batch under new `file_batch_id` suffix | Signature deduplicator stripped `75` duplicate settlement lines and classified bookings cleanly as `DUPLICATE_FILE_DEDUPED` | Open exposure unchanged at `320,998` USD cents | 20.30 ms |
| `CHAOS-04` | Mid-month central bank FX revision (`v2_revised`) published 2 days after capture | Historical `BOOKING_CAPTURE` entries stayed locked to `v1` FX; `2` balanced `FX_REVISION_ADJUSTMENT` legs posted to `FX_GAIN_LOSS` | `original_v1_captures_unmutated = true`, imbalance `0` | 0.42 ms |
| `CHAOS-05` | Late `$450.00` chargeback arriving after `2026-04` reached `HARD_CLOSED` | Close engine preserved `2026-04` at exactly `426` lines and routed `2` balanced `is_restatement = true` legs to open period `2026-05` | Closed period immutable; global imbalance `0` | 0.77 ms |
| `CHAOS-06` | Broker taken offline mid-ingestion after 5 messages committed | Publish raised `BrokerUnavailableError` cleanly without advancing consumer group checkpoint; post-recovery read resumed at offset 5 | `pre_outage=5`, `post_recovery=5`, `zero_event_loss = true` | 1.72 ms |
| `CHAOS-07` | Injected malformed CDC event with IEEE-754 float money (`123.45`) and unknown enum | FastAvro contract validator rejected poison record and routed to `bronze.quarantine_dlq` without interrupting valid traffic | `4` poison records quarantined, `233` valid CDC processed, DQ `100%` pass | 229.40 ms |

## Follow-Up Action Items

1. Keep `pytest -m chaos` wired into CI (`.github/workflows/ci.yml`) so future refactors to ledger or reconciliation logic cannot silently break idempotency or closed-period immutability.
2. Add automated alert-rule evaluation tests against `infra/observability/alerts.yml` when running the Docker `observability` profile on a container-enabled host.
