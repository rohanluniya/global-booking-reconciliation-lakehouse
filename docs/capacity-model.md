# Capacity Model & Storage Layout

## Observed Single-Node Footprint (Measured on Apple M4 Pro, 48 GiB RAM)

Measured directly from `results/benchmarks/benchmark_results.json`:

| Profile | Bookings | Raw CDC Events | Ledger Lines | End-to-End Time (Iceberg V2 + DuckDB) | Peak Process RSS |
|---|---:|---:|---:|---:|---:|
| `smoke` | 150 | 1,175 | 1,909 | 402.04 ms | 198.94 MiB |
| `local` | 600 | 4,673 | 8,054 | 665.14 ms | 260.50 MiB |
| `stress` | 2,500 | 19,566 | 33,764 | 2,186.41 ms | 451.88 MiB |

Per-booking scaling ratios observed in the simulator:
- **CDC events per booking:** ~7.8 raw events (including duplicates and poison injections).
- **Double-entry journal lines per booking:** ~13.4 immutable lines (capture, fee accrual, modification reversals/adjustments, revenue recognition, bank clearing, host payout, refunds, FX revaluations).
- **Parquet storage efficiency:** ~110 bytes/row compressed in Iceberg V2 Parquet after compaction.

## Storage Layout, Partitioning & Retention Policy

| Layer | Partition Transform (Distributed Iceberg V2) | Compaction Cadence | Snapshot Expiration | Raw Data Retention |
|---|---|---|---|---|
| `bronze.*` | `days(recorded_at)` | Daily rewrite to 128 MiB target file size | 30 days | 7 years (immutable audit archive) |
| `silver.*` | `months(effective_at)` + Z-order by `booking_id` | Daily rewrite | 90 days | 7 years |
| `gold.ledger_journal_entries` | `accounting_period` (identity) + sort by `(booking_id, journal_entry_id)` | Daily + pre-close | Retain every month-end close tag indefinitely | 7 years (append-only) |
| `gold.reconciliation_*` | `accounting_period` | Daily rewrite | 90 days | 7 years |
| `gold.period_close_snapshots` | Unpartitioned (low row count, high audit value) | On close | Never expire | Indefinite |
| `bronze.quarantine_dlq` | `days(quarantined_at)` | Weekly | 90 days | 1 year after resolution |

## Distributed Scale Sizing Envelope (Design Estimate — Not Locally Measured)

At a target production load of **500,000 bookings/day** (~3.9M CDC events/day, ~6.7M ledger lines/day):
- **Kafka 4.3.2:** 12 partitions per topic (`cdc.booking_events`, `psp.settlements`), ~45 events/sec average, ~450 events/sec peak; 3-broker cluster with 100 GiB NVMe per broker (7-day retention).
- **Iceberg Parquet growth:** ~6.7M ledger lines/day * 110 B ≈ **0.74 GiB/day** (~270 GiB/year before bronze/silver), easily handled by S3/RustFS with daily Spark `rewrite_data_files`.
- **Compute:** 1 Spark driver + 4 executors (4 vCPU, 16 GiB RAM each) completes daily ledger + 3-way recon + compaction within a 15-minute SLA window.
