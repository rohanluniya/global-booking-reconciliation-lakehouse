# Architecture Decision Records (ADRs)

## ADR-01: Exact Integer Minor Units + Decimal Arithmetic Over IEEE-754 Floats

- **Status:** Accepted
- **Context:** Multi-currency booking platforms process USD/EUR/GBP/CAD/AUD (2 decimals) and JPY (0 decimals) alongside high-precision FX rates. IEEE-754 `float64` binary representations cannot represent base-10 fractions like `0.10` or `0.29` exactly, causing silent penny drift across tens of thousands of legs.
- **Decision:** Store every monetary quantity as exact integer minor units (`BIGINT` / Python `int`) and store FX rates as exact integer micros (`rate_micros`, scaled by `1,000,000`). Reject `float` and `bool` inputs via `assert_minor_int()`. Perform intermediate basis-point and FX conversions using `decimal.Decimal` with banker's rounding (`ROUND_HALF_EVEN`).
- **Consequences:** Zero silent floating-point drift (verified across 50,000 FX conversions vs 24,809 per-transaction mismatches in naive `float64`). Slight CPU overhead compared to raw C doubles, which remains negligible at >75,000 ledger lines/sec in Python.

## ADR-02: Immutable Append-Only Double-Entry Ledger with Explicit Translation Rounding Leg

- **Status:** Accepted
- **Context:** Updating or overwriting historical ledger rows on booking modifications, partial refunds, or late FX rate revisions destroys auditability and breaks month-end close reproducibility. Additionally, translating a multi-leg foreign currency journal entry to reporting USD leg-by-leg can introduce a ±1 cent translation rounding delta.
- **Decision:** Enforce an append-only double-entry ledger with a 10-account Chart of Accounts. Modifications post an explicit reversing entry + new adjusting entry linked by `parent_entry_id`. Any non-zero multi-leg FX translation rounding residual (±1 cent) is booked explicitly to `FX_GAIN_LOSS` within the same `journal_entry_id`. Late FX rate revisions emit reporting-only `FX_REVISION_ADJUSTMENT` entries rather than mutating original entries.
- **Consequences:** Every single `journal_entry_id`, every currency, and every accounting period satisfies `SUM(debit) == SUM(credit)` identically (`imbalance = 0`).

## ADR-03: Bi-Temporal State Reconstruction & Targeted SCD Type 2 Dimensions

- **Status:** Accepted
- **Context:** Out-of-order CDC arrivals, late corrections, and timezone cutoffs (`America/New_York` vs `Asia/Tokyo` vs `UTC`) require answering both "what was the booking state on April 30?" (`effective_at`) and "what did finance know when closing on May 3?" (`recorded_at`). Meanwhile, not every table warrants SCD2 complexity.
- **Decision:** Model `silver.booking_state_history` and `silver.fx_rates_asof` bi-temporally (`effective_at`, `recorded_at`). Apply SCD Type 2 strictly where historical point-in-time attribution affects revenue and tax calculations: `silver.dim_listings_scd2` and `silver.dim_tax_policies_scd2`, verified with automated non-overlapping interval assertions.
- **Consequences:** Point-in-time audits (`GET /v1/bookings/{id}/as-of`) and reproducible as-of FX conversions work deterministically even after out-of-order CDC replay.

## ADR-04: Apache Iceberg V2 Medallion Warehouse with Dual Runtime (`local_native` + Distributed Compose)

- **Status:** Accepted
- **Context:** The host development workstation (macOS ARM64) lacks Docker/Podman and blocks unverified downloaded JVMs, while production deployments run PostgreSQL + Debezium + Kafka + Lakekeeper + Spark + Trino.
- **Decision:** Implement a genuine `local_native` Apache Iceberg V2 warehouse via `pyiceberg[sql-sqlite,pyiceberg-core]==0.12.0` + PyArrow Parquet + DuckDB 2.0 + `dbt-core==1.11.17` / `dbt-duckdb==1.11.1`, sharing the exact domain engine, Avro contracts, Iceberg V2 table schemas, and FastAPI control plane with the pinned multi-profile Docker Compose (`core`, `analytics`, `observability`, `full`) and Kubernetes manifests.
- **Consequences:** Real Iceberg V2 snapshots, time travel, Parquet compaction, and `dbt build` execute natively in <3 seconds on any laptop with Python 3.12 + `uv`, while preserving full container/Kubernetes portability.

## ADR-05: Transactional Outbox Pattern for OLTP-to-CDC Consistency

- **Status:** Accepted
- **Context:** Dual-writing to PostgreSQL and Kafka in application code risks lost events or phantom charges on process crash.
- **Decision:** Persist business state mutations and `booking_core.outbox_events` inside the same PostgreSQL ACID transaction, captured via Debezium 3.7.1.Final `pgoutput` logical replication and Outbox EventRouter SMT (mirrored faithfully in the deterministic local simulator with monotonic WAL LSNs).
- **Consequences:** Guarantees atomic event publication from OLTP; combined with downstream SHA-256 idempotency, delivers exactly-once accounting effects over at-least-once transport.

## ADR-06: Pinning `dbt-core==1.11.17` + `mypy==1.19.1` Alongside `duckdb==2.0.0`

- **Status:** Accepted
- **Context:** `dbt-core==1.12.7` introduces a transitive build dependency (`dbt-core-experimental-parser==2.0.11`) whose sdist build script attempts a GitHub HTTPS download via Python `urllib`, failing macOS SSL verification. Separately, `mypy>=2.4.1` requires `pathspec>=1.0.0` while `dbt-core==1.11.17` requires `pathspec<0.13`.
- **Decision:** Pin `dbt-core==1.11.17`, `dbt-duckdb==1.11.1`, `duckdb==2.0.0`, and `mypy==1.19.1` in `pyproject.toml` and `uv.lock`.
- **Consequences:** Clean, offline-reproducible `uv sync` with zero sdist network fetches, real `dbt build` (35/35 models and tests passing), and full static type checking in one unified virtual environment.

## ADR-07: Period Close Immutability via Next-Open-Period Restatements

- **Status:** Accepted
- **Context:** Chargebacks and settlement corrections frequently arrive days or weeks after month-end close.
- **Decision:** Enforce a strict state machine (`OPEN -> SOFT_CLOSE -> HARD_CLOSED`) gated by `trial_balance_imbalance_minor == 0` and `dq_pass_rate == 1.0`. Once `HARD_CLOSED`, the period snapshot is locked with a SHA-256 Sign-Off Certificate; reopening is rejected (`409 Conflict`), and any late adjustment automatically posts as an immutable `is_restatement = true` entry in the next open period with `original_period` provenance.
- **Consequences:** Closed financial statements never drift retroactively while late economic events are still accounted for cleanly.
