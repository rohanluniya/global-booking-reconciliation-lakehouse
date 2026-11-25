# Live Walkthrough & Demo Script

Follow this 5-minute end-to-end walkthrough to demonstrate every capability live on a fresh machine.

## 1. One-Command Portable Startup (Zero Docker Required)

```bash
bash scripts/run_portable.sh
```

Or run step-by-step via `Makefile`:

```bash
make setup
make pipeline     # Runs bronze/silver/gold Iceberg V2 + DuckDB + real dbt build (35/35 nodes)
make serve        # Starts Finance Control Plane & Dashboard at http://127.0.0.1:8040/
```

## 2. Inspect Live Financial Control Tower UI

Open `http://127.0.0.1:8040/` in a browser:
- Verify the **Trial Balance Proof** card reads `$0.00` imbalance (`Debits = Credits` across all 8,056+ immutable journal legs).
- Toggle the Trial Balance period filter (`ALL` -> `2026-04` -> `2026-05`) to confirm each period balances independently to zero.
- Click a booking link (e.g., `BKG-2026-00001`) in the Exceptions table to load its complete bi-temporal state history and double-entry legs.
- Click **Resolve & Adjust** on an open exception ticket (with Role = `Admin` or `Auditor`) to watch a balanced adjustment journal entry post live to Iceberg V2 and decrease open exception exposure.
- Switch Role to **Reader** and attempt to resolve an exception or trigger a restatement to demonstrate `403 Forbidden` RBAC enforcement.
- Switch back to **Admin** and click **Simulate Post-Close Restatement (2026-04 -> 2026-05)** to verify closed April remains immutable while balanced restatement legs land in open May.

## 3. CLI / cURL Verification Commands

```bash
# Public health & Prometheus metrics
curl -s http://127.0.0.1:8040/health | python3 -m json.tool
curl -s http://127.0.0.1:8040/metrics | grep finance_trial_balance_imbalance_minor

# Verify unauthorized access is rejected (HTTP 401)
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8040/v1/ledger/trial-balance

# Authenticated trial balance for HARD_CLOSED period 2026-04
curl -s -H "X-API-Key: dev-finance-reader-key-change-me" \
  "http://127.0.0.1:8040/v1/ledger/trial-balance?period=2026-04" | python3 -m json.tool

# Run 7 automated chaos drills & full benchmark suite
make chaos
make bench
make test
```
