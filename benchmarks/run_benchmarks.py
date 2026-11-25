#!/usr/bin/env python3
"""Reproducible benchmark suite for Global Booking, Financial Ledger & Reconciliation Lakehouse.

Profiles:
  - smoke:  150 bookings
  - local:  600 bookings
  - stress: 2500 bookings

Also runs:
  - Exact Integer Minor-Unit vs IEEE-754 Float64 Drift Audit (50,000 multi-currency FX legs)
  - Apache Iceberg V2 Small-File Compaction Before/After Benchmark
"""

from __future__ import annotations

import csv
import json
import os
import platform
import random
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psutil

from booking_ledger.close_engine import close_accounting_period
from booking_ledger.config import REPO_ROOT, load_settings
from booking_ledger.iceberg_store import IcebergLakehouseStore
from booking_ledger.ledger import generate_double_entry_ledger, verify_ledger_zero_sum
from booking_ledger.money import audit_exact_int_vs_float64
from booking_ledger.pipeline import run_full_pipeline
from booking_ledger.reconciliation import run_three_way_reconciliation
from booking_ledger.simulator import generate_simulation_bundle
from booking_ledger.temporal import build_fx_rates_asof, reconstruct_booking_state_history

PROFILES = [
    ("smoke", 150, 20260501),
    ("local", 600, 20260505),
    ("stress", 2500, 20260509),
]


def measure_profile(name: str, num_bookings: int, seed: int) -> dict[str, Any]:
    proc = psutil.Process(os.getpid())
    with tempfile.TemporaryDirectory() as tmp:
        cfg = load_settings(Path(tmp))

        t_sim0 = time.perf_counter()
        sim = generate_simulation_bundle(seed=seed, num_bookings=num_bookings)
        sim_ms = (time.perf_counter() - t_sim0) * 1000.0

        # Stage-level latency breakdown (in-memory domain engines)
        valid_cdc = [e for e in sim.cdc_events if isinstance(e.get("gross_total_minor"), int) and e.get("listing_id") and e.get("event_type") != "UNKNOWN_STATE_XYZ"]
        t_tmp0 = time.perf_counter()
        hist = reconstruct_booking_state_history(valid_cdc)
        fx = build_fx_rates_asof(sim.fx_rates)
        temporal_ms = (time.perf_counter() - t_tmp0) * 1000.0

        t_led0 = time.perf_counter()
        ledger = generate_double_entry_ledger(hist, fx, sim.bank_statements)
        zero_sum = verify_ledger_zero_sum(ledger)
        ledger_ms = (time.perf_counter() - t_led0) * 1000.0

        t_rec0 = time.perf_counter()
        recon_rows, recon_excs, recon_summary = run_three_way_reconciliation(
            booking_history=hist,
            ledger_entries=ledger,
            raw_psp_settlements=sim.psp_settlements,
            bank_statements=sim.bank_statements,
        )
        recon_ms = (time.perf_counter() - t_rec0) * 1000.0

        t_cls0 = time.perf_counter()
        cert = close_accounting_period(
            period="2026-04",
            target_state="HARD_CLOSED",
            current_state="SOFT_CLOSE",
            ledger_entries=ledger,
            recon_summary=recon_summary,
            dq_pass_rate=1.0,
        )
        close_ms = (time.perf_counter() - t_cls0) * 1000.0

        # Full end-to-end durable broker + Avro + Iceberg V2 + DuckDB + DQ run
        t_e2e0 = time.perf_counter()
        e2e = run_full_pipeline(cfg, bundle=sim)
        e2e_ms = (time.perf_counter() - t_e2e0) * 1000.0

        peak_rss_mb = round(proc.memory_info().rss / (1024 * 1024), 2)
        ledger_lines = len(ledger)
        e2e_sec = max(e2e_ms / 1000.0, 1e-6)
        recon_sec = max(recon_ms / 1000.0, 1e-6)
        ledger_sec = max(ledger_ms / 1000.0, 1e-6)

        assert zero_sum["balanced"], f"Profile {name} failed zero-sum check!"
        assert e2e["dq_summary"]["all_passed"], f"Profile {name} failed DQ!"
        assert cert["trial_balance_imbalance_minor"] == 0

        return {
            "profile": name,
            "seed": seed,
            "bookings": num_bookings,
            "raw_cdc_events": len(sim.cdc_events),
            "ledger_lines": ledger_lines,
            "recon_transactions": len(recon_rows),
            "open_exceptions": len(recon_excs),
            "reconciliation_match_rate": recon_summary["match_rate"],
            "trial_balance_imbalance_minor": zero_sum["global_imbalance_minor"],
            "sim_ms": round(sim_ms, 2),
            "temporal_ms": round(temporal_ms, 2),
            "ledger_ms": round(ledger_ms, 2),
            "recon_ms": round(recon_ms, 2),
            "close_ms": round(close_ms, 2),
            "e2e_iceberg_duckdb_ms": round(e2e_ms, 2),
            "ledger_lines_per_sec": round(ledger_lines / ledger_sec, 1),
            "recon_tx_per_sec": round(len(recon_rows) / recon_sec, 1),
            "e2e_ledger_lines_per_sec": round(ledger_lines / e2e_sec, 1),
            "rss_mb": peak_rss_mb,
        }


def run_float_drift_benchmark(sample_count: int = 50_000) -> dict[str, Any]:
    rng = random.Random(20260515)
    currencies = [
        ("EUR", 1_084_375),
        ("GBP", 1_268_125),
        ("JPY", 6_543),
        ("CAD", 728_450),
        ("AUD", 652_175),
    ]
    samples: list[tuple[int, str, int]] = []
    for _ in range(sample_count):
        curr, base_fx = rng.choice(currencies)
        amt = rng.randint(10_001, 249_999)
        fx = base_fx + rng.randint(-350, 350)
        samples.append((amt, curr, fx))

    audit = audit_exact_int_vs_float64(samples, reporting_currency="USD")
    return {
        "sample_count": audit.sample_count,
        "exact_sum_usd_minor": audit.exact_sum_minor,
        "float64_sum_usd_minor": audit.float_sum_minor_rounded,
        "per_tx_mismatches": audit.per_tx_mismatches,
        "cumulative_drift_usd_minor": audit.cumulative_drift_minor,
        "max_single_tx_drift_usd_minor": audit.max_single_tx_drift_minor,
    }


def run_iceberg_compaction_benchmark() -> dict[str, Any]:
    """Create a intentionally fragmented Iceberg V2 ledger table (16 small appends) and compact to 1 file."""
    with tempfile.TemporaryDirectory() as tmp:
        cfg = load_settings(Path(tmp))
        store = IcebergLakehouseStore(cfg)
        sim = generate_simulation_bundle(seed=20260520, num_bookings=600)
        valid_cdc = [e for e in sim.cdc_events if isinstance(e.get("gross_total_minor"), int) and e.get("listing_id") and e.get("event_type") != "UNKNOWN_STATE_XYZ"]
        hist = reconstruct_booking_state_history(valid_cdc)
        fx = build_fx_rates_asof(sim.fx_rates)
        ledger = generate_double_entry_ledger(hist, fx, sim.bank_statements)

        # Simulate 16 streaming micro-batch appends (producing 16 small Parquet files)
        chunks = 16
        chunk_size = max(1, len(ledger) // chunks)
        store.overwrite_table("gold.ledger_journal_entries", ledger[:chunk_size])
        for i in range(1, chunks):
            start = i * chunk_size
            end = len(ledger) if i == chunks - 1 else (i + 1) * chunk_size
            store.append_table("gold.ledger_journal_entries", ledger[start:end])

        # Average 5 scans before compaction
        tbl = store.catalog.load_table("gold.ledger_journal_entries")
        files_before = len(list(tbl.scan().plan_files()))
        before_times: list[float] = []
        for _ in range(5):
            t0 = time.perf_counter()
            rows_b = tbl.scan().to_arrow().num_rows
            before_times.append((time.perf_counter() - t0) * 1000.0)

        compact_res = store.compact_table("gold.ledger_journal_entries")

        # Average 5 scans after compaction
        tbl_after = store.catalog.load_table("gold.ledger_journal_entries")
        files_after = len(list(tbl_after.scan().plan_files()))
        after_times: list[float] = []
        for _ in range(5):
            t0 = time.perf_counter()
            rows_a = tbl_after.scan().to_arrow().num_rows
            after_times.append((time.perf_counter() - t0) * 1000.0)

        assert rows_b == rows_a == len(ledger)
        mean_before = round(sum(before_times) / len(before_times), 2)
        mean_after = round(sum(after_times) / len(after_times), 2)
        speedup = round(mean_before / max(mean_after, 0.01), 2)

        return {
            "table": "gold.ledger_journal_entries",
            "row_count": len(ledger),
            "files_before": files_before,
            "files_after": files_after,
            "file_reduction_ratio": round(files_before / max(1, files_after), 1),
            "mean_scan_before_ms": mean_before,
            "mean_scan_after_ms": mean_after,
            "scan_speedup_x": speedup,
            "compaction_duration_ms": compact_res["compact_ms"],
        }


def main() -> int:
    profile_results = [measure_profile(name, n, seed) for name, n, seed in PROFILES]
    drift_audit = run_float_drift_benchmark(50_000)
    compaction_bench = run_iceberg_compaction_benchmark()

    hardware = {
        "os": f"{platform.system()} {platform.release()}",
        "machine": platform.machine(),
        "python_version": platform.python_version(),
        "cpu_count_logical": os.cpu_count(),
        "ram_gib": round(psutil.virtual_memory().total / (1024**3), 1),
    }

    report = {
        "executed_at": datetime.now(UTC).isoformat(),
        "hardware": hardware,
        "profiles": profile_results,
        "float_drift_audit": drift_audit,
        "iceberg_compaction": compaction_bench,
    }

    out_dir = REPO_ROOT / "results" / "benchmarks"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "benchmark_results.json").write_text(json.dumps(report, indent=2), encoding="utf-8")

    csv_path = out_dir / "benchmark_profiles.csv"
    fieldnames = list(profile_results[0].keys())
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(profile_results)

    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
