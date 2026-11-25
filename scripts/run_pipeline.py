#!/usr/bin/env python3
"""Run the end-to-end Global Booking, Financial Ledger & Reconciliation Lakehouse pipeline + dbt build."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

from booking_ledger.config import REPO_ROOT, load_settings
from booking_ledger.pipeline import export_run_artifacts, run_full_pipeline


def run_dbt_build(duckdb_path: Path, results_dbt_dir: Path) -> dict[str, object]:
    """Execute real `dbt build` (run + test) against the DuckDB mirror of Iceberg V2."""
    dbt_dir = REPO_ROOT / "dbt_project"
    results_dbt_dir.mkdir(parents=True, exist_ok=True)
    env = {
        **os.environ,
        "FINANCE_DUCKDB_PATH": str(duckdb_path.resolve()),
        "DBT_PROFILES_DIR": str(dbt_dir.resolve()),
    }
    dbt_bin = Path(sys.executable).parent / "dbt"
    cmd = [str(dbt_bin), "build", "--profiles-dir", str(dbt_dir), "--project-dir", str(dbt_dir)]
    proc = subprocess.run(cmd, cwd=str(dbt_dir), env=env, capture_output=True, text=True, check=False)

    import re

    ansi_re = re.compile(r"\x1b\[[0-9;]*m")
    clean_out = [ansi_re.sub("", ln) for ln in proc.stdout.splitlines()]
    clean_err = [ansi_re.sub("", ln) for ln in proc.stderr.splitlines()]
    run_results_path = dbt_dir / "target" / "run_results.json"
    summary: dict[str, object] = {
        "returncode": proc.returncode,
        "stdout_tail": clean_out[-25:],
        "stderr_tail": clean_err[-15:],
        "results": [],
    }
    if run_results_path.exists():
        raw = json.loads(run_results_path.read_text(encoding="utf-8"))
        nodes = [
            {
                "unique_id": r.get("unique_id"),
                "status": r.get("status"),
                "execution_time_s": round(float(r.get("execution_time", 0.0)), 4),
                "failures": r.get("failures"),
            }
            for r in raw.get("results", [])
        ]
        summary["results"] = nodes
        summary["total_nodes"] = len(nodes)
        summary["passed_nodes"] = sum(1 for n in nodes if n["status"] in ("success", "pass"))
        summary["failed_nodes"] = sum(1 for n in nodes if n["status"] not in ("success", "pass"))

    (results_dbt_dir / "dbt_build_summary.json").write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Global Booking & Reconciliation Lakehouse pipeline")
    parser.add_argument("--seed", type=int, default=None, help="Deterministic seed override")
    parser.add_argument("--bookings", type=int, default=None, help="Number of simulated bookings")
    parser.add_argument("--skip-dbt", action="store_true", help="Skip dbt build step")
    parser.add_argument("--export-results", action="store_true", help="Export DQ/lineage/dbt artifacts to results/")
    args = parser.parse_args()

    settings = load_settings()
    summary = run_full_pipeline(settings, seed=args.seed, num_bookings=args.bookings)
    results_dir = REPO_ROOT / "results"
    if args.export_results:
        export_run_artifacts(settings, summary, results_dir)

    dbt_summary: dict[str, object] | None = None
    if not args.skip_dbt:
        dbt_summary = run_dbt_build(settings.duckdb_path, results_dir / "dbt")

    output = {
        "pipeline_run_id": summary["run_id"],
        "elapsed_ms": summary["elapsed_ms"],
        "bookings": summary["num_bookings"],
        "ledger_lines": summary["counts"]["ledger_journal_lines"],
        "trial_balance_imbalance_minor": summary["ledger_verification"]["global_imbalance_minor"],
        "reconciliation_match_rate": summary["reconciliation_summary"]["match_rate"],
        "open_exceptions": summary["reconciliation_summary"]["open_exception_count"],
        "dq_pass_rate": summary["dq_summary"]["pass_rate"],
        "quarantined_dlq": summary["counts"]["quarantined_dlq_events"],
        "close_periods": summary["close_periods"],
        "dbt_passed_nodes": dbt_summary.get("passed_nodes") if dbt_summary else None,
        "dbt_total_nodes": dbt_summary.get("total_nodes") if dbt_summary else None,
        "dbt_returncode": dbt_summary.get("returncode") if dbt_summary else None,
    }
    print(json.dumps(output, indent=2))
    if not summary["ledger_verification"]["balanced"] or not summary["dq_summary"]["all_passed"]:
        return 1
    if dbt_summary and dbt_summary.get("returncode") != 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
