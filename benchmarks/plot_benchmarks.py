#!/usr/bin/env python3
"""Generate clean Matplotlib benchmark summary chart from results/benchmarks/benchmark_results.json."""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from booking_ledger.config import REPO_ROOT


def main() -> int:
    src = REPO_ROOT / "results" / "benchmarks" / "benchmark_results.json"
    if not src.exists():
        raise FileNotFoundError(f"Run benchmarks/run_benchmarks.py first; missing {src}")
    data = json.loads(src.read_text(encoding="utf-8"))
    profiles = data["profiles"]
    comp = data["iceberg_compaction"]
    drift = data["float_drift_audit"]

    plt.rcParams.update(
        {
            "figure.facecolor": "#0b1120",
            "axes.facecolor": "#111827",
            "axes.edgecolor": "#334155",
            "axes.labelcolor": "#e2e8f0",
            "xtick.color": "#cbd5e1",
            "ytick.color": "#cbd5e1",
            "text.color": "#f8fafc",
            "grid.color": "#1e293b",
            "font.size": 10,
        }
    )

    fig, axes = plt.subplots(1, 3, figsize=(15.5, 5.0), dpi=160)
    fig.suptitle(
        "Global Booking & Financial Reconciliation Lakehouse — Measured Benchmark Suite",
        fontsize=13,
        fontweight="bold",
        y=0.98,
    )

    # Panel 1: End-to-End & Stage Latency across smoke / local / stress
    names = [f"{p['profile']}\n({p['bookings']} bkgs)" for p in profiles]
    e2e_ms = [p["e2e_iceberg_duckdb_ms"] for p in profiles]
    ledger_ms = [p["ledger_ms"] for p in profiles]
    recon_ms = [p["recon_ms"] for p in profiles]

    x = list(range(len(names)))
    w = 0.25
    b1 = axes[0].bar([i - w for i in x], ledger_ms, width=w, color="#38bdf8", label="Double-Entry Ledger")
    b2 = axes[0].bar(x, recon_ms, width=w, color="#10b981", label="3-Way Recon")
    b3 = axes[0].bar([i + w for i in x], e2e_ms, width=w, color="#818cf8", label="Full E2E Iceberg+DuckDB")
    axes[0].set_title("Stage & Full Medallion Latency (ms)", fontsize=11, pad=10)
    axes[0].set_xticks(x)
    axes[0].set_xticklabels(names)
    axes[0].set_ylabel("Latency (ms)")
    axes[0].set_ylim(0, max(e2e_ms) * 1.25)
    axes[0].grid(axis="y", linestyle="--", alpha=0.5)
    axes[0].legend(frameon=False, fontsize=8.5, loc="upper left")
    for bars in (b1, b2, b3):
        for bar in bars:
            h = bar.get_height()
            axes[0].annotate(
                f"{h:.0f}",
                xy=(bar.get_x() + bar.get_width() / 2, h),
                xytext=(0, 3),
                textcoords="offset points",
                ha="center",
                va="bottom",
                fontsize=7.5,
                color="#e2e8f0",
            )

    # Panel 2: Ledger & 3-Way Recon Throughput
    ledger_tps = [p["ledger_lines_per_sec"] / 1000.0 for p in profiles]
    recon_tps = [p["recon_tx_per_sec"] / 1000.0 for p in profiles]
    t1 = axes[1].bar([i - 0.17 for i in x], ledger_tps, width=0.34, color="#14b8a6", label="Ledger Lines / sec (k)")
    t2 = axes[1].bar([i + 0.17 for i in x], recon_tps, width=0.34, color="#f59e0b", label="3-Way Recon Tx / sec (k)")
    axes[1].set_title("Compute Throughput (thousands / sec)", fontsize=11, pad=10)
    axes[1].set_xticks(x)
    axes[1].set_xticklabels(names)
    axes[1].set_ylabel("Thousands / sec")
    axes[1].set_ylim(0, max(recon_tps) * 1.35)
    axes[1].grid(axis="y", linestyle="--", alpha=0.5)
    axes[1].legend(frameon=False, fontsize=8.5, loc="upper right")
    for bars in (t1, t2):
        for bar in bars:
            h = bar.get_height()
            axes[1].annotate(
                f"{h:.1f}k",
                xy=(bar.get_x() + bar.get_width() / 2, h),
                xytext=(0, 3),
                textcoords="offset points",
                ha="center",
                va="bottom",
                fontsize=8,
                color="#e2e8f0",
            )

    # Panel 3: Iceberg V2 Compaction Before/After + Float64 Drift Callout
    labels_c = [
        f"Before Compaction\n({comp['files_before']} Parquet files)",
        f"After Compaction\n({comp['files_after']} Parquet file)",
    ]
    vals_c = [comp["mean_scan_before_ms"], comp["mean_scan_after_ms"]]
    bars_c = axes[2].bar([0, 1], vals_c, width=0.38, color=["#f43f5e", "#10b981"])
    axes[2].set_title(
        f"Iceberg V2 Scan Latency ({comp['scan_speedup_x']}x Faster)",
        fontsize=11,
        pad=10,
    )
    axes[2].set_xticks([0, 1])
    axes[2].set_xticklabels(labels_c)
    axes[2].set_ylabel("Mean Full Table Scan (ms)")
    axes[2].set_ylim(0, max(vals_c) * 1.25)
    axes[2].grid(axis="y", linestyle="--", alpha=0.5)
    for bar in bars_c:
        h = bar.get_height()
        axes[2].annotate(
            f"{h:.2f} ms",
            xy=(bar.get_x() + bar.get_width() / 2, h),
            xytext=(0, 3),
            textcoords="offset points",
            ha="center",
            va="bottom",
            fontsize=8.5,
            color="#f8fafc",
            fontweight="bold",
        )
    axes[2].text(
        0.72,
        0.68,
        f"50k FX Float64 Audit\n"
        f"Int Drift: 0 cents\n"
        f"Float64 Drift: {drift['cumulative_drift_usd_minor']} cents\n"
        f"({drift['per_tx_mismatches']:,} tx mismatches)",
        transform=axes[2].transAxes,
        ha="center",
        va="center",
        fontsize=8.0,
        bbox={"boxstyle": "round,pad=0.45", "facecolor": "#1e293b", "edgecolor": "#38bdf8", "alpha": 0.94},
    )

    plt.tight_layout(rect=(0, 0.02, 1, 0.93))
    out_path = Path(REPO_ROOT) / "docs" / "assets" / "benchmark_finance_reconciliation.png"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=160)
    plt.close(fig)
    print(f"Saved benchmark plot to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
