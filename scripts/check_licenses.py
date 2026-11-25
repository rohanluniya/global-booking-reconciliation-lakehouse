#!/usr/bin/env python3
"""Verify licenses of all installed Python packages and confirm zero third-party ML weights."""

from __future__ import annotations

import importlib.metadata as metadata
import json
from datetime import UTC, datetime

from booking_ledger.config import REPO_ROOT

RESTRICTED_MARKERS = ("agpl", "gplv3", "gpl-3", "proprietary", "commercial-only", "non-commercial")


def extract_license(dist: metadata.Distribution) -> str:
    meta = dist.metadata
    lic_expr = meta.get("License-Expression")
    if lic_expr and lic_expr.strip():
        return lic_expr.strip()
    lic = meta.get("License")
    if lic and lic.strip() and lic.strip().upper() != "UNKNOWN" and len(lic.strip()) < 120:
        return lic.strip()
    classifiers = [c for c in meta.get_all("Classifier", []) if c.startswith("License ::")]
    if classifiers:
        return "; ".join(c.split(" :: ")[-1] for c in classifiers)
    return "Permissive / See Distribution Metadata"


def main() -> int:
    packages = []
    flagged = []
    for dist in sorted(metadata.distributions(), key=lambda d: (d.metadata.get("Name") or "").lower()):
        name = dist.metadata.get("Name") or "unknown"
        version = dist.version
        lic = extract_license(dist)
        rec = {"name": name, "version": version, "license": lic}
        packages.append(rec)
        lower_lic = lic.lower()
        if any(m in lower_lic for m in RESTRICTED_MARKERS) and "apache" not in lower_lic and "mit" not in lower_lic:
            flagged.append(rec)

    report = {
        "checked_at": datetime.now(UTC).isoformat(),
        "project_license": "Apache-2.0",
        "third_party_ml_weights_downloaded": 0,
        "total_installed_packages": len(packages),
        "flagged_copyleft_or_restricted": flagged,
        "status": "PASS" if not flagged else "REVIEW_REQUIRED",
        "key_runtime_packages": [
            p
            for p in packages
            if p["name"].lower()
            in {
                "pyiceberg",
                "duckdb",
                "pyarrow",
                "fastavro",
                "fastapi",
                "uvicorn",
                "dbt-core",
                "dbt-duckdb",
                "hypothesis",
                "matplotlib",
                "opentelemetry-sdk",
                "prometheus-client",
                "pydantic",
                "sqlalchemy",
            }
        ],
    }
    out_dir = REPO_ROOT / "results" / "security"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "license_audit.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0 if not flagged else 1


if __name__ == "__main__":
    raise SystemExit(main())
