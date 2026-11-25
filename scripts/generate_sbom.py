#!/usr/bin/env python3
"""Generate a CycloneDX 1.5 JSON Software Bill of Materials (SBOM) in results/security/sbom.json."""

from __future__ import annotations

import importlib.metadata as metadata
import json
import uuid
from datetime import UTC, datetime

from booking_ledger.config import REPO_ROOT


def main() -> int:
    components = []
    for dist in sorted(metadata.distributions(), key=lambda d: (d.metadata.get("Name") or "").lower()):
        name = dist.metadata.get("Name") or "unknown"
        version = dist.version
        components.append(
            {
                "type": "library",
                "name": name,
                "version": version,
                "purl": f"pkg:pypi/{name.lower()}@{version}",
            }
        )

    sbom = {
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "serialNumber": f"urn:uuid:{uuid.uuid4()}",
        "version": 1,
        "metadata": {
            "timestamp": datetime.now(UTC).isoformat(),
            "component": {
                "type": "application",
                "name": "global-booking-reconciliation-lakehouse",
                "version": "0.1.0",
                "licenses": [{"license": {"id": "Apache-2.0"}}],
            },
        },
        "components": components,
    }
    out_dir = REPO_ROOT / "results" / "security"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "sbom.json"
    out_path.write_text(json.dumps(sbom, indent=2), encoding="utf-8")
    print(f"Wrote CycloneDX SBOM with {len(components)} components to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
