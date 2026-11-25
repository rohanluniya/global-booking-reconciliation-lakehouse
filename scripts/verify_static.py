#!/usr/bin/env python3
"""Static AST, JSON, YAML, zero-unpinned-tag, and hygiene verification script."""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path

import yaml

from booking_ledger.config import REPO_ROOT

SKIP_DIRS = {".venv", ".git", ".local_data", "__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache", ".hypothesis", "target", "logs"}
FORBIDDEN_TAG_RE = re.compile(":" + "lat" + "est\\b", re.IGNORECASE)
FORBIDDEN_PII_RE = re.compile(
    "|".join(["deshmukh" + "ankita", "google" + "plex", "roam\\." + "internal", "github_" + "pat_"]),
    re.IGNORECASE,
)


def should_skip(path: Path) -> bool:
    return any(part in SKIP_DIRS for part in path.parts)


def main() -> int:
    errors: list[str] = []
    checked = {"py": 0, "json": 0, "yaml": 0, "text_hygiene": 0}

    for path in sorted(REPO_ROOT.rglob("*")):
        if path.is_dir() or should_skip(path):
            continue
        rel = path.relative_to(REPO_ROOT)

        if path.suffix == ".py":
            try:
                ast.parse(path.read_text(encoding="utf-8"), filename=str(rel))
                checked["py"] += 1
            except SyntaxError as exc:
                errors.append(f"Python AST error in {rel}: {exc}")

        elif path.suffix in (".json", ".avsc"):
            try:
                json.loads(path.read_text(encoding="utf-8"))
                checked["json"] += 1
            except json.JSONDecodeError as exc:
                errors.append(f"JSON parse error in {rel}: {exc}")

        elif path.suffix in (".yml", ".yaml"):
            try:
                list(yaml.safe_load_all(path.read_text(encoding="utf-8")))
                checked["yaml"] += 1
            except yaml.YAMLError as exc:
                errors.append(f"YAML parse error in {rel}: {exc}")

        # Hygiene scan on text files (skip binary images & this script's own regex literal)
        if path.suffix not in (".png", ".jpg", ".pdf", ".sqlite", ".duckdb", ".parquet") and rel.as_posix() != "scripts/verify_static.py":
            try:
                content = path.read_text(encoding="utf-8")
                checked["text_hygiene"] += 1
                if FORBIDDEN_TAG_RE.search(content):
                    errors.append(f"Unpinned container tag found in {rel}")
                if FORBIDDEN_PII_RE.search(content):
                    errors.append(f"Sensitive host/token identifier found in {rel}")
            except UnicodeDecodeError:
                pass

    result = {"status": "PASS" if not errors else "FAIL", "checked": checked, "errors": errors}
    print(json.dumps(result, indent=2))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
