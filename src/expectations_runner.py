"""Load config/expectations.yaml and validate a Spark DataFrame.

Uses plain PySpark only (no Great Expectations). GE's SparkDFDataset often calls
``cache``/persist paths that Databricks serverless rejects
(``[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE``).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pyspark.sql.functions import col

from paths import repo_root


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def validate_mart_dataframe(df, expectations_yaml: str | Path) -> None:
    """Run validations from ``expectations_yaml``; raise RuntimeError if any fail."""
    path = Path(expectations_yaml)
    if not path.is_absolute():
        path = repo_root() / path
    cfg = load_yaml(path)

    not_null = cfg.get("not_null_columns") or []
    unique = cfg.get("unique_columns") or []
    rc = cfg.get("row_count") or {}
    rmin, rmax = rc.get("min"), rc.get("max")
    has_row_bounds = rmin is not None or rmax is not None

    if not not_null and not unique and not has_row_bounds:
        return

    cols_present = set(df.columns)
    failures: list[str] = []

    for c in not_null:
        if c not in cols_present:
            failures.append(f"not_null: column {c!r} missing from DataFrame")
            continue
        if df.filter(col(c).isNull()).take(1):
            failures.append(f"not_null: column {c!r} has null values")

    for c in unique:
        if c not in cols_present:
            failures.append(f"unique: column {c!r} missing from DataFrame")
            continue
        if df.groupBy(col(c)).count().filter("count > 1").take(1):
            failures.append(f"unique: column {c!r} has duplicate values")

    if has_row_bounds:
        n = df.count()
        if rmin is not None and n < rmin:
            failures.append(f"row_count: {n} < min {rmin}")
        if rmax is not None and n > rmax:
            failures.append(f"row_count: {n} > max {rmax}")

    if failures:
        raise RuntimeError("Data quality expectations failed:\n" + "\n".join(failures[:20]))
