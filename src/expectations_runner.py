"""Load config/expectations.yaml and validate a Spark DataFrame with Great Expectations."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def load_yaml(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def validate_mart_dataframe(df, expectations_yaml: str | Path) -> None:
    """Run GE validations; raise RuntimeError if any expectation fails.

    Uses Great Expectations 0.18.x ``SparkDFDataset`` (stable on PyPI). There is
    no stable ``1.0`` release yet; ``>=1.0`` only matches pre-releases with ``pip --pre``.
    """
    path = Path(expectations_yaml)
    if not path.is_absolute():
        path = _repo_root() / path
    cfg = load_yaml(path)

    not_null = cfg.get("not_null_columns") or []
    unique = cfg.get("unique_columns") or []
    rc = cfg.get("row_count") or {}
    rmin, rmax = rc.get("min"), rc.get("max")
    has_row_bounds = rmin is not None or rmax is not None

    if not not_null and not unique and not has_row_bounds:
        return

    from great_expectations.dataset import SparkDFDataset

    ge_df = SparkDFDataset(df)
    failures: list[str] = []
    for col in not_null:
        result = ge_df.expect_column_values_to_not_be_null(col)
        print(f'Not null validation result : {result}')
        if not result.get(success, True):
            failures.append(str(result))

    for col in unique:
        result = ge_df.expect_column_values_to_be_unique(col)
        print(f'Unique validation result : {result}')
        if not result.get("success", True):
            failures.append(str(result))

    if has_row_bounds:
        result = ge_df.expect_table_row_count_to_be_between(
            min_value=rmin if rmin is not None else None,
            max_value=rmax if rmax is not None else None,
        )
        print(f'Row bound result : {result}')
        if not result.get("success", True):
            failures.append(str(result))

    if failures:
        raise RuntimeError(
            "Data quality expectations failed:\n" + "\n".join(failures[:20])
        )
