# PySpark boilerplate to execute SQL query and load into DataFrame
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Notebooks define `spark`; spark_python_task jobs do not — get the session explicitly.
spark = SparkSession.builder.getOrCreate()


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _load_ingest_config() -> dict[str, Any]:
    path = _repo_root() / "config" / "ingest.yaml"
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _apply_column_renames(df, renames: dict[str, str] | None, *, context: str):
    if not renames:
        return df
    out_df = df
    for old, new in renames.items():
        if old == new:
            continue
        if old not in out_df.columns:
            """
            It means your column of your table and the column mentioned here are not the same
            which should not happen.
            """
            raise ValueError(
                f"{context}: rename source column {old!r} not found; this should not happen; "
                f"columns  you have: {sorted(out_df.columns)}"
            )
        out_df = out_df.withColumnRenamed(old, new)
    return out_df


def _table_from_ingest_entry(entry: dict[str, Any], *, context: str):
    fqn = entry["fqn"]
    df = spark.table(fqn)
    return _apply_column_renames(df, entry.get("column_renames") or {}, context=context)


def _core_denormalized_through_suppliers(cfg: dict[str, Any] | None = None):
    """Fact + customer + franchise + supplier (one row per transaction)."""
    cfg = _load_ingest_config()
    sales_transactions_df = _table_from_ingest_entry(
        cfg["sales_transactions"],
        context="config/ingest.yaml sales_transactions",
    )
    sales_suppliers_df = _table_from_ingest_entry(
        cfg["sales_suppliers"],
        context="config/ingest.yaml sales_suppliers",
    )
    sales_customers_df = _table_from_ingest_entry(
        cfg["sales_customers"],
        context="config/ingest.yaml sales_customers",
    )
    sales_franchises_df = _table_from_ingest_entry(
        cfg["sales_franchises"],
        context="config/ingest.yaml sales_franchises",
    )

    temp_df1 = sales_transactions_df.join(
        sales_customers_df,
        sales_transactions_df.customerID == sales_customers_df.customerID,
        how="left",
    )
    temp_df2 = temp_df1.join(sales_franchises_df, on="franchiseID", how="left")
    return temp_df2.join(
        sales_suppliers_df,
        temp_df2.supplierID == sales_suppliers_df.supplierID,
        how="left",
    )


def build_mart_dataframe():
    """DataFrame suitable for MERGE on transactionID (no media joins — avoids row explosion)."""
    return _core_denormalized_through_suppliers()


def build_denormalized_dataframe(cfg: dict[str, Any] | None = None):
    """Wide denormalized DataFrame including review tables (may duplicate transactionID)."""
    denormalized_sales_transactions_df = _core_denormalized_through_suppliers(cfg)

    gold_cfg = cfg.get("media_gold_reviews_chunked") or {}
    media_gold_reviews_chunked_df = _table_from_ingest_entry(
        gold_cfg,
        context="config/ingest.yaml media_gold_reviews_chunked",
    )

    mcr_cfg = cfg.get("media_customer_reviews") or {}
    media_customer_reviews_df = _table_from_ingest_entry(
        mcr_cfg,
        context="config/ingest.yaml media_customer_reviews",
    )

    temp_df3 = denormalized_sales_transactions_df.join(
        media_gold_reviews_chunked_df,
        on="franchiseID",
        how="left",
    )
    return temp_df3.join(
        media_customer_reviews_df,
        on="franchiseID",
        how="left",
    )


def read_data():
    denormalized_df = build_denormalized_dataframe()
    denormalized_df.limit(10).show(truncate=False)

    filtered_df = denormalized_df.filter(col("franchise_name").isNotNull())
    count = filtered_df.count()
    print(f"Number of rows in the filtered frame: {count}")
    return count
