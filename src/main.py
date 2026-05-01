"""Entry point for Databricks Jobs (spark_python_task): quality checks + Delta MERGE."""

from __future__ import annotations
from pathlib import Path
import yaml
from pyspark.sql import SparkSession
from expectations_runner import validate_mart_dataframe
from ingest_data import build_mart_dataframe
from mart_loader import ensure_table, merge_into_delta_table, select_mart_columns


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def load_pipeline_config() -> dict:
    path = _repo_root()/"config"/"pipeline.yaml"
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def main() -> None:
    cfg = load_pipeline_config()
    spark = SparkSession.builder.getOrCreate()

    target = cfg["target"]
    paths = cfg["paths"]
    merge_key = cfg["merge_key"]
    mart_columns: list[str] = cfg["mart_columns"]

    df = build_mart_dataframe()
    mart_df = select_mart_columns(df,mart_columns)
    validate_mart_dataframe(mart_df, paths["expectations"])

    ensure_table(
        spark,
        paths["ddl"],
        target["catalog"],
        target["schema"],
        target["table"],
    )
    merge_into_delta_table(
        spark,
        mart_df,
        target["catalog"],
        target["schema"],
        target["table"],
        merge_key,
    )

    fqn = f"{target['catalog']}.{target['schema']}.{target['table']}"
    print(f"MERGE finished successfully into {fqn}")


if __name__ == "__main__":
    main()
