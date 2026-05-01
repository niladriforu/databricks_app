"""DDL + Delta MERGE for the denormalized sales mart."""

from __future__ import annotations

from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def target_fqn(catalog: str, schema: str, table: str) -> str:
    return f"`{catalog}`.`{schema}`.`{table}`"


def table_fqn_plain(catalog: str, schema: str, table: str) -> str:
    """catalog.schema.table without backticks (for catalog API)."""
    return f"{catalog}.{schema}.{table}"


def table_exists(spark, catalog: str, schema: str, table: str) -> bool:
    return bool(spark.catalog.tableExists(table_fqn_plain(catalog, schema, table)))


def ensure_table(spark, ddl_path: str | Path, catalog: str, schema: str, table: str) -> None:
    """Run DDL from file only when the target Delta table is missing."""
    if table_exists(spark, catalog, schema, table):
        return
    return bool(apply_ddl(spark, ddl_path, catalog, schema, table))


def apply_ddl(spark, ddl_path: str | Path, catalog: str, schema: str, table: str) -> None:
    path = Path(ddl_path)
    if not path.is_absolute():
        path = _repo_root() / path
    ddl = path.read_text(encoding="utf-8")
    fqn = target_fqn(catalog, schema, table)
    ddl = ddl.replace("{{TARGET_FQN}}", fqn)
    for stmt in ddl.split(";"):
        s = stmt.strip()
        if s:
            spark.sql(s)
        if table_exists(spark, catalog, schema, table):
            return True
        else:
            raise ValueError(
                f"DDL file {path} did not create table {table_fqn_plain(catalog, schema, table)}"
            )
            return False


def merge_into_delta_table(
    spark, df, catalog: str, schema: str, table: str, merge_key: str
) -> None:
    fqn = target_fqn(catalog, schema, table)
    tmp = "src_denormalized_mart_merge"
    df.createOrReplaceTempView(tmp)
    spark.sql(
        f"""
        MERGE INTO {fqn} AS t
        USING {tmp} AS s
        ON t.`{merge_key}` = s.`{merge_key}`
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def select_mart_columns(df, columns: list[str]):
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(
            "Mart columns missing from DataFrame (update pipeline.yaml / DDL): "
            + ", ".join(missing)
        )

    return df.select(*columns)
