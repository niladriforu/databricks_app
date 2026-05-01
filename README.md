# databricks_app

Quick Python project scaffold managed with Poetry.

## Create a Databricks job (YAML bundle — recommended for SQL warehouses)

This repo includes `databricks.yml` (Databricks Asset Bundle) that defines a job with a `sql_task` against your **SQL warehouse**.

1. Install the Databricks CLI (outside Poetry): follow Databricks docs for your OS.
2. Export auth (same vars as most Databricks tooling):
   - `export DATABRICKS_HOST="https://<your-workspace-url>"`
   - `export DATABRICKS_TOKEN="<your-pat-token>"`
3. Provide the warehouse id to the bundle (either works):
   - `export BUNDLE_VAR_warehouse_id="<warehouse-id>"`, or
   - `databricks bundle deploy --var="warehouse_id=<warehouse-id>"`
4. Deploy + run:
   - `databricks bundle deploy`
   - `databricks bundle run run_ingest_queries`

SQL lives in `sql/ingest_join.sql`.

## Deploy from GitHub Actions

This repo includes `.github/workflows/databricks-bundle-deploy.yml`, which runs on pushes to `main` (and can be run manually via **Actions → Deploy Databricks bundle → Run workflow**).

Add these **repository secrets** (`Settings → Secrets and variables → Actions`):

- `DATABRICKS_HOST` — workspace URL (example: `https://dbc-xxxx.cloud.databricks.com`)
- `DATABRICKS_TOKEN` — personal access token (or your org’s preferred CI auth)
- `BUNDLE_VAR_WAREHOUSE_ID` — SQL warehouse id (this becomes `BUNDLE_VAR_warehouse_id` at deploy time)

The workflow runs:

- `databricks bundle validate -t dev`
- `databricks bundle deploy -t dev`
