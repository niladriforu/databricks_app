# databricks_app

Quick Python project scaffold managed with Poetry.

## Create a Databricks job (bundle)

This repo includes `databricks.yml` (Databricks Asset Bundle) with a **`spark_python_task`** that runs `src/main.py` on an **existing cluster** (no `new_cluster` / job cluster). That fits personal accounts that are not allowed to create clusters but can attach jobs to a cluster that already exists.

1. Install the Databricks CLI (outside Poetry): follow Databricks docs for your OS.
2. Export auth:
   - `export DATABRICKS_HOST="https://<your-workspace-url>"`
   - `export DATABRICKS_TOKEN="<your-pat-token>"`
3. Set the cluster id (from **Compute** → open your cluster → id in the URL or cluster details):
   - `export BUNDLE_VAR_cluster_id="<cluster-id>"`, or
   - `databricks bundle deploy --var="cluster_id=<cluster-id>"`
4. Deploy + run:
   - `databricks bundle deploy`
   - `databricks bundle run run_ingest_queries`

The cluster must be **running** (or your job policy must allow starting it) and your user must have **Can attach to** (or equivalent) on that cluster.

If you have **no interactive cluster at all** (SQL warehouse only), you cannot run this PySpark entrypoint as-is; use a **`sql_task`** against `sql/ingest_join.sql` and a warehouse instead (different job shape than this bundle’s default).

Sample SQL: `sql/ingest_join.sql`.

## Deploy from GitHub Actions

This repo includes `.github/workflows/databricks-bundle-deploy.yml` (manual **Actions → Deploy Databricks bundle → Run workflow**).

Add these **repository secrets** (`Settings → Secrets and variables → Actions`):

- `DATABRICKS_HOST` — workspace URL (example: `https://dbc-xxxx.cloud.databricks.com`)
- `DATABRICKS_TOKEN` — personal access token (or your org’s preferred CI auth)
- `BUNDLE_VAR_CLUSTER_ID` — existing cluster id (passed as `BUNDLE_VAR_cluster_id` in the workflow)

The workflow runs:

- `databricks bundle validate -t dev`
- `databricks bundle deploy -t dev`
