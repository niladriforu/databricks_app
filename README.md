# databricks_app

Quick Python project scaffold managed with Poetry.

## Create a Databricks job (bundle)

This repo includes `databricks.yml` with a **`spark_python_task`** that runs `src/main.py` on **serverless compute for workflows** (`environment_key` + `environments`). You do **not** need a cluster id or permission to create classic clusters.

Docs: [Run jobs with serverless compute](https://docs.databricks.com/en/workflows/jobs/run-serverless-jobs.html).

### Serverless in the notebook UI vs jobs

When you use **General Compute → Serverless** in a notebook, that is **interactive** serverless. Databricks spins compute up and down per session; there is **no stable cluster id** you can copy and reuse as `existing_cluster_id` on a job. Sometimes `spark.conf.get("spark.databricks.clusterUsageTags.clusterId")` shows an id for the current run, but it is **not** the same as attaching a classic all-purpose cluster to a job, and it will not work reliably for `existing_cluster_id`.

For **Jobs**, serverless is configured with **`environment_key`** and job-level **`environments`** (as in this bundle), not with a warehouse id or classic cluster id.

1. Install the Databricks CLI (outside Poetry): follow Databricks docs for your OS.
2. Export auth:
   - `export DATABRICKS_HOST="https://<your-workspace-url>"`
   - `export DATABRICKS_TOKEN="<your-pat-token>"`
3. Deploy + run:
   - `databricks bundle deploy`
   - `databricks bundle run run_ingest_queries`

If you need extra Python packages on serverless, add them under `environments[].spec.dependencies` in `databricks.yml` (see [bundle examples](https://docs.databricks.com/aws/en/dev-tools/bundles/examples)).

For **SQL only** (warehouse), use a **`sql_task`** and `sql/ingest_join.sql` instead of this Python job.

## Deploy from GitHub Actions

This repo includes `.github/workflows/databricks-bundle-deploy.yml` (manual **Actions → Deploy Databricks bundle → Run workflow**).

Add these **repository secrets** (`Settings → Secrets and variables → Actions`):

- `DATABRICKS_HOST` — workspace URL (example: `https://dbc-xxxx.cloud.databricks.com`)
- `DATABRICKS_TOKEN` — personal access token (or your org’s preferred CI auth)

The workflow runs:

- `databricks bundle validate -t dev`
- `databricks bundle deploy -t dev`
