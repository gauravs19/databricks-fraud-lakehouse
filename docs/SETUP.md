# Setup — Databricks Free Edition walkthrough

End-to-end setup takes ~45 minutes the first time. Everything runs on serverless
compute inside one free workspace.

> New to Databricks? Read [CONCEPTS.md](CONCEPTS.md) first (or keep it open in
> a second tab) — it explains every term this walkthrough uses.

## 0. Prerequisites

- A [Databricks Free Edition](https://www.databricks.com/learn/free-edition) account
  (sign up with any email or Google account — no credit card, no cloud account)
- This repo pushed to your GitHub

> Free Edition replaced the old Community Edition in 2025. It is a real
> serverless workspace: Unity Catalog, Delta Lake, Lakeflow pipelines, Jobs,
> Databricks SQL and MLflow all included, with usage quotas instead of a time limit.

## 1. Connect the repo

1. In the workspace sidebar: **Workspace → your user folder → Create → Git folder**
2. Paste the GitHub URL of this repo, provider *GitHub*, and create.
   (Public repo needs no token; private repo needs a GitHub PAT under
   **Settings → Linked accounts**.)

Working from a Git folder — not loose notebooks — is itself the production
pattern: code review, history, and rollback come from Git, not the workspace.

## 2. Bootstrap Unity Catalog objects

Open and run `notebooks/00_setup_catalog.py` (attach to **Serverless** compute —
the only and default option). It creates:

- schema `workspace.fraud_lakehouse`
- volume `workspace.fraud_lakehouse.raw` — the file landing zone

## 3. Generate 30 days of history

Run `notebooks/01_generate_transactions.py` with the default widgets
(`start_date=2026-06-01`, `num_days=30`). ~2–3 minutes; writes one JSONL file
per day (~250k rows total, ~0.4% fraud) into the volume. Reruns skip existing
days, so it is safe to run again.

## 4. Create and run the pipeline

1. **Jobs & Pipelines → Create → ETL pipeline** (Lakeflow Declarative Pipelines)
2. Source code: select the **file** `pipelines/fraud_pipeline.py` **inside your
   Git folder** — the file itself, not the `pipelines/` folder and not the repo
   root. If the create dialog scaffolds its own pipeline folder instead of
   letting you browse, create the pipeline first, then open **Settings →
   Source code → Add/Configure** and point it at the file in the Git folder.
3. Destination — catalog `workspace`, schema `fraud_lakehouse`; compute: serverless; mode: **Triggered**
4. **Start**. First run ingests all 30 files; watch the DAG build
   Bronze → Silver (+quarantine) → 3 Gold tables. Click Silver to see the
   expectation metrics — the deliberately dirty rows land in
   `silver_transactions_quarantine`.

### Troubleshooting: `NO_TABLES_IN_PIPELINE`

*"Pipelines are expected to have at least one table defined but no tables were
found"* means the update ran but scanned **zero `@dlt.table` definitions** —
i.e. the pipeline is not actually reading `fraud_pipeline.py`. Check in order:

1. **Settings → Source code** must list the exact path to the file, e.g.
   `/Workspace/Users/<you>/databricks-fraud-lakehouse/pipelines/fraud_pipeline.py`.
   The most common cause is a folder (or the wrong folder) selected instead of
   the file — a scanned folder with no matching source yields exactly this error.
2. **New Lakeflow pipeline editor**: creating an "ETL pipeline" may scaffold a
   dedicated pipeline root folder (with a `transformations/` subfolder) and
   only scan *that* folder — your Git folder's file is never seen. Either add
   the Git-folder file explicitly as source code in settings, or copy
   `fraud_pipeline.py` into the scaffolded `transformations/` folder.
3. **Pull the latest repo version** (Git folder → ⋮ → Pull). The file carries a
   `# Databricks notebook source` header so every picker treats it as
   selectable source; very old checkouts may predate it.
4. **Run As / sharing** (per the error text): if the pipeline runs as a
   different principal, that principal needs access to the Git folder. On a
   single-user Free Edition workspace this is rarely the issue.

After fixing, run the pipeline again — the DAG should show 6 tables
(bronze, silver, quarantine, 3 gold).

## 5. Train and register the model

Run `notebooks/02_train_fraud_model.py`. It trains a logistic-regression
baseline and a gradient-boosting candidate, logs both to MLflow, registers the
winner in Unity Catalog as `workspace.fraud_lakehouse.fraud_model`, and (first
run) promotes it to the `@champion` alias. Check **Experiments** for runs and
**Catalog → models** for the registered version.

Rerun it later and watch the champion/challenger gate: the new version is
registered but only promoted if it beats the incumbent on the same test window.

## 6. Score and monitor

1. Run `notebooks/03_batch_score.py` (widget `score_date=auto` scores the latest
   day) — writes `ml_predictions` with an alert flag and model lineage
2. Run `notebooks/04_model_monitoring.py` — writes PSI + precision/recall to
   `ml_monitoring_metrics`

## 7. Build the dashboard

In **SQL editor**, create the queries from `sql/dashboard_queries.sql` (numbered,
with suggested visualization per query), then assemble a dashboard with four
sections: **Executive KPIs / Fraud Operations / Data Quality / Model Health**.

## 8. Automate it (the daily job)

UI path: **Jobs & Pipelines → Create job** with four tasks mirroring
`databricks.yml`:

| # | Task | Type | Depends on |
|---|---|---|---|
| 1 | land_new_data | Notebook `01_generate_transactions` (num_days=1) | — |
| 2 | run_pipeline | Pipeline task → fraud-lakehouse-pipeline | 1 |
| 3 | batch_score | Notebook `03_batch_score` | 2 |
| 4 | monitor | Notebook `04_model_monitoring` | 3 |

CLI path (production-grade): install the Databricks CLI, `databricks auth login`
against your workspace, set your workspace URL in `databricks.yml`, then:

```bash
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run -t dev daily_fraud_job
```

Each daily run lands one new day of data, the pipeline picks up **only the new
file** (Auto Loader checkpointing — verify in the pipeline event log), scores
it, and appends monitoring metrics. That incremental behavior is the core
lakehouse pattern this project demonstrates.

## Free Edition constraints that shaped this project

| Constraint | Impact | Design response |
|---|---|---|
| Serverless compute only | No custom clusters/instance types | Everything targets serverless; no cluster configs in the repo |
| Usage quotas (compute hours, concurrency) | Heavy jobs may throttle | ~250k-row dataset; triggered (not continuous) pipeline; daily batch cadence |
| No external cloud storage credentials | Can't land data in your own S3/ADLS | UC **Volumes** as the file landing zone |
| Model Serving limited | No always-on REST endpoint | Batch scoring pattern (which is how most fraud review queues actually run) |
| Single workspace, limited users | No workspace-per-env isolation | Bundle `dev`/`prod` targets simulate env separation via naming/prefixes |

Quotas change; check the current [Free Edition limits](https://docs.databricks.com/getting-started/free-edition-limitations)
if something is throttled.
