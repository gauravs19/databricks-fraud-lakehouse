# Concepts — a beginner's guide to everything this project uses

Read this top to bottom before (or alongside) [SETUP.md](SETUP.md). Concepts are
ordered the way you meet them in the project: platform → data engineering →
machine learning → operations. Every section ends with **In this project**
(where the concept lives in the repo) and most have **See it yourself** (how to
observe it in your workspace).

---

## Part 1 — The platform

### 1.1 What is Databricks?

Databricks is a cloud platform for working with data at any scale. Under the
hood it runs **Apache Spark** (a distributed compute engine — your code runs in
parallel across many machines), but you interact with it through notebooks, SQL
editors, pipelines and jobs. One platform covers what traditionally needed
four separate tools:

| Traditional tool | Databricks equivalent |
|---|---|
| Data warehouse (Snowflake, Teradata) | Databricks SQL + Delta tables |
| Data lake (raw files on S3/ADLS) | Volumes + Delta Lake |
| ETL tool (Informatica, Airflow DAGs) | Lakeflow pipelines + Jobs |
| ML platform (SageMaker, custom) | Notebooks + MLflow |

### 1.2 What is a *lakehouse*?

The word is a merge of data **lake** and ware**house**, and so is the idea.

- A **data warehouse** stores clean, structured tables. Great for SQL and BI;
  bad at raw/messy/huge data, and expensive.
- A **data lake** is cheap object storage full of raw files (CSV, JSON,
  Parquet). Great for volume and flexibility; but no transactions, no schema
  enforcement — lakes famously rot into "data swamps."
- A **lakehouse** keeps data in cheap open files *but* adds a transaction
  layer on top (Delta Lake, next section) so those files behave like proper
  database tables. You get warehouse reliability at lake economics, and one
  copy of the data serves SQL analysts, data engineers and ML — no copying
  data between systems.

**In this project:** the entire design. Raw JSON files land in a volume,
become reliable Delta tables, and the *same* tables feed a SQL dashboard and an
ML model.

### 1.3 Delta Lake

A Delta table = a folder of Parquet data files **plus a transaction log**
(`_delta_log/`) that records every change as an ordered series of commits.
That little log is what upgrades files into a database table:

- **ACID transactions** — a write either fully happens or doesn't; readers
  never see a half-written table.
- **Schema enforcement** — writes with wrong column types are rejected instead
  of silently corrupting the table.
- **Time travel** — the log keeps history, so you can query the table *as of*
  yesterday: `SELECT * FROM t VERSION AS OF 12`.
- **MERGE** — transactional upsert (update-if-exists, insert-if-new), which
  plain files simply cannot do.

**In this project:** every table (`bronze_transactions` … `ml_predictions`) is
a Delta table. You never say "Delta" — it's the default format.
**See it yourself:** Catalog → any table → *History* tab shows the commit log.

### 1.4 Unity Catalog (UC)

The governance layer — one place that answers: *what data exists, who may
touch it, and where did it come from?* Everything has a three-level name:

```
catalog . schema . object
workspace.fraud_lakehouse.silver_transactions
```

Objects include tables, views, **volumes** (governed folders for raw *files* —
our JSONL landing zone), ML **models**, and functions. Permissions (GRANTs),
audit logs and lineage all hang off these names.

**In this project:** `00_setup_catalog.py` creates the schema and volume; the
model is registered as `workspace.fraud_lakehouse.fraud_model`. Every name is
defined once in `_config.py`.
**See it yourself:** Catalog → `fraud_lakehouse` → click `silver_transactions`
→ *Lineage* tab shows it feeds the gold tables — computed automatically.

### 1.5 Serverless compute

Classically you configure Spark *clusters* (machine types, node counts, idle
timeouts). **Serverless** means Databricks manages all of that: you run a
notebook and compute just… appears, billed per use. Free Edition is
serverless-only, which conveniently forces the modern pattern.

**In this project:** there are no cluster configs anywhere in the repo — a
deliberate design consequence (see `docs/SETUP.md`, constraints table).

---

## Part 2 — Data engineering

### 2.1 Medallion architecture (Bronze / Silver / Gold)

The standard lakehouse pattern: data flows through three layers of increasing
quality, like ore being refined.

| Layer | Contract | Rule of thumb |
|---|---|---|
| **Bronze** | Raw, as-landed, nothing lost | "I can always replay from here" |
| **Silver** | Validated, typed, deduplicated | "Downstream can trust every row" |
| **Gold** | Aggregated for a specific consumer | "One table answers one business question" |

Why not clean the data in one step? Because **you will get cleaning wrong**
and requirements will change. With Bronze preserved, a bug in Silver logic is
recoverable — rebuild Silver from Bronze. Without Bronze, bad cleaning
destroys data forever. The layers also give every consumer the right level:
auditors want Bronze, ML wants Silver, executives want Gold.

**In this project:** `pipelines/fraud_pipeline.py` — one file, all three
layers. Note how Bronze does *no* validation at all (that's the point), and
each Gold table serves exactly one dashboard section.

### 2.2 Auto Loader & incremental ingestion

Naive ingestion re-reads every file in the folder every run — cost grows
forever and rows duplicate. **Auto Loader** (`cloudFiles` format) keeps a
*checkpoint* recording which files it has already processed, so each run
ingests **only new files, exactly once**. It also handles schema: unexpected
values are *rescued* into a `_rescued_data` column instead of crashing the
pipeline at 3am.

**In this project:** the Bronze table definition. The generator writes one
file per day precisely so you can watch Auto Loader pick up only the newest.
**See it yourself:** after the daily job runs, the pipeline event log shows
`1 file` ingested, not 31.

### 2.3 Streaming, watermarks, and deduplication

Bronze→Silver runs as **Structured Streaming**: instead of "process this
dataset," you declare "process whatever arrives, forever," and each triggered
run processes the increment. One subtlety matters here — dedup:

To drop duplicate `transaction_id`s, Spark must remember IDs it has seen. It
can't remember *all* of them forever (memory grows unboundedly), so you declare
a **watermark**: "duplicates only arrive within 2 days of each other — after
that, forget." `withWatermark("event_ts", "2 days")` + 
`dropDuplicatesWithinWatermark` is the bounded-memory dedup pattern.

**In this project:** the Silver table. The generator injects ~0.5% exact
duplicates specifically to give this something to do.

### 2.4 Declarative pipelines (Lakeflow / DLT)

Two ways to build a pipeline:

- **Imperative** (do this, then this): you write read/transform/write code,
  manage checkpoints, ordering, retries, and table creation yourself.
- **Declarative** (here's what should exist): you write functions that *define
  tables*, and the framework figures out the dependency graph (DAG), execution
  order, checkpoints, and incremental processing.

In `fraud_pipeline.py`, `@dlt.table` marks a function as a table definition,
and `dlt.read("silver_transactions")` both reads the table *and* declares the
dependency — that's how the DAG draws itself.

**See it yourself:** open the pipeline in the UI; the Bronze→Silver→Gold graph
was never written down anywhere — it's derived from the code.

### 2.5 Data quality: expectations & quarantine

An **expectation** is a named rule attached to a table, e.g.
`"valid_amount": "amount IS NOT NULL AND amount > 0"`. This project uses the
two severities deliberately:

- **Hard rules** (`expect_all_or_drop`) — violations would break downstream
  logic (a null transaction_id can't be deduped or joined). Rows are dropped
  from Silver…
- **Soft rules** (`expect_all`) — imperfections worth tracking but not
  blocking (a null merchant). Rows pass; failure *counts* are recorded.

…but "dropped" must never mean "vanished." The **quarantine** table captures
every hard-rule failure with a `_quarantine_reason`, so data loss through the
quality gate is visible, measurable, and debuggable. That's the difference
between a quality gate and a silent data shredder.

**In this project:** `HARD_RULES` / `SOFT_RULES` dicts drive both the Silver
expectations *and* the quarantine filter — one definition, two uses.
**See it yourself:** dashboard queries 8–9, or click Silver in the pipeline UI
for per-rule pass rates.

### 2.6 Materialized views (the Gold layer)

A regular view re-runs its query every time it's read. A **materialized
view** stores the result and the pipeline keeps it fresh — dashboards read
precomputed numbers instead of re-aggregating millions of rows per page load.
The classic trade: compute once at write time, serve cheaply at read time.

---

## Part 3 — Machine learning

### 3.1 Features & feature engineering

Models don't understand "transactions" — they understand numbers. A
**feature** is one numeric signal derived from raw data. The craft is encoding
*domain knowledge* as features; ours map one-to-one to fraud patterns:

| Feature | Fraud intuition |
|---|---|
| `txn_count_1h` | card testing = burst of transactions |
| `amount_over_avg` | account takeover = spending unlike *this* customer's history |
| `is_new_device`, `is_foreign` | takeover happens from new devices/places |
| `merchant_fraud_rate` | some merchants attract fraud |

### 3.2 Data leakage & point-in-time correctness

**The most important ML concept in this repo.** Leakage = training on
information that won't exist at prediction time. The model looks brilliant in
testing and useless in production.

Sneaky example: compute each customer's average spend over *all 30 days*, then
train on days 1–24. A day-10 transaction's feature now contains day-25 spending
— information from the future. **Point-in-time correct** means every feature
for a transaction uses only data *strictly before* that transaction. In
`_features.py`, every window ends at `-1` (row or second) — that's the leakage
guard. Same reason `merchant_fraud_rate` is computed on the training window
only: computing it on test data would bake test *labels* into a feature.

### 3.3 Time-based splits

Random train/test splits are wrong for fraud: a 12-transaction card-testing
episode gets split ~6/6 between train and test, so the model is tested on
episodes it partially memorized. Splitting by **time** (train on days 1–24,
test on 25–30) matches reality: predict *future* fraud from *past* data.

### 3.4 Class imbalance — and why accuracy is a lie here

~0.4% of transactions are fraud. A "model" that always answers *not fraud* is
99.6% accurate and 100% useless. So we never report accuracy, and instead use:

- **Precision** — of the transactions we flagged, how many were fraud?
  (Low precision = analysts drown in false alarms.)
- **Recall** — of actual fraud, how much did we catch?
  (Low recall = losses walk out the door.)
- **PR-AUC** — precision/recall trade-off summarized across all thresholds;
  the standard headline metric for rare-event models.
- **Precision@200** — precision within the top-200 daily alerts, because the
  ops team can only review ~200 cases/day. Metrics should mirror the business
  constraint.

During training, fraud rows also get ~250× **sample weight** so the model
can't win by ignoring the rare class.

### 3.5 The two models (and why there are two)

- **Logistic regression** — simple, fast, interpretable *baseline*.
- **Gradient boosting** (`HistGradientBoostingClassifier`) — hundreds of small
  decision trees, each correcting the previous ones' errors; the workhorse for
  tabular data.

The baseline isn't decoration: it anchors whether the complex model *earns its
complexity*. If boosting beats logistic regression by 1%, ship the simple one.

### 3.6 MLflow: experiments, registry, aliases

Without tracking, ML devolves into `model_final_v3_REAL.pkl`. MLflow gives:

- **Experiment tracking** — every training run logs its parameters, metrics
  and artifacts; runs are comparable side by side forever.
- **Model registry** (in UC) — trained models become versioned, governed
  objects: `fraud_model` v1, v2, v3…
- **Aliases** — a named pointer to a version. Scoring code says "load
  `@champion`" and never hardcodes a version, so *promoting* or *rolling back*
  a model is just moving the pointer. No code change, no redeploy.

### 3.7 Champion/challenger

The production answer to "is the new model actually better?" Every retrain
produces a **challenger**; it is always registered (audit trail), but
`@champion` only moves if the challenger *measurably beats* the incumbent on
the identical test window. The gate is code, not judgment — nobody promotes a
model on a hunch at 6pm on Friday.

**In this project:** the promotion-gate cell in `02_train_fraud_model.py`.
**See it yourself:** rerun training; watch a version get registered but (if it
doesn't win) *not* promoted.

### 3.8 Batch scoring & idempotency

**Batch scoring** = score yesterday's transactions on a schedule, feeding a
human review queue (vs. *real-time serving* = score each transaction in
milliseconds during authorization — see ADR-0003 for why that's out of scope
here, and why both layers coexist in real banks).

**Idempotent** = running twice produces the same result as once. Scoring
writes via `MERGE` keyed on `transaction_id`: reruns and backfills *update*
rows instead of duplicating them. In operations, safe-to-rerun is the property
that lets you fix things at 3am without fear.

### 3.9 Drift & PSI

Models decay because the world changes — fraudsters adapt, customers shift.
**Drift** = today's data no longer resembles training data. We watch the
*score distribution*: snapshot its histogram at training time, compare each
day's histogram against it with **PSI (Population Stability Index)** — a
single number for "how much has this distribution moved":

- PSI < 0.1 — stable
- 0.1–0.2 — watch
- \> 0.2 — investigate, likely retrain

Rule from the RUNBOOK worth internalizing: **drift is usually a data problem
before it is a model problem** — check quarantine rates and volumes before
blaming the model.

---

## Part 4 — Operations

### 4.1 Jobs & orchestration

A **job** is a DAG of tasks with a schedule, retries and notifications. Ours:
`land_new_data → run_pipeline → batch_score → monitor` — each task depends on
the previous, so a pipeline failure means scoring never runs on stale data,
and **Repair run** re-executes only failed tasks.

### 4.2 Infrastructure as code: Asset Bundles

Clicking resources together in a UI works once, then can't be reviewed,
reproduced, or promoted between environments. A **Databricks Asset Bundle**
(`databricks.yml`) declares jobs and pipelines in YAML that lives in git —
deployable to `dev`/`prod` targets with one CLI command, reviewable in a PR
like any code.

### 4.3 CI & the runbook

- **CI** (`.github/workflows/ci.yml`): every push is linted, syntax-checked
  and bundle-validated *before* it reaches the workspace. Broken code should
  fail in CI, not in the 5am job run.
- **Runbook** (`RUNBOOK.md`): written *before* the incident — what to do when
  the job fails, quarantine spikes, drift alerts, or a bad model ships. The
  discipline of writing playbooks in calm weather is itself the production
  skill being showcased.

---

## Quick glossary

| Term | One-liner |
|---|---|
| Lakehouse | Cheap open files + transaction layer = warehouse reliability at lake cost |
| Delta Lake | Parquet files + transaction log → ACID tables with time travel |
| Unity Catalog | Governance: `catalog.schema.object` names, permissions, lineage |
| Volume | UC-governed folder for raw files |
| Medallion | Bronze (raw) → Silver (valid) → Gold (business aggregates) |
| Auto Loader | Incremental file ingestion; each file exactly once |
| Watermark | "Forget state older than X" — bounded memory for streaming |
| Expectation | Named data-quality rule attached to a table |
| Quarantine | Table holding rejected rows + reasons; nothing dropped silently |
| Materialized view | Precomputed query result, kept fresh by the pipeline |
| Leakage | Training on information unavailable at prediction time |
| PR-AUC | Precision/recall summary metric for rare-event models |
| Registry alias | Movable pointer (`@champion`) → promotion/rollback without code change |
| Champion/challenger | New model must measurably beat incumbent to be promoted |
| Idempotent | Safe to run twice; MERGE makes scoring reruns harmless |
| PSI | Single-number distribution-shift metric; >0.2 = investigate |
| Asset Bundle | Databricks jobs/pipelines declared as YAML in git |
