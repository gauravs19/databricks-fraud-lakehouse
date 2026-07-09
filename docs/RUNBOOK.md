# Runbook — fraud-lakehouse operations

Playbooks for the failure modes this system can hit. Ordered by likelihood.

## 1. Daily job failed

**Detect**: failure email (job notification) or red run in Jobs & Pipelines.

1. Open the run, find the first failed task — downstream tasks are skipped by design.
2. `land_new_data` failed → usually volume permissions or quota. Rerun the task;
   the generator skips days that already exist, so reruns are safe.
3. `run_pipeline` failed → open the pipeline event log. Most common causes:
   - *Schema change in source*: new/renamed field lands in `_rescued_data`
     (Bronze never fails on schema). Decide: promote the field into the Silver
     contract (edit `pipelines/fraud_pipeline.py`, PR, redeploy) or ignore.
   - *Quota exhausted*: wait for the quota window to reset; reduce data volume
     if recurring (generator `TXNS_PER_DAY`).
4. `batch_score` failed with the "no transactions found" assertion → the pipeline
   didn't ingest the expected day. Fix the pipeline first, then **Repair run**
   (reruns only failed/skipped tasks).
5. After fixing: use **Repair run**, never a fresh run — it preserves the run's
   date parameters.

## 2. Backfill a missed day

Scoring is a MERGE, so backfills are safe and repeatable:

1. Confirm the day's file exists in the volume; if not, run
   `01_generate_transactions` with `start_date=<day>`, `num_days=1`
2. Trigger a pipeline update (picks up the file incrementally)
3. Run `03_batch_score` with widget `score_date=<YYYY-MM-DD>`
4. Run `04_model_monitoring` with `monitor_date=<YYYY-MM-DD>`

## 3. Quarantine rate spiked

**Detect**: Data Quality section of the dashboard (query 8) trending up.

1. Sample the quarantine table and group by `_quarantine_reason`:
   ```sql
   SELECT _quarantine_reason, COUNT(*)
   FROM workspace.fraud_lakehouse.silver_transactions_quarantine
   WHERE DATE(_ingested_at) = current_date()
   GROUP BY 1;
   ```
2. One reason dominating = upstream contract break (e.g. amounts arriving as
   strings). The data is intact in Bronze/quarantine — nothing is lost.
3. Fix direction: upstream producer fix (preferred) or a documented cast added
   to the Silver contract. Never silently widen a hard rule to make the number
   go down — the rule was catching something.

## 4. Model drift alert (PSI ≥ 0.2)

**Detect**: monitoring notebook prints a WARNING; dashboard query 10 shows the trend.

1. Compare today's score distribution (query 11) against the baseline — where
   did mass move?
2. Check whether input data changed first (quarantine spike? volume spike?
   fraud-pattern mix in query 6?). **Drift is usually a data problem before it
   is a model problem.**
3. If data is healthy and drift persists 2–3 days: run `02_train_fraud_model`.
   The champion/challenger gate decides promotion — no human judgment call on
   "is the new model better" is needed.

## 5. Bad model got promoted — roll back

Scoring resolves `@champion` at run time, so rollback is one operation:

```python
from mlflow.tracking import MlflowClient
MlflowClient().set_registered_model_alias(
    "workspace.fraud_lakehouse.fraud_model", "champion", <previous_version>)
```

Then re-score affected days (playbook 2) — MERGE overwrites the bad predictions
and stamps the corrected `model_version` on every row.

## 6. Pipeline full refresh (rebuild Silver/Gold from Bronze)

When Silver logic changed materially (new dedup rule, contract change):
pipeline UI → **Full refresh**. Bronze is the durable copy; Silver/Gold rebuild
from it. Expect a full-history recompute (minutes at this scale). Predictions
are unaffected (separate table); re-score only if the Silver change altered
feature inputs.

## 7. Alert queue flooded / empty

- Flooded: `MAX_DAILY_ALERTS` in `_config.py` caps the queue at analyst
  capacity; if legitimate fraud volume grew, raise it consciously — it's a
  staffing decision, not a tuning knob.
- Empty for multiple days: check alert threshold vs score distribution
  (query 11) — after a retrain, scores can be calibrated differently. Adjust
  `ALERT_THRESHOLD` with eyes on precision/recall history (query 10).

## Known free-tier failure modes

| Symptom | Cause | Response |
|---|---|---|
| Job queued for a long time | Serverless quota/concurrency limit | Let it queue; stagger schedules; reduce volume |
| Pipeline update throttled | Compute-hour quota | Triggered mode (already default); run less frequently |
| MLflow run slow to log | Free-tier control-plane limits | Harmless; don't parallelize training runs |
