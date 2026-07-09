# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Batch scoring (daily inference)
# MAGIC
# MAGIC Scores the latest day of Silver transactions with the `@champion` model and
# MAGIC upserts into the predictions table.
# MAGIC
# MAGIC Production practices:
# MAGIC - **Idempotent**: `MERGE` on `transaction_id` — reruns and backfills never
# MAGIC   create duplicate predictions
# MAGIC - **Alias-based model resolution**: the job pins to `@champion`, not a version
# MAGIC   number; rollback is just re-pointing the alias (see RUNBOOK)
# MAGIC - **Full lineage on every row**: model version and scoring timestamp are stored,
# MAGIC   so any historical alert can be traced to the exact model that produced it
# MAGIC - **Alert budget**: the review queue is capped at analyst capacity; the threshold
# MAGIC   catches the flagrant cases, the cap protects the ops team from alert floods
# MAGIC
# MAGIC **Widget:** `score_date` — `auto` (default) scores the latest date in Silver;
# MAGIC pass an explicit `YYYY-MM-DD` to backfill a specific day.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %run ./_features

# COMMAND ----------

dbutils.widgets.text("score_date", "auto")

# COMMAND ----------

import mlflow
from mlflow.tracking import MlflowClient
from pyspark.sql import Window
from pyspark.sql import functions as F

mlflow.set_registry_uri("databricks-uc")

model_uri = f"models:/{MODEL_NAME}@{CHAMPION_ALIAS}"
model = mlflow.sklearn.load_model(model_uri)
model_version = int(MlflowClient().get_model_version_by_alias(MODEL_NAME, CHAMPION_ALIAS).version)
print(f"scoring with {MODEL_NAME} v{model_version} (@{CHAMPION_ALIAS})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve scoring window and build features
# MAGIC Velocity features need trailing history, so features are computed over a
# MAGIC 30-day lookback and then filtered to the scoring date.

# COMMAND ----------

txns = spark.table(T_SILVER)
score_date = dbutils.widgets.get("score_date")
if score_date == "auto":
    score_date = txns.select(F.max(F.to_date("event_ts"))).first()[0].isoformat()
print(f"scoring date: {score_date}")

lookback = txns.where(
    (F.to_date("event_ts") > F.date_sub(F.lit(score_date), 30))
    & (F.to_date("event_ts") <= F.lit(score_date))
)
feat = build_features(lookback, spark.table(T_MERCHANT_RISK))
to_score = feat.where(F.to_date("event_ts") == F.lit(score_date))
n = to_score.count()
assert n > 0, f"no transactions found for {score_date} — check pipeline ran first"
print(f"{n} transactions to score")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Score and upsert

# COMMAND ----------

pdf = to_score.select("transaction_id", "event_ts", "customer_id", "amount",
                      *FEATURE_COLS).toPandas()
pdf["fraud_score"] = model.predict_proba(pdf[FEATURE_COLS])[:, 1]

scored = spark.createDataFrame(
    pdf[["transaction_id", "event_ts", "customer_id", "amount", "fraud_score"]]
)

# alert = above threshold AND within the daily analyst budget (top-scored first)
scored = (
    scored
    .withColumn("_rank", F.row_number().over(Window.orderBy(F.desc("fraud_score"))))
    .withColumn("is_alert",
                ((F.col("fraud_score") >= ALERT_THRESHOLD)
                 & (F.col("_rank") <= MAX_DAILY_ALERTS)).cast("int"))
    .drop("_rank")
    .withColumn("model_version", F.lit(model_version))
    .withColumn("scored_at", F.current_timestamp())
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {T_PREDICTIONS} (
        transaction_id STRING, event_ts TIMESTAMP, customer_id STRING,
        amount DOUBLE, fraud_score DOUBLE, is_alert INT,
        model_version INT, scored_at TIMESTAMP
    )
""")
scored.createOrReplaceTempView("_scored_batch")
spark.sql(f"""
    MERGE INTO {T_PREDICTIONS} t
    USING _scored_batch s ON t.transaction_id = s.transaction_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

alerts = scored.where("is_alert = 1").count()
print(f"done: {n} scored, {alerts} alerts queued for review "
      f"(threshold={ALERT_THRESHOLD}, budget={MAX_DAILY_ALERTS})")
