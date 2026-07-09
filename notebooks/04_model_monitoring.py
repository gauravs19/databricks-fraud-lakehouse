# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Model monitoring (drift + effectiveness)
# MAGIC
# MAGIC Runs after daily scoring. Writes one row of metrics per (day, model_version)
# MAGIC to `ml_monitoring_metrics`, which the dashboard trends over time.
# MAGIC
# MAGIC What is monitored, and why:
# MAGIC - **PSI (Population Stability Index)** of the daily score distribution vs the
# MAGIC   training-time baseline — the standard early-warning signal that incoming data
# MAGIC   no longer looks like training data. Rule of thumb: <0.1 stable, 0.1–0.2 watch,
# MAGIC   >0.2 investigate/retrain.
# MAGIC - **Alert precision & fraud recall** against confirmed labels. In production,
# MAGIC   fraud labels arrive days/weeks late (chargebacks, investigations); here the
# MAGIC   synthetic feed carries labels immediately, standing in for that delayed feed.
# MAGIC - **Alert volume** — a sudden spike or collapse usually means upstream data
# MAGIC   problems before it means fraud trends.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

import numpy as np
from pyspark.sql import functions as F

dbutils.widgets.text("monitor_date", "auto")

preds = spark.table(T_PREDICTIONS)
monitor_date = dbutils.widgets.get("monitor_date")
if monitor_date == "auto":
    monitor_date = preds.select(F.max(F.to_date("event_ts"))).first()[0].isoformat()

day = preds.where(F.to_date("event_ts") == F.lit(monitor_date))
assert day.count() > 0, f"no predictions for {monitor_date}"
model_version = day.select(F.max("model_version")).first()[0]

# COMMAND ----------

# MAGIC %md
# MAGIC ## PSI vs training baseline

# COMMAND ----------

baseline = spark.table(T_SCORE_BASELINE).orderBy("bin_low").toPandas()
scores = np.array(day.select("fraud_score").toPandas()["fraud_score"])
bins = np.append(baseline["bin_low"].values, 1.0)
actual_hist, _ = np.histogram(scores, bins=bins)

expected = np.clip(baseline["fraction"].values, 1e-4, None)
actual = np.clip(actual_hist / max(actual_hist.sum(), 1), 1e-4, None)
psi = float(np.sum((actual - expected) * np.log(actual / expected)))
print(f"PSI({monitor_date}) = {psi:.4f}  "
      f"[{'STABLE' if psi < 0.1 else 'WATCH' if psi < PSI_ALERT_LEVEL else 'DRIFT — investigate'}]")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Effectiveness vs (delayed) labels

# COMMAND ----------

labeled = (
    day.join(spark.table(T_SILVER).select("transaction_id", "is_fraud"), "transaction_id")
    .agg(
        F.count("*").alias("scored_count"),
        F.sum("is_alert").alias("alert_count"),
        F.sum("is_fraud").alias("actual_fraud_count"),
        F.sum(F.when((F.col("is_alert") == 1) & (F.col("is_fraud") == 1), 1)
              .otherwise(0)).alias("true_positive_count"),
    ).first()
)
precision = (labeled["true_positive_count"] / labeled["alert_count"]) if labeled["alert_count"] else None
recall = (labeled["true_positive_count"] / labeled["actual_fraud_count"]) if labeled["actual_fraud_count"] else None
print(f"alerts={labeled['alert_count']}, precision={precision}, fraud recall={recall}")

# COMMAND ----------

metrics_row = spark.createDataFrame(
    [(monitor_date, int(model_version), float(psi),
      int(labeled["scored_count"]), int(labeled["alert_count"]),
      int(labeled["actual_fraud_count"]), int(labeled["true_positive_count"]),
      float(precision) if precision is not None else None,
      float(recall) if recall is not None else None)],
    "monitor_date STRING, model_version INT, psi DOUBLE, scored_count INT, "
    "alert_count INT, actual_fraud_count INT, true_positive_count INT, "
    "alert_precision DOUBLE, fraud_recall DOUBLE",
).withColumn("computed_at", F.current_timestamp())

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {T_MONITORING} (
        monitor_date STRING, model_version INT, psi DOUBLE, scored_count INT,
        alert_count INT, actual_fraud_count INT, true_positive_count INT,
        alert_precision DOUBLE, fraud_recall DOUBLE, computed_at TIMESTAMP
    )
""")
metrics_row.createOrReplaceTempView("_metrics_row")
spark.sql(f"""
    MERGE INTO {T_MONITORING} t
    USING _metrics_row s
    ON t.monitor_date = s.monitor_date AND t.model_version = s.model_version
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

if psi >= PSI_ALERT_LEVEL:
    # surfaces in job output & notifications; in a paid workspace this would page
    print(f"WARNING: PSI {psi:.3f} >= {PSI_ALERT_LEVEL} — score distribution has drifted; "
          f"review features and consider retraining (see RUNBOOK)")
