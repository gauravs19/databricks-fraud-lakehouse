# Databricks notebook source
# MAGIC %md
# MAGIC # _config — single source of truth for names, paths and thresholds
# MAGIC
# MAGIC Included by every notebook via `%run ./_config`. Nothing else in the repo
# MAGIC hardcodes a catalog, schema, path or threshold.

# COMMAND ----------

# --- Unity Catalog ---------------------------------------------------------
CATALOG = "workspace"                # Free Edition default catalog
SCHEMA = "fraud_lakehouse"
FQ = f"{CATALOG}.{SCHEMA}"           # fully-qualified prefix

# --- Storage ---------------------------------------------------------------
RAW_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/raw"
RAW_TRANSACTIONS_DIR = f"{RAW_VOLUME}/transactions"

# --- Tables ----------------------------------------------------------------
T_SILVER = f"{FQ}.silver_transactions"
T_QUARANTINE = f"{FQ}.silver_transactions_quarantine"
T_MERCHANT_RISK = f"{FQ}.ml_merchant_risk"
T_PREDICTIONS = f"{FQ}.ml_predictions"
T_MONITORING = f"{FQ}.ml_monitoring_metrics"
T_SCORE_BASELINE = f"{FQ}.ml_score_baseline"

# --- Model -----------------------------------------------------------------
MODEL_NAME = f"{FQ}.fraud_model"
CHAMPION_ALIAS = "champion"

# --- Scoring / alerting ----------------------------------------------------
ALERT_THRESHOLD = 0.50               # score above which a txn enters the review queue
MAX_DAILY_ALERTS = 200               # analyst capacity guardrail: cap review queue size

# --- Training --------------------------------------------------------------
TRAIN_TEST_SPLIT_DAY = 24            # days 1..N train, rest test (time-based split)
MIN_PROMOTION_GAIN = 0.0             # challenger must beat champion PR-AUC by this margin

# --- Monitoring ------------------------------------------------------------
PSI_ALERT_LEVEL = 0.2                # population stability index above this = drift warning
