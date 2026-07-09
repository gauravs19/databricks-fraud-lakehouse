# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Train fraud model (MLflow + UC registry, champion/challenger)
# MAGIC
# MAGIC Production practices in this notebook:
# MAGIC - **Time-based split** (train on the past, test on the future) — random splits
# MAGIC   overstate fraud-model performance because fraud episodes leak across the split
# MAGIC - **Point-in-time features** from the shared `_features` module (same code scores prod)
# MAGIC - **Imbalance-aware evaluation**: PR-AUC and recall at a fixed alert budget —
# MAGIC   accuracy and ROC-AUC are misleading at a 0.4% positive rate
# MAGIC - **Champion/challenger gate**: the new model is registered, but the `@champion`
# MAGIC   alias only moves if it beats the incumbent on the same test window
# MAGIC - **Signature + input example** logged, so serving/scoring schema mismatches fail fast

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %run ./_features

# COMMAND ----------

import mlflow
import numpy as np
import pandas as pd
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient
from pyspark.sql import functions as F
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score, roc_curve
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time-based split and feature build

# COMMAND ----------

txns = spark.table(T_SILVER)
dates = txns.select(F.min(F.to_date("event_ts")).alias("lo"),
                    F.max(F.to_date("event_ts")).alias("hi")).first()
split_date = F.date_add(F.lit(dates["lo"]), TRAIN_TEST_SPLIT_DAY)
print(f"data: {dates['lo']} .. {dates['hi']}, split after day {TRAIN_TEST_SPLIT_DAY}")

train_txns = txns.where(F.to_date("event_ts") <= split_date)

# merchant risk snapshot: TRAIN window only (no label leakage), persisted for scoring
merchant_risk = build_merchant_risk(train_txns)
merchant_risk.write.mode("overwrite").saveAsTable(T_MERCHANT_RISK)

# features over the full history (velocity windows need continuity), then split
feat = build_features(txns, spark.table(T_MERCHANT_RISK))
train_pdf = feat.where(F.to_date("event_ts") <= split_date).select(*FEATURE_COLS, "is_fraud").toPandas()
test_pdf = feat.where(F.to_date("event_ts") > split_date).select(*FEATURE_COLS, "is_fraud").toPandas()

X_train, y_train = train_pdf[FEATURE_COLS], train_pdf["is_fraud"]
X_test, y_test = test_pdf[FEATURE_COLS], test_pdf["is_fraud"]
print(f"train: {len(X_train)} rows ({y_train.mean():.4%} fraud), "
      f"test: {len(X_test)} rows ({y_test.mean():.4%} fraud)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluation helpers — metrics that matter for a fraud ops team

# COMMAND ----------

def recall_at_fpr(y_true, scores, max_fpr=0.005):
    """Recall achievable while flagging at most `max_fpr` of legitimate traffic."""
    fpr, tpr, _ = roc_curve(y_true, scores)
    return float(tpr[fpr <= max_fpr].max()) if (fpr <= max_fpr).any() else 0.0


def precision_at_k(y_true, scores, k=MAX_DAILY_ALERTS):
    """Precision within the top-k alerts — what analysts actually review."""
    idx = np.argsort(scores)[::-1][:k]
    return float(np.asarray(y_true)[idx].mean())


def evaluate(y_true, scores):
    return {
        "pr_auc": float(average_precision_score(y_true, scores)),
        "roc_auc": float(roc_auc_score(y_true, scores)),
        "recall_at_0p5pct_fpr": recall_at_fpr(y_true, scores),
        f"precision_at_{MAX_DAILY_ALERTS}": precision_at_k(y_true, scores),
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train: interpretable baseline + gradient-boosting candidate
# MAGIC Always keep a simple baseline in the experiment — it anchors whether the
# MAGIC complex model is earning its complexity.

# COMMAND ----------

mlflow.set_experiment(f"/Shared/{SCHEMA}_experiments")
sample_weight = np.where(y_train == 1, (1 - y_train.mean()) / y_train.mean(), 1.0)

candidates = {
    "logreg_baseline": Pipeline([
        ("scaler", StandardScaler()),
        ("clf", LogisticRegression(max_iter=1000, class_weight="balanced")),
    ]),
    "hist_gradient_boosting": HistGradientBoostingClassifier(
        max_iter=300, learning_rate=0.1, max_depth=6,
        early_stopping=True, validation_fraction=0.15, random_state=42,
    ),
}

results = {}
for name, model in candidates.items():
    with mlflow.start_run(run_name=name) as run:
        if name == "hist_gradient_boosting":
            model.fit(X_train, y_train, sample_weight=sample_weight)
        else:
            model.fit(X_train, y_train)
        scores = model.predict_proba(X_test)[:, 1]
        metrics = evaluate(y_test, scores)
        mlflow.log_params({"model_type": name, "n_features": len(FEATURE_COLS),
                           "split_day": TRAIN_TEST_SPLIT_DAY})
        mlflow.log_metrics(metrics)
        mlflow.sklearn.log_model(
            model, name="model",
            signature=infer_signature(X_train.head(), scores[:5]),
            input_example=X_train.head(),
        )
        results[name] = {"run_id": run.info.run_id, "metrics": metrics, "scores": scores}
        print(f"{name}: {metrics}")

challenger_name = max(results, key=lambda n: results[n]["metrics"]["pr_auc"])
challenger = results[challenger_name]
print(f"\nchallenger: {challenger_name} (PR-AUC {challenger['metrics']['pr_auc']:.4f})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Promotion gate — challenger must beat the incumbent champion
# MAGIC The new version is always registered (full lineage), but `@champion` only
# MAGIC moves on a measured win over the same test window.

# COMMAND ----------

client = MlflowClient()
new_version = mlflow.register_model(f"runs:/{challenger['run_id']}/model", MODEL_NAME)
client.set_model_version_tag(MODEL_NAME, new_version.version, "algorithm", challenger_name)
client.set_model_version_tag(MODEL_NAME, new_version.version, "test_pr_auc",
                             f"{challenger['metrics']['pr_auc']:.4f}")

promote, incumbent_pr_auc = True, None
try:
    incumbent = client.get_model_version_by_alias(MODEL_NAME, CHAMPION_ALIAS)
    inc_model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/{incumbent.version}")
    incumbent_pr_auc = evaluate(y_test, inc_model.predict_proba(X_test)[:, 1])["pr_auc"]
    promote = challenger["metrics"]["pr_auc"] >= incumbent_pr_auc + MIN_PROMOTION_GAIN
except Exception:
    print("no existing champion — first version promotes automatically")

if promote:
    client.set_registered_model_alias(MODEL_NAME, CHAMPION_ALIAS, new_version.version)
    print(f"PROMOTED v{new_version.version} to @{CHAMPION_ALIAS} "
          f"(challenger {challenger['metrics']['pr_auc']:.4f} vs incumbent {incumbent_pr_auc})")
else:
    print(f"NOT promoted: challenger {challenger['metrics']['pr_auc']:.4f} "
          f"did not beat incumbent {incumbent_pr_auc:.4f}. v{new_version.version} "
          f"registered for audit; @{CHAMPION_ALIAS} unchanged.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Baseline score distribution snapshot — reference for drift monitoring (04)

# COMMAND ----------

if promote:
    bins = np.linspace(0, 1, 11)
    hist, _ = np.histogram(challenger["scores"], bins=bins)
    baseline = pd.DataFrame({
        "bin_low": bins[:-1], "bin_high": bins[1:],
        "fraction": hist / max(hist.sum(), 1),
        "model_version": int(new_version.version),
    })
    spark.createDataFrame(baseline).write.mode("overwrite").saveAsTable(T_SCORE_BASELINE)
    print(f"score baseline snapshot written to {T_SCORE_BASELINE}")
