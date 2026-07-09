# Databricks notebook source
# MAGIC %md
# MAGIC # _features — shared feature engineering
# MAGIC
# MAGIC One implementation used by BOTH training (`02`) and batch scoring (`03`) via
# MAGIC `%run ./_features`. Training/serving skew — training on one feature definition
# MAGIC and scoring on a drifted copy — is one of the most common production ML failure
# MAGIC modes; sharing the code path eliminates it.
# MAGIC
# MAGIC All features are **point-in-time correct**: every feature for a transaction is
# MAGIC computed only from data strictly *before* that transaction (windows end at
# MAGIC `-1 second` / `-1 row`), so no label or future information leaks into training.

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

FEATURE_COLS = [
    "amount",
    "log_amount",
    "hour",
    "is_night",
    "is_online",
    "is_foreign",
    "is_new_device",
    "txn_count_1h",
    "txn_count_24h",
    "amount_over_avg",
    "seconds_since_prev_txn",
    "merchant_fraud_rate",
]


def build_features(txns: DataFrame, merchant_risk: DataFrame) -> DataFrame:
    """Point-in-time feature set for fraud scoring.

    txns          : silver_transactions rows (must span enough trailing history
                    for the velocity windows — 30 days is ample)
    merchant_risk : ml_merchant_risk snapshot (merchant_id, merchant_fraud_rate),
                    always computed on the training window only
    """
    ts = F.col("event_ts").cast("long")

    w_cust = Window.partitionBy("customer_id").orderBy(ts)
    w_1h = w_cust.rangeBetween(-3600, -1)
    w_24h = w_cust.rangeBetween(-86400, -1)
    w_hist = w_cust.rowsBetween(Window.unboundedPreceding, -1)
    w_device = Window.partitionBy("customer_id", "device_id").orderBy(ts)

    feat = (
        txns
        .withColumn("log_amount", F.log1p("amount"))
        .withColumn("hour", F.hour("event_ts"))
        .withColumn("is_night", (F.hour("event_ts") < 6).cast("int"))
        .withColumn("is_online", (F.col("channel") == "online").cast("int"))
        # home country := first country ever seen for the customer (stable proxy)
        .withColumn("home_country",
                    F.first("country").over(w_cust.rowsBetween(Window.unboundedPreceding,
                                                               Window.currentRow)))
        .withColumn("is_foreign", (F.col("country") != F.col("home_country")).cast("int"))
        # first time this customer used this device?
        .withColumn("is_new_device", (F.row_number().over(w_device) == 1).cast("int"))
        # velocity: bursts are the strongest card-testing signal
        .withColumn("txn_count_1h", F.count("*").over(w_1h))
        .withColumn("txn_count_24h", F.count("*").over(w_24h))
        # spend relative to the customer's own history (ATO signal)
        .withColumn("hist_avg_amount", F.avg("amount").over(w_hist))
        .withColumn("amount_over_avg",
                    F.when(F.col("hist_avg_amount").isNull(), F.lit(1.0))
                     .otherwise(F.col("amount") / F.col("hist_avg_amount")))
        .withColumn("prev_ts", F.lag(ts).over(w_cust))
        .withColumn("seconds_since_prev_txn",
                    F.coalesce(ts - F.col("prev_ts"), F.lit(86400 * 30)))
        .join(merchant_risk, "merchant_id", "left")
        .withColumn("merchant_fraud_rate", F.coalesce("merchant_fraud_rate", F.lit(0.0)))
        .drop("hist_avg_amount", "prev_ts", "home_country")
    )
    return feat


def build_merchant_risk(train_txns: DataFrame) -> DataFrame:
    """Historical fraud rate per merchant, smoothed (Laplace) so low-volume
    merchants don't get extreme rates. MUST be computed on the training window
    only, then reused as a static snapshot at scoring time — recomputing it on
    scoring data would leak labels."""
    return (
        train_txns.groupBy("merchant_id")
        .agg(F.count("*").alias("n"), F.sum("is_fraud").alias("n_fraud"))
        .select(
            "merchant_id",
            ((F.col("n_fraud") + 1) / (F.col("n") + 200)).alias("merchant_fraud_rate"),
        )
    )
