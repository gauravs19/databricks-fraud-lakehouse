# Lakeflow Declarative Pipeline (DLT): Bronze -> Silver (+quarantine) -> Gold
#
# Configure the pipeline with:
#   catalog: workspace
#   schema (target): fraud_lakehouse
#   serverless: true
#
# Design notes
# - Bronze keeps data raw-as-landed (schema inferred, bad values rescued into
#   _rescued_data) plus file lineage metadata. Reprocessing is always possible
#   from Bronze without touching the source.
# - Silver enforces the data contract: explicit types, quality expectations,
#   watermarked dedup. Rows failing HARD expectations are dropped from Silver
#   but captured in a quarantine table with a reason, so nothing is silently lost.
# - Gold tables are materialized views: business-level aggregates recomputed
#   incrementally by the pipeline, consumed by the dashboard and ML.

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

RAW_PATH = "/Volumes/workspace/fraud_lakehouse/raw/transactions"

# --------------------------------------------------------------------------
# Data contract for Silver: hard rules drop the row (and route it to
# quarantine); soft rules only record a metric in the pipeline event log.
# --------------------------------------------------------------------------
HARD_RULES = {
    "valid_transaction_id": "transaction_id IS NOT NULL",
    "valid_event_ts": "event_ts IS NOT NULL",
    "valid_customer": "customer_id IS NOT NULL",
    "valid_amount": "amount IS NOT NULL AND amount > 0",
}
SOFT_RULES = {
    "known_merchant": "merchant_id IS NOT NULL",
    "known_currency": "currency IN ('USD')",
}


# ----------------------------- BRONZE --------------------------------------
@dlt.table(
    name="bronze_transactions",
    comment="Raw transaction events as landed (JSONL via Auto Loader). "
            "Schema inferred; unparseable values rescued into _rescued_data.",
    table_properties={"quality": "bronze"},
)
def bronze_transactions():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .load(RAW_PATH)
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )


# ----------------------------- SILVER --------------------------------------
def _typed_bronze():
    """Explicit casts from inferred Bronze types to the Silver contract."""
    return (
        dlt.read_stream("bronze_transactions")
        .withColumn("event_ts", F.col("event_ts").cast("timestamp"))
        .withColumn("amount", F.col("amount").cast(DoubleType()))
        .withColumn("is_fraud", F.col("is_fraud").cast("int"))
    )


@dlt.table(
    name="silver_transactions",
    comment="Validated, typed, deduplicated transactions. One row per transaction_id. "
            "This is the data contract every downstream consumer relies on.",
    table_properties={"quality": "silver"},
)
@dlt.expect_all(SOFT_RULES)
@dlt.expect_all_or_drop(HARD_RULES)
def silver_transactions():
    return (
        _typed_bronze()
        .withWatermark("event_ts", "2 days")
        .dropDuplicatesWithinWatermark(["transaction_id"])
        .select(
            "transaction_id", "event_ts", "customer_id", "merchant_id",
            "merchant_category", "amount", "currency", "country", "device_id",
            "channel", "is_fraud", "fraud_type", "_source_file", "_ingested_at",
        )
    )


@dlt.table(
    name="silver_transactions_quarantine",
    comment="Rows rejected by Silver hard expectations, with failure reasons. "
            "Reviewed via the DQ section of the dashboard; nothing is silently dropped.",
    table_properties={"quality": "silver"},
)
def silver_transactions_quarantine():
    quarantine_filter = " OR ".join(f"NOT ({rule})" for rule in HARD_RULES.values())
    reason = F.concat_ws(", ", *[
        F.when(~F.expr(rule), F.lit(name)) for name, rule in HARD_RULES.items()
    ])
    return (
        _typed_bronze()
        .where(quarantine_filter)
        .withColumn("_quarantine_reason", reason)
    )


# ------------------------------ GOLD ---------------------------------------
@dlt.table(
    name="gold_daily_kpis",
    comment="Executive KPIs: daily volumes, fraud counts and rates, exposure.",
    table_properties={"quality": "gold"},
)
def gold_daily_kpis():
    t = dlt.read("silver_transactions")
    return (
        t.groupBy(F.to_date("event_ts").alias("txn_date"))
        .agg(
            F.count("*").alias("txn_count"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
            F.sum("is_fraud").alias("fraud_count"),
            F.round(F.sum(F.when(F.col("is_fraud") == 1, F.col("amount"))
                          .otherwise(0)), 2).alias("fraud_amount"),
            F.round(F.avg("is_fraud") * 100, 4).alias("fraud_rate_pct"),
            F.countDistinct("customer_id").alias("active_customers"),
        )
    )


@dlt.table(
    name="gold_merchant_stats",
    comment="Per-merchant volumes and fraud incidence, for merchant risk review.",
    table_properties={"quality": "gold"},
)
def gold_merchant_stats():
    t = dlt.read("silver_transactions")
    return (
        t.groupBy("merchant_id", "merchant_category")
        .agg(
            F.count("*").alias("txn_count"),
            F.round(F.avg("amount"), 2).alias("avg_amount"),
            F.sum("is_fraud").alias("fraud_count"),
            F.round(F.avg("is_fraud") * 100, 4).alias("fraud_rate_pct"),
        )
    )


@dlt.table(
    name="gold_customer_profiles",
    comment="Per-customer behavioral profile: spend patterns, geo and device footprint.",
    table_properties={"quality": "gold"},
)
def gold_customer_profiles():
    t = dlt.read("silver_transactions")
    return (
        t.groupBy("customer_id")
        .agg(
            F.count("*").alias("txn_count"),
            F.round(F.avg("amount"), 2).alias("avg_amount"),
            F.round(F.stddev("amount"), 2).alias("std_amount"),
            F.countDistinct("country").alias("distinct_countries"),
            F.countDistinct("device_id").alias("distinct_devices"),
            F.round(F.avg((F.hour("event_ts") < 6).cast("int")), 4).alias("night_txn_ratio"),
            F.max("event_ts").alias("last_seen_ts"),
        )
    )
