# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup Unity Catalog objects
# MAGIC
# MAGIC Creates the schema and raw-landing volume this project uses.
# MAGIC Free Edition ships with a `workspace` catalog you can create schemas in.
# MAGIC
# MAGIC Run once before anything else.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "fraud_lakehouse"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw")

# COMMAND ----------

# MAGIC %md
# MAGIC Verify — you should see the schema and volume below.

# COMMAND ----------

display(spark.sql(f"SHOW VOLUMES IN {CATALOG}.{SCHEMA}"))
print(f"Landing zone ready: /Volumes/{CATALOG}/{SCHEMA}/raw/transactions/")
