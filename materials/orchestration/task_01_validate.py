# Databricks notebook source
# MAGIC %md
# MAGIC # Task 1: Validate Source Data
# MAGIC Validates that the Bronze orders table exists and meets the minimum row count threshold.  
# MAGIC Returns status + count via `dbutils.notebook.exit()`.
# MAGIC
# MAGIC **RetailHub scenario:** Check that raw order data has been ingested before transformation.

# COMMAND ----------

# Parameters from Job
dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("source_table", "bronze.bronze_orders", "Source Table (schema.table)")
dbutils.widgets.text("min_rows", "100")

catalog = dbutils.widgets.get("catalog")
source_table = dbutils.widgets.get("source_table")
min_rows = int(dbutils.widgets.get("min_rows"))

full_table = f"{catalog}.{source_table}" if catalog else source_table

# COMMAND ----------

# Validation
df = spark.table(full_table)
row_count = df.count()

print(f"Source: {full_table}")
print(f"Row count: {row_count:,}")
print(f"Minimum required: {min_rows:,}")

if row_count < min_rows:
    raise Exception(f"Validation FAILED: {row_count} rows < {min_rows} minimum")

print("✅ Validation PASSED")

# COMMAND ----------

# Return result to next task
import json
dbutils.notebook.exit(json.dumps({
    "status": "SUCCESS",
    "source_table": full_table,
    "row_count": row_count
}))
