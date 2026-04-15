# Databricks notebook source
# MAGIC %md
# MAGIC # Task 2: Transform Data (Bronze → Silver)
# MAGIC Reads previous task result via `taskValues`, transforms raw orders  
# MAGIC into Silver layer with business calculations. Returns row count.
# MAGIC
# MAGIC **RetailHub scenario:** Clean orders, compute net amounts, add processing metadata.

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import date
import json

# Parameters
dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("source_table", "bronze.bronze_orders", "Source Table")
dbutils.widgets.text("target_table", "silver.silver_orders", "Target Table")
dbutils.widgets.text("run_date", "")

catalog = dbutils.widgets.get("catalog")
source_table = f"{catalog}.{dbutils.widgets.get('source_table')}" if catalog else dbutils.widgets.get("source_table")
target_table = f"{catalog}.{dbutils.widgets.get('target_table')}" if catalog else dbutils.widgets.get("target_table")
run_date = dbutils.widgets.get("run_date") or str(date.today())

# COMMAND ----------

# Get result from previous task (optional)
try:
    prev_result = dbutils.jobs.taskValues.get(
        taskKey="validate",
        key="returnValue",
        default="{}"
    )
    prev_data = json.loads(prev_result)
    print(f"Previous task result: {prev_data}")
except:
    print("Running standalone (no previous task)")

# COMMAND ----------

# Transformation: Bronze → Silver
print(f"Transforming: {source_table} → {target_table}")

df = spark.table(source_table)

df_transformed = (
    df
    .withColumn("gross_amount", col("quantity") * col("unit_price"))
    .withColumn("discount_amount",
                round(col("quantity") * col("unit_price") * col("discount_percent") / 100, 2))
    .withColumn("net_amount",
                round(col("quantity") * col("unit_price") * (1 - col("discount_percent") / 100), 2))
    .withColumn("order_date", col("order_datetime").cast("date"))
    .withColumn("processing_date", lit(run_date))
    .filter(col("order_id").isNotNull())
    .filter(col("quantity") > 0)
)

row_count = df_transformed.count()
print(f"Transformed {row_count:,} rows")

df_transformed.select(
    "order_id", "customer_id", "quantity", "unit_price",
    "gross_amount", "discount_amount", "net_amount"
).show(5)

# COMMAND ----------

# Return result
dbutils.notebook.exit(json.dumps({
    "status": "SUCCESS",
    "rows_transformed": row_count
}))
