# Databricks notebook source
# MAGIC %md
# MAGIC # Task 3: Generate Sales Report
# MAGIC Aggregates RetailHub metrics (total orders, revenue, avg order value, top segments).  
# MAGIC Prints summary report.
# MAGIC
# MAGIC **RetailHub scenario:** Daily sales summary from Silver orders + customer segments.

# COMMAND ----------

from pyspark.sql.functions import *
import json
from datetime import datetime

# Parameters
dbutils.widgets.text("catalog", "", "Catalog")
dbutils.widgets.text("source_table", "silver.silver_orders", "Source Table")

catalog = dbutils.widgets.get("catalog")
source_table = f"{catalog}.{dbutils.widgets.get('source_table')}" if catalog else dbutils.widgets.get("source_table")

# COMMAND ----------

# Aggregations
df = spark.table(source_table)

report = df.agg(
    count("*").alias("total_orders"),
    countDistinct("customer_id").alias("unique_customers"),
    round(sum("net_amount"), 2).alias("total_revenue"),
    round(avg("net_amount"), 2).alias("avg_order_value"),
    round(max("net_amount"), 2).alias("max_order_value"),
    round(avg("discount_percent"), 1).alias("avg_discount_pct")
).collect()[0]

# COMMAND ----------

# Display report
print("\n" + "=" * 50)
print("  RETAILHUB — DAILY SALES REPORT")
print("=" * 50)
print(f"  Total Orders:      {report.total_orders:,}")
print(f"  Unique Customers:  {report.unique_customers:,}")
print(f"  Total Revenue:     ${report.total_revenue:,.2f}")
print(f"  Avg Order Value:   ${report.avg_order_value:.2f}")
print(f"  Max Order Value:   ${report.max_order_value:.2f}")
print(f"  Avg Discount:      {report.avg_discount_pct}%")
print("=" * 50)
print(f"  Generated at:      {datetime.now()}")
print("=" * 50 + "\n")

# COMMAND ----------

# Return result
dbutils.notebook.exit(json.dumps({
    "status": "SUCCESS",
    "total_orders": report.total_orders,
    "total_revenue": float(report.total_revenue)
}))
