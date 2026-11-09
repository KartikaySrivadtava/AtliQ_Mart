# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Notebook 1 of 3 — BRONZE
# MAGIC ### AtliQ Mart · 10-minute grocery delivery
# MAGIC
# MAGIC **Rule of Bronze: change nothing.**
# MAGIC No cleaning, no filtering, no renaming. We land the files exactly as they arrived
# MAGIC and add two audit columns, so we always know *when* a row landed and *which file* it came from.
# MAGIC
# MAGIC ```
# MAGIC   /Volumes (raw CSV)  →  🥉 BRONZE  →  🥈 SILVER  →  🥇 GOLD
# MAGIC        you are here
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0 — One-time setup
# MAGIC
# MAGIC Run the next cell **once**. It creates a schema called `quickcommerce` and a **Volume** called `raw`.
# MAGIC
# MAGIC Then go to **Catalog → workspace → quickcommerce → Volumes → raw → Upload to this volume**
# MAGIC and upload the 5 CSV files.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA  = "quickcommerce"
VOLUME  = "raw"

RAW_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

print("Upload your CSV files here →", RAW_PATH)

# COMMAND ----------

# Confirm the files actually landed in the Volume
import os

for file_name in sorted(os.listdir(RAW_PATH)):
    print(file_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Read one file the long way
# MAGIC
# MAGIC Before we automate it, let's see every option spelled out.

# COMMAND ----------

orders_raw = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{RAW_PATH}/orders.csv")
)

display(orders_raw.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Land all five files in Bronze
# MAGIC
# MAGIC Same three lines, five times. One small function keeps it readable.

# COMMAND ----------

from pyspark.sql import functions as F

def load_to_bronze(file_name: str, table_name: str):
    """Read one CSV from the Volume and land it in a Bronze Delta table."""
    df = (
        spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(f"{RAW_PATH}/{file_name}")
            .withColumn("_source_file", F.lit(file_name))
            .withColumn("_ingested_at", F.current_timestamp())
    )
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
    print(f"{table_name:<22} {df.count():>7,} rows")

load_to_bronze("orders.csv",       "bronze_orders")
load_to_bronze("order_items.csv",  "bronze_order_items")
load_to_bronze("products.csv",     "bronze_products")
load_to_bronze("riders.csv",       "bronze_riders")
load_to_bronze("dark_stores.csv",  "bronze_dark_stores")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Reality check: how dirty is this data?
# MAGIC
# MAGIC We clean nothing in Bronze. But we should *look* at the damage —
# MAGIC this is the moment the Silver layer earns its salary.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Problem 1: the same status, spelled seven different ways
# MAGIC SELECT order_status, count(*) AS orders
# MAGIC FROM bronze_orders
# MAGIC GROUP BY order_status
# MAGIC ORDER BY orders DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Problem 2: duplicate orders (retries and replays from the source system)
# MAGIC SELECT
# MAGIC   count(*)                            AS total_rows,
# MAGIC   count(DISTINCT order_id)            AS unique_orders,
# MAGIC   count(*) - count(DISTINCT order_id) AS duplicate_rows
# MAGIC FROM bronze_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Problem 3: junk test orders, and orders with no rider attached
# MAGIC SELECT
# MAGIC   sum(CASE WHEN order_amount <= 0 THEN 1 ELSE 0 END) AS junk_orders,
# MAGIC   sum(CASE WHEN rider_id IS NULL  THEN 1 ELSE 0 END) AS orders_without_rider
# MAGIC FROM bronze_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Problem 4: one city, written five different ways
# MAGIC SELECT city, count(*) AS stores
# MAGIC FROM bronze_dark_stores
# MAGIC GROUP BY city
# MAGIC ORDER BY city

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Bronze is done
# MAGIC
# MAGIC | Table | What it holds |
# MAGIC |---|---|
# MAGIC | `bronze_orders` | every order, duplicates and junk included |
# MAGIC | `bronze_order_items` | the lines inside each order |
# MAGIC | `bronze_products` | product catalogue |
# MAGIC | `bronze_riders` | delivery riders |
# MAGIC | `bronze_dark_stores` | the mini-warehouses orders leave from |
# MAGIC
# MAGIC We found four data quality problems and **fixed none of them**. That is correct.
# MAGIC
# MAGIC ➡️ **Next: open `02_silver_layer`.**
