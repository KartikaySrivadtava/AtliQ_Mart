# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Notebook 2 of 3 — SILVER
# MAGIC ### AtliQ Mart · 10-minute grocery delivery
# MAGIC
# MAGIC **Rule of Silver: make it trustworthy.**
# MAGIC We fix the four problems Bronze exposed, and we **derive** the columns the business
# MAGIC actually talks about — the ones no source system ever sends you.
# MAGIC
# MAGIC | Derived column | Business meaning |
# MAGIC |---|---|
# MAGIC | `delivery_minutes` | how long the delivery really took |
# MAGIC | `is_late` | did we break the 10-minute promise? |
# MAGIC | `net_amount` | what we actually earned after discount |
# MAGIC
# MAGIC ```
# MAGIC   /Volumes  →  🥉 BRONZE  →  🥈 SILVER  →  🥇 GOLD
# MAGIC                             you are here
# MAGIC ```
# MAGIC
# MAGIC ⚠️ Run `01_bronze_layer` first.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA  = "quickcommerce"

spark.sql(f"USE {CATALOG}.{SCHEMA}")

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — `silver_orders`
# MAGIC
# MAGIC Four fixes and four derived columns, in one readable chain.

# COMMAND ----------

silver_orders = (
    spark.table("bronze_orders")
        .dropDuplicates(["order_id"])                                   # fix 1: kill the replays
        .filter(F.col("order_amount") > 0)                              # fix 2: drop junk / test orders
        .withColumn("order_status", F.upper(F.trim("order_status")))    # fix 3: DELIVERED / CANCELLED
        .withColumn("payment_mode", F.upper(F.trim("payment_mode")))    # fix 4: standardise payments
        .withColumn("payment_mode", F.when(F.col("payment_mode") == "CASH ON DELIVERY", "COD")
                                     .otherwise(F.col("payment_mode")))
        .withColumn("order_date", F.to_date("order_placed_ts"))
        .withColumn("order_hour", F.hour("order_placed_ts"))
        .withColumn(
            "delivery_minutes",
            F.round((F.unix_timestamp("delivered_ts") - F.unix_timestamp("order_placed_ts")) / 60, 1),
        )
        .withColumn("is_late", F.col("delivery_minutes") > F.col("promised_minutes"))
        .withColumn("net_amount", F.round(F.col("order_amount") - F.col("discount_amount"), 2))
        .select(
            "order_id", "store_id", "rider_id", "customer_id",
            "order_placed_ts", "delivered_ts", "order_date", "order_hour",
            "order_status", "payment_mode", "promised_minutes", "delivery_minutes", "is_late",
            "item_count", "order_amount", "discount_amount", "net_amount",
        )
)

silver_orders.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_orders")
display(spark.table("silver_orders").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC > 💡 `delivery_minutes` is **null** for cancelled orders — there is no delivery time,
# MAGIC > so `is_late` is null too. That is deliberate, and it makes the Gold layer honest.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — `silver_stores`
# MAGIC
# MAGIC One city was written five different ways. Fix it once, here, and every
# MAGIC downstream table inherits the fix.

# COMMAND ----------

silver_stores = (
    spark.table("bronze_dark_stores")
        .withColumn("city", F.initcap(F.trim("city")))
        .withColumn("area", F.trim("area"))
        .select("store_id", "store_name", "city", "area", "pincode", "opened_date")
)

silver_stores.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_stores")
display(spark.table("silver_stores"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — `silver_riders`
# MAGIC
# MAGIC Attach the city to every rider, so nothing downstream has to join two tables.

# COMMAND ----------

silver_riders = (
    spark.table("bronze_riders").alias("r")
        .join(spark.table("silver_stores").alias("s"), "store_id", "left")
        .select(
            "r.rider_id", "r.rider_name", "r.store_id", "s.city",
            "r.vehicle_type", "r.joining_date", "r.is_active",
        )
)

silver_riders.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_riders")
display(spark.table("silver_riders").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — `silver_order_items`
# MAGIC
# MAGIC Enrich every order line with its product details and its parent order's context.

# COMMAND ----------

silver_order_items = (
    spark.table("bronze_order_items").alias("i")
        .join(spark.table("bronze_products").alias("p"), "product_id", "inner")
        .join(spark.table("silver_orders").alias("o"), "order_id", "inner")
        .withColumn("line_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
        .select(
            "i.order_item_id", "i.order_id", "o.order_date", "o.order_status", "o.store_id",
            "i.product_id", "p.product_name", "p.category", "p.brand",
            "i.quantity", "i.unit_price", "line_amount",
        )
)

silver_order_items.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_order_items")
display(spark.table("silver_order_items").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Did the cleaning actually work?
# MAGIC
# MAGIC Run the same checks we ran on Bronze.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Seven spellings became two. Duplicates are gone.
# MAGIC SELECT order_status, count(*) AS orders, count(DISTINCT order_id) AS unique_orders
# MAGIC FROM silver_orders
# MAGIC GROUP BY order_status

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Five spellings of the cities became four real cities
# MAGIC SELECT city, count(*) AS stores
# MAGIC FROM silver_stores
# MAGIC GROUP BY city
# MAGIC ORDER BY city

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Silver is done
# MAGIC
# MAGIC | Table | What changed |
# MAGIC |---|---|
# MAGIC | `silver_orders` | deduplicated, standardised, + 5 derived columns |
# MAGIC | `silver_stores` | city names conformed |
# MAGIC | `silver_riders` | city attached to each rider |
# MAGIC | `silver_order_items` | product and order context joined in |
# MAGIC
# MAGIC The data is now trustworthy — but it still does not answer a single business question.
# MAGIC That is Gold's job.
# MAGIC
# MAGIC ➡️ **Next: open `03_gold_layer`.**