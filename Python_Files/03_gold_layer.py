# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Notebook 3 of 3 — GOLD
# MAGIC ### AtliQ Mart · 10-minute grocery delivery
# MAGIC
# MAGIC **Rule of Gold: answer the question.**
# MAGIC Small, aggregated tables, each **named after the decision it supports**.
# MAGIC
# MAGIC The COO asked three questions. We build one table per question.
# MAGIC
# MAGIC | # | Question | Gold table |
# MAGIC |---|----------|------------|
# MAGIC | 1 | Which city is breaking the 10-minute promise? | `gold_city_performance` |
# MAGIC | 2 | Who are my best and worst riders? | `gold_rider_scorecard` |
# MAGIC | 3 | Which categories actually make the money? | `gold_category_sales` |
# MAGIC
# MAGIC ```
# MAGIC   /Volumes  →  🥉 BRONZE  →  🥈 SILVER  →  🥇 GOLD  →  📊 Dashboard
# MAGIC                                          you are here
# MAGIC ```
# MAGIC
# MAGIC ⚠️ Run `01_bronze_layer` and `02_silver_layer` first.

# COMMAND ----------

CATALOG = "workspace"
SCHEMA  = "quickcommerce"

spark.sql(f"USE {CATALOG}.{SCHEMA}")

from pyspark.sql import functions as F

# on_time_pct, used by two Gold tables.
# is_late is NULL for cancelled orders, so avg() quietly ignores them — meaning
# on-time is measured only across orders we actually delivered.
on_time_pct = F.round(
    100 * F.avg(F.when(F.col("is_late") == False, 1.0)
                 .when(F.col("is_late") == True, 0.0)), 2
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 1 — Which city is breaking the promise?

# COMMAND ----------

gold_city_performance = (
    spark.table("silver_orders").alias("o")
        .join(spark.table("silver_stores").alias("s"), "store_id", "inner")
        .groupBy("order_date", "city")
        .agg(
            F.count("*").alias("total_orders"),
            F.sum(F.when(F.col("order_status") == "DELIVERED", 1).otherwise(0)).alias("delivered_orders"),
            F.sum(F.when(F.col("order_status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_orders"),
            F.round(F.avg("delivery_minutes"), 2).alias("avg_delivery_minutes"),
            on_time_pct.alias("on_time_pct"),
            F.round(F.sum("net_amount"), 2).alias("revenue"),
        )
)

gold_city_performance.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_city_performance")
display(spark.table("gold_city_performance").orderBy("order_date", "city").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 2 — Who are my best and worst riders?

# COMMAND ----------

gold_rider_scorecard = (
    spark.table("silver_orders").alias("o")
        .filter((F.col("order_status") == "DELIVERED") & F.col("rider_id").isNotNull())
        .join(spark.table("silver_riders").alias("r"), "rider_id", "inner")
        .groupBy("rider_id", "rider_name", "city", "vehicle_type")
        .agg(
            F.count("*").alias("deliveries"),
            F.round(F.avg("delivery_minutes"), 2).alias("avg_delivery_minutes"),
            on_time_pct.alias("on_time_pct"),
            F.round(F.sum("net_amount"), 2).alias("revenue_delivered"),
        )
        .filter(F.col("deliveries") >= 20)      # too few deliveries to judge fairly
)

gold_rider_scorecard.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_rider_scorecard")
display(spark.table("gold_rider_scorecard").orderBy(F.desc("on_time_pct")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 3 — Which categories make the money?

# COMMAND ----------

gold_category_sales = (
    spark.table("silver_order_items")
        .filter(F.col("order_status") == "DELIVERED")
        .groupBy("category")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("line_amount"), 2).alias("revenue"),
        )
)

gold_category_sales.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_category_sales")
display(spark.table("gold_category_sales").orderBy(F.desc("revenue")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Answer the COO
# MAGIC
# MAGIC The pipeline is done. From here it is just SQL.
# MAGIC
# MAGIC > 💡 After running each cell, click **+ → Visualization** under the result to chart it.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q1: Which city is breaking the 10-minute promise?
# MAGIC -- Chart: Bar — X = city, Y = on_time_pct
# MAGIC SELECT
# MAGIC   city,
# MAGIC   sum(total_orders)                   AS orders,
# MAGIC   round(avg(avg_delivery_minutes), 2) AS avg_delivery_minutes,
# MAGIC   round(avg(on_time_pct), 2)          AS on_time_pct
# MAGIC FROM gold_city_performance
# MAGIC GROUP BY city
# MAGIC ORDER BY on_time_pct ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q1b: Is it getting better or worse? Daily trend per city.
# MAGIC -- Chart: Line — X = order_date, Y = on_time_pct, Group by = city
# MAGIC SELECT order_date, city, on_time_pct
# MAGIC FROM gold_city_performance
# MAGIC ORDER BY order_date

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q2: The 10 riders who need coaching
# MAGIC SELECT rider_name, city, vehicle_type, deliveries, avg_delivery_minutes, on_time_pct
# MAGIC FROM gold_rider_scorecard
# MAGIC ORDER BY on_time_pct ASC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC > 🔍 **Look closely.** Every one of the worst riders is in the same city.
# MAGIC > Is it really the riders — or is it the city? This is why a pipeline beats a single query.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q3: Which categories make the money?
# MAGIC -- Chart: Bar — X = category, Y = revenue
# MAGIC SELECT category, orders, units_sold, revenue
# MAGIC FROM gold_category_sales
# MAGIC ORDER BY revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC > 🔍 The most-ordered category is nowhere near the top on revenue. **Volume is not value.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Delta superpower we got for free
# MAGIC
# MAGIC Every table we created is a **Delta table**, so every write was versioned.
# MAGIC We never asked for this — it just happened.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY silver_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Time travel: read the table exactly as it was at version 0
# MAGIC SELECT count(*) AS rows_at_version_0
# MAGIC FROM silver_orders VERSION AS OF 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ You just built an end-to-end pipeline
# MAGIC
# MAGIC | Layer | Tables | What it gave us |
# MAGIC |---|---|---|
# MAGIC | 🥉 Bronze | 5 | a faithful, replayable copy of the source |
# MAGIC | 🥈 Silver | 4 | clean, deduplicated, business-ready facts |
# MAGIC | 🥇 Gold | 3 | answers, not data |
# MAGIC
# MAGIC **None of those three answers existed as a column in the raw files. You built them.**
# MAGIC
# MAGIC ### Where to go next
# MAGIC 1. Schedule these notebooks as a **Job** (Workflows → Create Job) so they run every morning
# MAGIC 2. Swap the Bronze read for **Auto Loader** so new files are picked up automatically
# MAGIC 3. Build a **Dashboard** on the Gold tables and share it with the business
# MAGIC 4. Add **data quality expectations** so bad data never reaches Gold