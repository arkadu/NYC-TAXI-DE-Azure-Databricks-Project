# Databricks notebook source
# DBTITLE 1,NYC Taxi Medallion Pipeline - Monthly Table Maintenance
# MAGIC %md
# MAGIC # 🔧 NYC Taxi Medallion Pipeline - Monthly Table Maintenance
# MAGIC
# MAGIC This notebook runs **OPTIMIZE** (file compaction) and **VACUUM** (stale file cleanup) on all pipeline tables.
# MAGIC
# MAGIC Scheduled to run on the **2nd of every month at 2:00 AM IST**, one day after the pipeline update.

# COMMAND ----------

# DBTITLE 1,OPTIMIZE Bronze Tables
# MAGIC %sql
# MAGIC OPTIMIZE nyctaxi_databricks.bronze.yellow_trips_raw;
# MAGIC OPTIMIZE nyctaxi_databricks.bronze.green_trips_raw;
# MAGIC OPTIMIZE nyctaxi_databricks.bronze.fhv_trips_raw;
# MAGIC OPTIMIZE nyctaxi_databricks.bronze.fhvheavy_trips_raw;

# COMMAND ----------

# DBTITLE 1,OPTIMIZE Silver Tables
# MAGIC %sql
# MAGIC OPTIMIZE nyctaxi_databricks.silver.yellow_trips;
# MAGIC OPTIMIZE nyctaxi_databricks.silver.green_trips;
# MAGIC OPTIMIZE nyctaxi_databricks.silver.fhv_trips;
# MAGIC OPTIMIZE nyctaxi_databricks.silver.fhvheavy_trips;
# MAGIC OPTIMIZE nyctaxi_databricks.silver.quarantine_trips;

# COMMAND ----------

# DBTITLE 1,OPTIMIZE Gold Tables
# MAGIC %sql
# MAGIC OPTIMIZE nyctaxi_databricks.gold.daily_borough_metrics;
# MAGIC OPTIMIZE nyctaxi_databricks.gold.hourly_demand;
# MAGIC OPTIMIZE nyctaxi_databricks.gold.airport_metrics;
# MAGIC OPTIMIZE nyctaxi_databricks.gold.zone_revenue;
# MAGIC OPTIMIZE nyctaxi_databricks.gold.payment_trends;
# MAGIC OPTIMIZE nyctaxi_databricks.gold.trip_type_summary;

# COMMAND ----------

# DBTITLE 1,VACUUM All Tables (7-day retention)
# MAGIC %sql
# MAGIC VACUUM nyctaxi_databricks.bronze.yellow_trips_raw RETAIN 168 HOURS;
# MAGIC VACUUM nyctaxi_databricks.bronze.green_trips_raw RETAIN 168 HOURS;
# MAGIC VACUUM nyctaxi_databricks.bronze.fhv_trips_raw RETAIN 168 HOURS;
# MAGIC VACUUM nyctaxi_databricks.bronze.fhvheavy_trips_raw RETAIN 168 HOURS;
# MAGIC VACUUM nyctaxi_databricks.silver.yellow_trips RETAIN 168 HOURS;
# MAGIC VACUUM nyctaxi_databricks.silver.green_trips RETAIN 168 HOURS;
# MAGIC VACUUM nyctaxi_databricks.silver.fhv_trips RETAIN 168 HOURS;
# MAGIC VACUUM nyctaxi_databricks.silver.fhvheavy_trips RETAIN 168 HOURS;
# MAGIC VACUUM nyctaxi_databricks.silver.quarantine_trips RETAIN 168 HOURS;
# MAGIC VACUUM nyctaxi_databricks.gold.daily_borough_metrics RETAIN 168 HOURS;
# MAGIC VACUUM nyctaxi_databricks.gold.hourly_demand RETAIN 168 HOURS;
# MAGIC VACUUM nyctaxi_databricks.gold.airport_metrics RETAIN 168 HOURS;
# MAGIC VACUUM nyctaxi_databricks.gold.zone_revenue RETAIN 168 HOURS;
# MAGIC VACUUM nyctaxi_databricks.gold.payment_trends RETAIN 168 HOURS;
# MAGIC VACUUM nyctaxi_databricks.gold.trip_type_summary RETAIN 168 HOURS;

# COMMAND ----------

# DBTITLE 1,Quarantine Retention - Purge rows older than 90 days
# MAGIC %sql
# MAGIC DELETE FROM nyctaxi_databricks.silver.quarantine_trips WHERE _quarantined_at < current_timestamp() - INTERVAL 90 DAYS;

# COMMAND ----------

# DBTITLE 1,Schedule & Configuration Notes
# MAGIC %md
# MAGIC ## Schedule & Configuration Notes
# MAGIC
# MAGIC **Schedule:** 2nd of every month at 2:00 AM IST (cron: `0 0 2 2 * ? *`)
# MAGIC
# MAGIC Runs 1 day after the pipeline monthly update to ensure fresh data is compacted.
# MAGIC
# MAGIC **Cluster:** Single-node, smallest instance type.
# MAGIC
# MAGIC **Retention:**
# MAGIC - VACUUM removes files older than 7 days
# MAGIC - Quarantine purges records older than 90 days