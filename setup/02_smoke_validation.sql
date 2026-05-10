-- Run after a successful pipeline update.

SELECT 'bronze.yellow_trips_raw' AS table_name, COUNT(*) AS row_count
FROM nyctaxi_databricks.bronze.yellow_trips_raw
UNION ALL
SELECT 'silver.yellow_trips', COUNT(*)
FROM nyctaxi_databricks.silver.yellow_trips
UNION ALL
SELECT 'gold.daily_borough_metrics', COUNT(*)
FROM nyctaxi_databricks.gold.daily_borough_metrics
UNION ALL
SELECT 'silver.quarantine_trips', COUNT(*)
FROM nyctaxi_databricks.silver.quarantine_trips;

SELECT *
FROM nyctaxi_databricks.gold.daily_borough_metrics
ORDER BY trip_date DESC, trip_count DESC
LIMIT 20;

