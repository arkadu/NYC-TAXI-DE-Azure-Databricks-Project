# Gold: Daily Borough Metrics - Revenue and Trip Aggregations by Borough
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from transformations._config import GOLD_TABLE_PROPERTIES


@dp.materialized_view(
    name="nyctaxi_databricks.gold.daily_borough_metrics",
    comment="Daily trip metrics aggregated by borough and taxi type (yellow, green, fhvheavy with revenue)",
    table_properties=GOLD_TABLE_PROPERTIES,
    cluster_by=["trip_date", "borough"],
    schema="""
        trip_date DATE,
        borough STRING,
        taxi_type STRING,
        trip_count BIGINT,
        total_revenue DOUBLE,
        avg_distance DOUBLE,
        CONSTRAINT daily_borough_metrics_pk PRIMARY KEY (trip_date, borough, taxi_type)
    """,
)
def daily_borough_metrics():
    yellow = (
        spark.read.table("nyctaxi_databricks.silver.yellow_trips")
        .select(
            F.col("pickup_datetime").cast("date").alias("trip_date"),
            F.col("pickup_location_id"),
            F.col("taxi_type"),
            F.col("total_amount").alias("revenue"),
            F.col("trip_distance"),
        )
    )
    green = (
        spark.read.table("nyctaxi_databricks.silver.green_trips")
        .select(
            F.col("pickup_datetime").cast("date").alias("trip_date"),
            F.col("pickup_location_id"),
            F.col("taxi_type"),
            F.col("total_amount").alias("revenue"),
            F.col("trip_distance"),
        )
    )
    fhvheavy = (
        spark.read.table("nyctaxi_databricks.silver.fhvheavy_trips")
        .select(
            F.col("pickup_datetime").cast("date").alias("trip_date"),
            F.col("pickup_location_id"),
            F.col("taxi_type"),
            F.col("base_passenger_fare").alias("revenue"),
            F.col("trip_miles").alias("trip_distance"),
        )
    )
    zones = spark.read.table("nyctaxi_databricks.silver.zone_lookup")

    return (
        yellow.unionByName(green).unionByName(fhvheavy)
        .join(zones, F.col("pickup_location_id") == zones.location_id, "left")
        .filter(F.col("borough").isNotNull())
        .groupBy("trip_date", "borough", "taxi_type")
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("revenue").alias("total_revenue"),
            F.avg("trip_distance").alias("avg_distance"),
        )
    )
