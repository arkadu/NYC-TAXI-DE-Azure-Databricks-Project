# Gold: Zone Revenue - Revenue Metrics by Pickup Zone
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from transformations._config import GOLD_TABLE_PROPERTIES


@dp.materialized_view(
    name="nyctaxi_databricks.gold.zone_revenue",
    comment="Revenue metrics aggregated by pickup zone, borough, and taxi type",
    table_properties=GOLD_TABLE_PROPERTIES,
    cluster_by=["borough", "taxi_type"],
    schema="""
        pickup_location_id INT,
        zone_name STRING,
        borough STRING,
        taxi_type STRING,
        trip_count BIGINT,
        total_revenue DOUBLE,
        avg_revenue DOUBLE,
        min_revenue DOUBLE,
        max_revenue DOUBLE,
        CONSTRAINT zone_revenue_pk PRIMARY KEY (pickup_location_id, taxi_type)
    """,
)
def zone_revenue():
    yellow = (
        spark.read.table("nyctaxi_databricks.silver.yellow_trips")
        .select("pickup_location_id", "taxi_type", F.col("total_amount").alias("revenue"))
    )
    green = (
        spark.read.table("nyctaxi_databricks.silver.green_trips")
        .select("pickup_location_id", "taxi_type", F.col("total_amount").alias("revenue"))
    )
    fhvheavy = (
        spark.read.table("nyctaxi_databricks.silver.fhvheavy_trips")
        .select("pickup_location_id", "taxi_type", F.col("base_passenger_fare").alias("revenue"))
    )
    zones = spark.read.table("nyctaxi_databricks.silver.zone_lookup")

    all_revenue_trips = yellow.unionByName(green).unionByName(fhvheavy)

    return (
        all_revenue_trips
        .join(zones, F.col("pickup_location_id") == zones.location_id, "left")
        .groupBy("pickup_location_id", "zone_name", "borough", "taxi_type")
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("revenue").alias("total_revenue"),
            F.avg("revenue").alias("avg_revenue"),
            F.min("revenue").alias("min_revenue"),
            F.max("revenue").alias("max_revenue"),
        )
    )
