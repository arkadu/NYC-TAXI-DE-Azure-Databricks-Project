# Gold: Hourly Demand - Trip Count Patterns by Hour, Day, Borough, Taxi Type
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from transformations._config import GOLD_TABLE_PROPERTIES


@dp.materialized_view(
    name="nyctaxi_databricks.gold.hourly_demand",
    comment="Hourly trip demand patterns by hour of day, day of week, borough, and taxi type",
    table_properties=GOLD_TABLE_PROPERTIES,
    cluster_by=["hour_of_day", "day_of_week"],
    schema="""
        hour_of_day INT,
        day_of_week INT,
        borough STRING,
        taxi_type STRING,
        trip_count BIGINT,
        CONSTRAINT hourly_demand_pk PRIMARY KEY (hour_of_day, day_of_week, borough, taxi_type)
    """,
)
def hourly_demand():
    yellow = (
        spark.read.table("nyctaxi_databricks.silver.yellow_trips")
        .select("pickup_datetime", "pickup_location_id", "taxi_type")
    )
    green = (
        spark.read.table("nyctaxi_databricks.silver.green_trips")
        .select("pickup_datetime", "pickup_location_id", "taxi_type")
    )
    fhv = (
        spark.read.table("nyctaxi_databricks.silver.fhv_trips")
        .select("pickup_datetime", "pickup_location_id", "taxi_type")
    )
    fhvheavy = (
        spark.read.table("nyctaxi_databricks.silver.fhvheavy_trips")
        .select("pickup_datetime", "pickup_location_id", "taxi_type")
    )
    zones = spark.read.table("nyctaxi_databricks.silver.zone_lookup")

    all_trips = yellow.unionByName(green).unionByName(fhv).unionByName(fhvheavy)

    return (
        all_trips
        .withColumn("hour_of_day", F.hour("pickup_datetime"))
        .withColumn("day_of_week", F.dayofweek("pickup_datetime"))
        .join(zones, F.col("pickup_location_id") == zones.location_id, "left")
        .filter(F.col("borough").isNotNull())
        .groupBy("hour_of_day", "day_of_week", "borough", "taxi_type")
        .agg(F.count("*").alias("trip_count"))
    )
