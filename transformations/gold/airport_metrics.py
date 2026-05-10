# Gold: Airport Metrics - JFK, LaGuardia, Newark Trip Analysis
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from transformations._config import GOLD_TABLE_PROPERTIES

# Airport location IDs
JFK_LOCATION_ID = 132
LAGUARDIA_LOCATION_ID = 138
NEWARK_LOCATION_ID = 1


@dp.materialized_view(
    name="nyctaxi_databricks.gold.airport_metrics",
    comment="Daily airport trip metrics for JFK, LaGuardia, and Newark airports",
    table_properties=GOLD_TABLE_PROPERTIES,
    cluster_by=["trip_date", "taxi_type"],
    schema="""
        trip_date DATE,
        taxi_type STRING,
        jfk_pickups BIGINT,
        jfk_dropoffs BIGINT,
        laguardia_pickups BIGINT,
        laguardia_dropoffs BIGINT,
        newark_pickups BIGINT,
        newark_dropoffs BIGINT,
        total_airport_trips BIGINT,
        CONSTRAINT airport_metrics_pk PRIMARY KEY (trip_date, taxi_type)
    """,
)
def airport_metrics():
    yellow = (
        spark.read.table("nyctaxi_databricks.silver.yellow_trips")
        .select("pickup_datetime", "pickup_location_id", "dropoff_location_id", "taxi_type")
    )
    green = (
        spark.read.table("nyctaxi_databricks.silver.green_trips")
        .select("pickup_datetime", "pickup_location_id", "dropoff_location_id", "taxi_type")
    )
    fhv = (
        spark.read.table("nyctaxi_databricks.silver.fhv_trips")
        .select("pickup_datetime", "pickup_location_id", "dropoff_location_id", "taxi_type")
    )
    fhvheavy = (
        spark.read.table("nyctaxi_databricks.silver.fhvheavy_trips")
        .select("pickup_datetime", "pickup_location_id", "dropoff_location_id", "taxi_type")
    )

    all_trips = yellow.unionByName(green).unionByName(fhv).unionByName(fhvheavy)

    airport_ids = [JFK_LOCATION_ID, LAGUARDIA_LOCATION_ID, NEWARK_LOCATION_ID]
    airport_trips = all_trips.filter(
        F.col("pickup_location_id").isin(airport_ids)
        | F.col("dropoff_location_id").isin(airport_ids)
    )

    return (
        airport_trips
        .withColumn("trip_date", F.col("pickup_datetime").cast("date"))
        .groupBy("trip_date", "taxi_type")
        .agg(
            F.sum(F.when(F.col("pickup_location_id") == JFK_LOCATION_ID, 1).otherwise(0)).alias("jfk_pickups"),
            F.sum(F.when(F.col("dropoff_location_id") == JFK_LOCATION_ID, 1).otherwise(0)).alias("jfk_dropoffs"),
            F.sum(F.when(F.col("pickup_location_id") == LAGUARDIA_LOCATION_ID, 1).otherwise(0)).alias("laguardia_pickups"),
            F.sum(F.when(F.col("dropoff_location_id") == LAGUARDIA_LOCATION_ID, 1).otherwise(0)).alias("laguardia_dropoffs"),
            F.sum(F.when(F.col("pickup_location_id") == NEWARK_LOCATION_ID, 1).otherwise(0)).alias("newark_pickups"),
            F.sum(F.when(F.col("dropoff_location_id") == NEWARK_LOCATION_ID, 1).otherwise(0)).alias("newark_dropoffs"),
            F.count("*").alias("total_airport_trips"),
        )
    )
