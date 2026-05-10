# Silver: FHV Heavy (HVFHS) Trips - Cleaned, Standardized, Deduplicated
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from transformations._config import SILVER_TABLE_PROPERTIES


@dp.table(
    name="nyctaxi_databricks.silver.fhvheavy_trips",
    comment="Cleaned and deduplicated high-volume for-hire vehicle trips (Uber/Lyft)",
    table_properties=SILVER_TABLE_PROPERTIES,
    partition_cols=["pickup_year", "pickup_month"],
    spark_conf={
        "spark.sql.shuffle.partitions": "800",
        "spark.sql.adaptive.skewJoin.enabled": "true",
    },
)
# Critical DQ - drop invalid rows
@dp.expect_all_or_drop({
    "valid_pickup_datetime": "pickup_datetime IS NOT NULL",
    "valid_dropoff_datetime": "dropoff_datetime IS NOT NULL",
    "valid_pickup_location": "pickup_location_id BETWEEN 1 AND 265",
    "valid_dropoff_location": "dropoff_location_id BETWEEN 1 AND 265",
    "valid_trip_duration": "trip_duration_mins BETWEEN 0 AND 1440",
    "valid_date_range": "pickup_datetime >= '2024-01-01' AND pickup_datetime < '2027-01-01'",
    "valid_base_fare": "base_passenger_fare BETWEEN 0 AND 10000",
})
# Monitoring DQ - keep rows but track metrics
@dp.expect_all({
    "valid_trip_miles": "trip_miles BETWEEN 0 AND 500",
    "valid_driver_pay": "driver_pay >= 0",
    "valid_tips": "tips >= 0",
})
def fhvheavy_trips():
    return (
        spark.readStream.table("nyctaxi_databricks.bronze.fhvheavy_trips_raw")
        .withColumnRenamed("PULocationID", "pickup_location_id")
        .withColumnRenamed("DOLocationID", "dropoff_location_id")
        .withColumn("taxi_type", F.lit("fhvheavy"))
        .withColumn("pickup_year", F.year("pickup_datetime"))
        .withColumn("pickup_month", F.month("pickup_datetime"))
        .withColumn(
            "trip_duration_mins",
            F.round(
                (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60,
                2,
            ),
        )
        .withColumn("_created_at", F.current_timestamp())
        .withColumn("_event_time", F.col("pickup_datetime").cast("timestamp"))
        .withWatermark("_event_time", "24 hours")
        .dropDuplicatesWithinWatermark([
            "pickup_datetime",
            "pickup_location_id",
            "dropoff_location_id",
            "hvfhs_license_num",
        ])
        .drop("_event_time")
    )
