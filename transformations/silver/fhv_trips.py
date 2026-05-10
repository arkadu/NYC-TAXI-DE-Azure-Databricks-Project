# Silver: FHV Trips - Cleaned, Standardized, Deduplicated
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from transformations._config import SILVER_TABLE_PROPERTIES


@dp.table(
    name="nyctaxi_databricks.silver.fhv_trips",
    comment="Cleaned and deduplicated for-hire vehicle trips with standardized column names",
    table_properties=SILVER_TABLE_PROPERTIES,
    partition_cols=["taxi_type", "pickup_year", "pickup_month"],
)
# Critical DQ - drop invalid rows (no fare data for FHV)
@dp.expect_all_or_drop({
    "valid_pickup_datetime": "pickup_datetime IS NOT NULL",
    "valid_dropoff_datetime": "dropoff_datetime IS NOT NULL",
    "valid_pickup_location": "pickup_location_id BETWEEN 1 AND 265",
    "valid_dropoff_location": "dropoff_location_id BETWEEN 1 AND 265",
    "valid_trip_duration": "trip_duration_mins BETWEEN 0 AND 1440",
    "valid_date_range": "pickup_datetime >= '2024-01-01' AND pickup_datetime < '2027-01-01'",
})
# Monitoring DQ - keep rows but track metrics
@dp.expect_all({
    "valid_dispatching_base": "dispatching_base_num IS NOT NULL",
})
def fhv_trips():
    return (
        spark.readStream.table("nyctaxi_databricks.bronze.fhv_trips_raw")
        .withColumnRenamed("dropOff_datetime", "dropoff_datetime")
        .withColumnRenamed("PUlocationID", "pickup_location_id")
        .withColumnRenamed("DOlocationID", "dropoff_location_id")
        .withColumn("taxi_type", F.lit("fhv"))
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
            "dispatching_base_num",
        ])
        .drop("_event_time")
    )
