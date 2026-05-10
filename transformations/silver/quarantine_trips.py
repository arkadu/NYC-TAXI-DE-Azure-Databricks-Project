# Silver: Quarantine Table - Captures rows dropped by DQ rules for data steward review
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from transformations._config import (
    YELLOW_TRIPS_SOURCE,
    GREEN_TRIPS_SOURCE,
    FHV_TRIPS_SOURCE,
    FHVHEAVY_TRIPS_SOURCE,
)

# =============================================================================
# Quarantine Streaming Table - Target for all failed DQ rows
# =============================================================================
QUARANTINE_TABLE_PROPERTIES = {
    "delta.enableChangeDataFeed": "true",
    "quality_tier": "quarantine",
    "data_owner": "data-engineering-team",
    "retention_days": "90",
}

dp.create_streaming_table(
    name="nyctaxi_databricks.silver.quarantine_trips",
    comment="Rows that failed critical DQ rules from all taxi types, for data steward review",
    table_properties=QUARANTINE_TABLE_PROPERTIES,
    schema="pickup_datetime TIMESTAMP, dropoff_datetime TIMESTAMP, pickup_location_id LONG, dropoff_location_id LONG, taxi_type STRING, quarantine_reason STRING, _quarantined_at TIMESTAMP",
)


# =============================================================================
# Append Flow: Yellow Taxi DQ Failures
# =============================================================================
@dp.append_flow(target="nyctaxi_databricks.silver.quarantine_trips", name="quarantine_yellow")
def quarantine_yellow():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(YELLOW_TRIPS_SOURCE)
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        .withColumnRenamed("PULocationID", "pickup_location_id")
        .withColumnRenamed("DOLocationID", "dropoff_location_id")
        .withColumn("taxi_type", F.lit("yellow"))
        .withColumn("_quarantined_at", F.current_timestamp())
        .withColumn(
            "quarantine_reason",
            F.concat_ws(", ",
                F.when(F.col("pickup_datetime").isNull(), F.lit("null_pickup_datetime")),
                F.when(F.col("dropoff_datetime").isNull(), F.lit("null_dropoff_datetime")),
                F.when(~F.col("pickup_location_id").between(1, 265), F.lit("invalid_pickup_location")),
                F.when(~F.col("dropoff_location_id").between(1, 265), F.lit("invalid_dropoff_location")),
                F.when(F.col("fare_amount") < 0, F.lit("negative_fare")),
                F.when(F.col("fare_amount") >= 10000, F.lit("excessive_fare")),
                F.when(F.col("total_amount") < 0, F.lit("negative_total")),
                F.when(F.col("total_amount") >= 50000, F.lit("excessive_total")),
            ),
        )
        .filter(F.col("quarantine_reason") != "")
        .select(
            F.col("pickup_datetime").cast("timestamp"),
            F.col("dropoff_datetime").cast("timestamp"),
            F.col("pickup_location_id").cast("long"),
            F.col("dropoff_location_id").cast("long"),
            "taxi_type", "quarantine_reason", "_quarantined_at",
        )
    )


# =============================================================================
# Append Flow: Green Taxi DQ Failures
# =============================================================================
@dp.append_flow(target="nyctaxi_databricks.silver.quarantine_trips", name="quarantine_green")
def quarantine_green():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(GREEN_TRIPS_SOURCE)
        .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")
        .withColumnRenamed("PULocationID", "pickup_location_id")
        .withColumnRenamed("DOLocationID", "dropoff_location_id")
        .withColumn("taxi_type", F.lit("green"))
        .withColumn("_quarantined_at", F.current_timestamp())
        .withColumn(
            "quarantine_reason",
            F.concat_ws(", ",
                F.when(F.col("pickup_datetime").isNull(), F.lit("null_pickup_datetime")),
                F.when(F.col("dropoff_datetime").isNull(), F.lit("null_dropoff_datetime")),
                F.when(~F.col("pickup_location_id").between(1, 265), F.lit("invalid_pickup_location")),
                F.when(~F.col("dropoff_location_id").between(1, 265), F.lit("invalid_dropoff_location")),
                F.when(F.col("fare_amount") < 0, F.lit("negative_fare")),
                F.when(F.col("fare_amount") >= 10000, F.lit("excessive_fare")),
                F.when(F.col("total_amount") < 0, F.lit("negative_total")),
                F.when(F.col("total_amount") >= 50000, F.lit("excessive_total")),
            ),
        )
        .filter(F.col("quarantine_reason") != "")
        .select(
            F.col("pickup_datetime").cast("timestamp"),
            F.col("dropoff_datetime").cast("timestamp"),
            F.col("pickup_location_id").cast("long"),
            F.col("dropoff_location_id").cast("long"),
            "taxi_type", "quarantine_reason", "_quarantined_at",
        )
    )


# =============================================================================
# Append Flow: FHV DQ Failures
# =============================================================================
@dp.append_flow(target="nyctaxi_databricks.silver.quarantine_trips", name="quarantine_fhv")
def quarantine_fhv():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(FHV_TRIPS_SOURCE)
        .withColumnRenamed("dropOff_datetime", "dropoff_datetime")
        .withColumnRenamed("PUlocationID", "pickup_location_id")
        .withColumnRenamed("DOlocationID", "dropoff_location_id")
        .withColumn("taxi_type", F.lit("fhv"))
        .withColumn("_quarantined_at", F.current_timestamp())
        .withColumn(
            "quarantine_reason",
            F.concat_ws(", ",
                F.when(F.col("pickup_datetime").isNull(), F.lit("null_pickup_datetime")),
                F.when(F.col("dropoff_datetime").isNull(), F.lit("null_dropoff_datetime")),
                F.when(~F.col("pickup_location_id").between(1, 265), F.lit("invalid_pickup_location")),
                F.when(~F.col("dropoff_location_id").between(1, 265), F.lit("invalid_dropoff_location")),
            ),
        )
        .filter(F.col("quarantine_reason") != "")
        .select(
            F.col("pickup_datetime").cast("timestamp"),
            F.col("dropoff_datetime").cast("timestamp"),
            F.col("pickup_location_id").cast("long"),
            F.col("dropoff_location_id").cast("long"),
            "taxi_type", "quarantine_reason", "_quarantined_at",
        )
    )


# =============================================================================
# Append Flow: FHV Heavy DQ Failures
# =============================================================================
@dp.append_flow(target="nyctaxi_databricks.silver.quarantine_trips", name="quarantine_fhvheavy")
def quarantine_fhvheavy():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(FHVHEAVY_TRIPS_SOURCE)
        .withColumnRenamed("PULocationID", "pickup_location_id")
        .withColumnRenamed("DOLocationID", "dropoff_location_id")
        .withColumn("taxi_type", F.lit("fhvheavy"))
        .withColumn("_quarantined_at", F.current_timestamp())
        .withColumn(
            "quarantine_reason",
            F.concat_ws(", ",
                F.when(F.col("pickup_datetime").isNull(), F.lit("null_pickup_datetime")),
                F.when(F.col("dropoff_datetime").isNull(), F.lit("null_dropoff_datetime")),
                F.when(~F.col("pickup_location_id").between(1, 265), F.lit("invalid_pickup_location")),
                F.when(~F.col("dropoff_location_id").between(1, 265), F.lit("invalid_dropoff_location")),
                F.when(F.col("base_passenger_fare") < 0, F.lit("negative_base_fare")),
                F.when(F.col("base_passenger_fare") >= 10000, F.lit("excessive_base_fare")),
            ),
        )
        .filter(F.col("quarantine_reason") != "")
        .select(
            F.col("pickup_datetime").cast("timestamp"),
            F.col("dropoff_datetime").cast("timestamp"),
            F.col("pickup_location_id").cast("long"),
            F.col("dropoff_location_id").cast("long"),
            "taxi_type", "quarantine_reason", "_quarantined_at",
        )
    )
