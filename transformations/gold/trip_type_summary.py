# Gold: Trip Type Summary - Street-hail vs Dispatch Analysis (Green Taxi only)
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from transformations._config import GOLD_TABLE_PROPERTIES


@dp.materialized_view(
    name="nyctaxi_databricks.gold.trip_type_summary",
    comment="Trip type breakdown (street-hail vs dispatch) for green taxis by date and borough",
    table_properties=GOLD_TABLE_PROPERTIES,
    cluster_by=["trip_date", "borough"],
    schema="""
        trip_date DATE,
        borough STRING,
        trip_type BIGINT,
        trip_count BIGINT,
        trip_type_desc STRING,
        CONSTRAINT trip_type_summary_pk PRIMARY KEY (trip_date, borough, trip_type)
    """,
)
def trip_type_summary():
    green = spark.read.table("nyctaxi_databricks.silver.green_trips")
    zones = spark.read.table("nyctaxi_databricks.silver.zone_lookup")

    return (
        green
        .filter(F.col("trip_type").isNotNull())
        .withColumn("trip_date", F.col("pickup_datetime").cast("date"))
        .join(zones, F.col("pickup_location_id") == zones.location_id, "left")
        .filter(F.col("borough").isNotNull())
        .groupBy("trip_date", "borough", "trip_type")
        .agg(F.count("*").alias("trip_count"))
        .withColumn(
            "trip_type_desc",
            F.when(F.col("trip_type") == 1, "Street-hail")
            .when(F.col("trip_type") == 2, "Dispatch")
            .otherwise("Unknown"),
        )
    )
