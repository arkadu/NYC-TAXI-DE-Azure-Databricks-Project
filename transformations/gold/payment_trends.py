# Gold: Payment Trends - Payment Method Analysis (Yellow + Green only)
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from transformations._config import GOLD_TABLE_PROPERTIES


@dp.materialized_view(
    name="nyctaxi_databricks.gold.payment_trends",
    comment="Payment method trends for yellow and green taxis (only types with payment_type data)",
    table_properties=GOLD_TABLE_PROPERTIES,
    cluster_by=["trip_date", "payment_type"],
    schema="""
        trip_date DATE,
        payment_type BIGINT,
        taxi_type STRING,
        trip_count BIGINT,
        total_revenue DOUBLE,
        avg_fare DOUBLE,
        CONSTRAINT payment_trends_pk PRIMARY KEY (trip_date, payment_type, taxi_type)
    """,
)
def payment_trends():
    yellow = (
        spark.read.table("nyctaxi_databricks.silver.yellow_trips")
        .select(
            F.col("pickup_datetime").cast("date").alias("trip_date"),
            "payment_type", "taxi_type", "total_amount", "fare_amount",
        )
    )
    green = (
        spark.read.table("nyctaxi_databricks.silver.green_trips")
        .select(
            F.col("pickup_datetime").cast("date").alias("trip_date"),
            "payment_type", "taxi_type", "total_amount", "fare_amount",
        )
    )

    return (
        yellow.unionByName(green)
        .filter(F.col("payment_type").isNotNull())
        .filter(F.col("trip_date").isNotNull())
        .groupBy("trip_date", "payment_type", "taxi_type")
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("fare_amount").alias("avg_fare"),
        )
    )
