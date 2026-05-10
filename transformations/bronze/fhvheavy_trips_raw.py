# Bronze: FHV Heavy (HVFHS) Trips - Raw Ingestion via Auto Loader
from pyspark import pipelines as dp
from transformations._config import FHVHEAVY_TRIPS_SOURCE, BRONZE_TABLE_PROPERTIES


@dp.table(
    name="nyctaxi_databricks.bronze.fhvheavy_trips_raw",
    comment="Raw high-volume for-hire vehicle trip records (Uber/Lyft) via Auto Loader",
    table_properties=BRONZE_TABLE_PROPERTIES,
    cluster_by=["pickup_datetime"],
    spark_conf={
        "spark.sql.shuffle.partitions": "800",
        "spark.sql.adaptive.skewJoin.enabled": "true",
    },
)
@dp.expect("rescued_data_is_null", "_rescued_data IS NULL")
def fhvheavy_trips_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(FHVHEAVY_TRIPS_SOURCE)
    )
