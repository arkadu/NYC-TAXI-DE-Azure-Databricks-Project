# Bronze: Green Taxi Trips - Raw Ingestion via Auto Loader
from pyspark import pipelines as dp
from transformations._config import GREEN_TRIPS_SOURCE, BRONZE_TABLE_PROPERTIES


@dp.table(
    name="nyctaxi_databricks.bronze.green_trips_raw",
    comment="Raw green taxi trip records ingested via Auto Loader from ADLS parquet files",
    table_properties=BRONZE_TABLE_PROPERTIES,
    cluster_by=["lpep_pickup_datetime"],
)
@dp.expect("rescued_data_is_null", "_rescued_data IS NULL")
def green_trips_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(GREEN_TRIPS_SOURCE)
    )
