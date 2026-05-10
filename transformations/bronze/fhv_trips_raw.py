# Bronze: FHV Trips - Raw Ingestion via Auto Loader
from pyspark import pipelines as dp
from transformations._config import FHV_TRIPS_SOURCE, BRONZE_TABLE_PROPERTIES


@dp.table(
    name="nyctaxi_databricks.bronze.fhv_trips_raw",
    comment="Raw for-hire vehicle trip records ingested via Auto Loader from ADLS parquet files",
    table_properties=BRONZE_TABLE_PROPERTIES,
    cluster_by=["pickup_datetime"],
)
@dp.expect("rescued_data_is_null", "_rescued_data IS NULL")
def fhv_trips_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(FHV_TRIPS_SOURCE)
    )
