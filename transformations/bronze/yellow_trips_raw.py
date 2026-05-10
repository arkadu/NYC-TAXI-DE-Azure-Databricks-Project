# Bronze: Yellow Taxi Trips - Raw Ingestion via Auto Loader
from pyspark import pipelines as dp
from transformations._config import YELLOW_TRIPS_SOURCE, BRONZE_TABLE_PROPERTIES


@dp.table(
    name="nyctaxi_databricks.bronze.yellow_trips_raw",
    comment="Raw yellow taxi trip records ingested via Auto Loader from ADLS parquet files",
    table_properties=BRONZE_TABLE_PROPERTIES,
    cluster_by=["tpep_pickup_datetime"],
)
@dp.expect("rescued_data_is_null", "_rescued_data IS NULL")
def yellow_trips_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(YELLOW_TRIPS_SOURCE)
    )
