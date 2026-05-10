# Bronze: Zone Lookup - Reference Data (Batch Read)
from pyspark import pipelines as dp
from transformations._config import ZONE_LOOKUP_SOURCE, BRONZE_TABLE_PROPERTIES


@dp.materialized_view(
    name="nyctaxi_databricks.bronze.zone_lookup_raw",
    comment="NYC TLC taxi zone lookup reference data (LocationID to Borough/Zone mapping)",
    table_properties=BRONZE_TABLE_PROPERTIES,
)
def zone_lookup_raw():
    return spark.read.format("parquet").load(ZONE_LOOKUP_SOURCE)
