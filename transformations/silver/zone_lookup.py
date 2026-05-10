# Silver: Zone Lookup - Standardized Reference Data
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from transformations._config import SILVER_TABLE_PROPERTIES


@dp.materialized_view(
    name="nyctaxi_databricks.silver.zone_lookup",
    comment="Standardized zone lookup with renamed columns for consistent downstream joins",
    table_properties=SILVER_TABLE_PROPERTIES,
)
@dp.expect_all_or_drop({
    "valid_location_id": "location_id IS NOT NULL",
    "valid_borough": "borough IS NOT NULL",
    "valid_zone_name": "zone_name IS NOT NULL",
})
def zone_lookup():
    return (
        spark.read.table("nyctaxi_databricks.bronze.zone_lookup_raw")
        .withColumnRenamed("LocationID", "location_id")
        .withColumnRenamed("Borough", "borough")
        .withColumnRenamed("Zone", "zone_name")
        .withColumn("_created_at", F.current_timestamp())
    )
