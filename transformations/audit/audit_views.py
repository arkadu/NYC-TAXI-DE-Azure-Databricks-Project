# Audit: Pipeline Observability Layer - Run Logs, DQ, SLA Monitoring
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from transformations._config import AUDIT_TABLE_PROPERTIES


# =============================================================================
# Shared Temporary View - DRY principle for event_log access
# =============================================================================
@dp.temporary_view()
def event_log_base():
    return spark.sql(
        "SELECT * FROM event_log(TABLE(nyctaxi_databricks.silver.yellow_trips))"
    )


# =============================================================================
# 1. Pipeline Run Log
# =============================================================================
@dp.materialized_view(
    name="nyctaxi_databricks.audit.pipeline_run_log",
    comment="Pipeline update execution history with duration and state tracking",
    table_properties=AUDIT_TABLE_PROPERTIES,
)
def pipeline_run_log():
    return (
        spark.read.table("event_log_base")
        .filter(F.col("event_type") == "update_progress")
        .select(
            F.col("id").alias("event_id"),
            F.col("timestamp"),
            F.get_json_object("details", "$.update_id").alias("update_id"),
            F.get_json_object("details", "$.state").alias("state"),
            F.get_json_object("details", "$.run_type").alias("run_type"),
            F.get_json_object("details", "$.cause").alias("cause"),
            F.get_json_object("details", "$.creation_time").alias("creation_time"),
            F.round(
                F.get_json_object("details", "$.elapsed_seconds").cast("double"), 2
            ).alias("elapsed_seconds"),
            F.round(
                F.get_json_object("details", "$.elapsed_seconds").cast("double") / 60, 2
            ).alias("elapsed_minutes"),
            F.when(
                F.get_json_object("details", "$.state").isin("COMPLETED", "FAILED", "CANCELED"), True
            ).otherwise(False).alias("is_terminal_state"),
        )
    )


# =============================================================================
# 2. Data Quality Log
# =============================================================================
@dp.materialized_view(
    name="nyctaxi_databricks.audit.data_quality_log",
    comment="Data quality expectation results per dataset per pipeline update",
    table_properties=AUDIT_TABLE_PROPERTIES,
)
def data_quality_log():
    return (
        spark.read.table("event_log_base")
        .filter(F.col("event_type") == "flow_progress")
        .filter(F.get_json_object("details", "$.data_quality") != "null")
        .select(
            F.col("id").alias("event_id"),
            F.col("timestamp"),
            F.get_json_object("details", "$.flow_name").alias("dataset_name"),
            F.explode(
                F.from_json(
                    F.get_json_object("details", "$.data_quality.expectations"),
                    "ARRAY<STRUCT<name: STRING, dataset: STRING, passed_records: BIGINT, failed_records: BIGINT>>",
                )
            ).alias("expectation"),
        )
        .select(
            "event_id",
            "timestamp",
            "dataset_name",
            F.col("expectation.name").alias("expectation_name"),
            F.col("expectation.passed_records").alias("passed_records"),
            F.col("expectation.failed_records").alias("failed_records"),
            F.round(
                F.col("expectation.passed_records")
                / (F.col("expectation.passed_records") + F.col("expectation.failed_records"))
                * 100,
                2,
            ).alias("pass_rate_pct"),
            F.when(F.col("dataset_name").contains("bronze"), "bronze")
            .when(F.col("dataset_name").contains("silver"), "silver")
            .when(F.col("dataset_name").contains("gold"), "gold")
            .otherwise("other").alias("data_layer"),
        )
    )


# =============================================================================
# 3. Row Count Log with Throughput
# =============================================================================
@dp.materialized_view(
    name="nyctaxi_databricks.audit.row_count_log",
    comment="Row counts per dataset per update with throughput metrics",
    table_properties=AUDIT_TABLE_PROPERTIES,
)
def row_count_log():
    return (
        spark.read.table("event_log_base")
        .filter(F.col("event_type") == "flow_progress")
        .filter(F.get_json_object("details", "$.num_output_rows").isNotNull())
        .select(
            F.col("id").alias("event_id"),
            F.col("timestamp"),
            F.get_json_object("details", "$.flow_name").alias("dataset_name"),
            F.get_json_object("details", "$.num_output_rows").cast("long").alias("num_output_rows"),
            F.get_json_object("details", "$.metrics.seconds_of_processing").cast("double").alias("processing_seconds"),
            F.when(F.get_json_object("details", "$.flow_name").contains("bronze"), "bronze")
            .when(F.get_json_object("details", "$.flow_name").contains("silver"), "silver")
            .when(F.get_json_object("details", "$.flow_name").contains("gold"), "gold")
            .otherwise("other").alias("data_layer"),
        )
        .withColumn(
            "rows_per_second",
            F.when(F.col("processing_seconds") > 0, F.round(F.col("num_output_rows") / F.col("processing_seconds"), 2))
            .otherwise(None),
        )
    )


# =============================================================================
# 4. SLA Monitoring
# =============================================================================
@dp.materialized_view(
    name="nyctaxi_databricks.audit.sla_monitoring",
    comment="SLA compliance monitoring - tracks freshness of each dataset against thresholds",
    table_properties=AUDIT_TABLE_PROPERTIES,
)
def sla_monitoring():
    return (
        spark.read.table("event_log_base")
        .filter(F.col("event_type") == "flow_progress")
        .filter(F.get_json_object("details", "$.status") == "COMPLETED")
        .groupBy(F.get_json_object("details", "$.flow_name").alias("dataset_name"))
        .agg(F.max("timestamp").alias("last_successful_refresh"))
        .withColumn("hours_since_refresh", F.round((F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("last_successful_refresh")) / 3600, 2))
        .withColumn(
            "sla_threshold_hours",
            F.when(F.col("dataset_name").contains("bronze"), 6)
            .when(F.col("dataset_name").contains("silver"), 12)
            .when(F.col("dataset_name").contains("gold"), 24)
            .otherwise(24),
        )
        .withColumn("sla_breach", F.col("hours_since_refresh") > F.col("sla_threshold_hours"))
        .withColumn(
            "severity",
            F.when(F.col("hours_since_refresh") > F.col("sla_threshold_hours") * 2, "CRITICAL")
            .when(F.col("hours_since_refresh") > F.col("sla_threshold_hours"), "WARNING")
            .otherwise("OK"),
        )
    )


# =============================================================================
# 5. Error Log
# =============================================================================
@dp.materialized_view(
    name="nyctaxi_databricks.audit.error_log",
    comment="Pipeline error events with classification and severity assignment",
    table_properties=AUDIT_TABLE_PROPERTIES,
)
def error_log():
    return (
        spark.read.table("event_log_base")
        .filter(F.col("level") == "ERROR")
        .select(
            F.col("id").alias("event_id"),
            F.col("timestamp"),
            F.col("message"),
            F.col("details"),
            F.when(F.col("message").contains("schema"), "SCHEMA")
            .when(F.col("message").contains("permission"), "PERMISSION")
            .when(F.col("message").contains("resource"), "RESOURCE")
            .when(F.col("message").contains("data"), "DATA")
            .otherwise("OTHER").alias("error_category"),
            F.when(F.col("message").contains("FATAL") | F.col("message").contains("critical"), "CRITICAL")
            .when(F.col("message").contains("schema") | F.col("message").contains("permission"), "HIGH")
            .otherwise("LOW").alias("severity"),
        )
    )
