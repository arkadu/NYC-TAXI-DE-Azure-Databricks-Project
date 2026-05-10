# =============================================================================
# NYC Taxi Medallion Pipeline - Centralized Configuration
# Production External Tables - All data stored on ADLS Gen2
# =============================================================================
# IMPORTANT: For external storage, set MANAGED LOCATION on each schema:
#   ALTER SCHEMA nyctaxi_databricks.bronze SET MANAGED LOCATION 'abfss://bronze@<storage-account>.dfs.core.windows.net/tables';
#   ALTER SCHEMA nyctaxi_databricks.silver SET MANAGED LOCATION 'abfss://silver@<storage-account>.dfs.core.windows.net';
#   ALTER SCHEMA nyctaxi_databricks.gold   SET MANAGED LOCATION 'abfss://gold@<storage-account>.dfs.core.windows.net';
#   ALTER SCHEMA nyctaxi_databricks.audit  SET MANAGED LOCATION 'abfss://silver@<storage-account>.dfs.core.windows.net/_audit';
# =============================================================================

# Storage Account Configuration
#
# The bundle passes this value through pipeline configuration:
#   nyc_taxi.storage_account: ${var.storage_account_name}
# Local syntax checks and tests fall back to environment variables.
import os


def _setting(key: str, env_name: str, default: str) -> str:
    try:
        return spark.conf.get(key, default)  # noqa: F821
    except Exception:
        return os.getenv(env_name, default)


STORAGE_ACCOUNT = _setting("nyc_taxi.storage_account", "NYC_TAXI_STORAGE_ACCOUNT", "replace-me-storage-account")
BRONZE_BASE = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
SILVER_BASE = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
GOLD_BASE = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# Catalog Configuration
CATALOG = _setting("nyc_taxi.catalog", "NYC_TAXI_CATALOG", "nyctaxi_databricks")

# =============================================================================
# Table Properties by Layer
# =============================================================================

BRONZE_TABLE_PROPERTIES = {
    "delta.feature.timestampNtz": "supported",
    "delta.enableChangeDataFeed": "true",
    "quality_tier": "bronze",
    "data_owner": "data-engineering-team",
    "source_system": "nyc_tlc_adls",
}

SILVER_TABLE_PROPERTIES = {
    "delta.feature.timestampNtz": "supported",
    "delta.enableChangeDataFeed": "true",
    "quality_tier": "silver",
    "data_owner": "data-engineering-team",
    "source_system": "nyc_tlc_adls",
}

GOLD_TABLE_PROPERTIES = {
    "delta.enableChangeDataFeed": "true",
    "quality_tier": "gold",
    "data_owner": "analytics-team",
    "sla_freshness_hours": "24",
    "retention_days": "1095",
    "downstream_consumers": "dashboard,reporting",
}

AUDIT_TABLE_PROPERTIES = {
    "delta.enableChangeDataFeed": "true",
    "quality_tier": "audit",
    "data_owner": "data-engineering-team",
    "retention_days": "365",
}

# =============================================================================
# Source Data Paths (Auto Loader landing zones)
# =============================================================================

YELLOW_TRIPS_SOURCE = f"{BRONZE_BASE}/trip-data-yellow/trip-data"
GREEN_TRIPS_SOURCE = f"{BRONZE_BASE}/trip-data-green"
FHV_TRIPS_SOURCE = f"{BRONZE_BASE}/trip-data-fhv"
FHVHEAVY_TRIPS_SOURCE = f"{BRONZE_BASE}/trip-data-fhvheavy"
ZONE_LOOKUP_SOURCE = f"{BRONZE_BASE}/trip_zone"
