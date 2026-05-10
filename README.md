# NYC Taxi Azure Databricks Medallion Pipeline

Portfolio-grade data engineering project that processes NYC Taxi data through a Databricks Lakeflow Declarative Pipeline on Azure. The project demonstrates a realistic medallion architecture: raw ingestion, silver data quality rules, quarantine handling, gold analytics marts, audit views, dashboard assets, and scheduled maintenance.

This is intentionally framed as a Databricks transformation and analytics pipeline. It assumes the NYC TLC parquet files have already been landed in ADLS Gen2.

## Architecture

```text
NYC TLC parquet files in ADLS Gen2
        |
        v
Bronze streaming tables
  - Auto Loader over parquet files
  - Schema evolution enabled
  - Raw append-only tables
        |
        v
Silver streaming tables
  - Standardized column names
  - Data quality expectations
  - Deduplication with watermarks
  - Quarantine table for failed records
        |
        v
Gold materialized views
  - Borough daily metrics
  - Hourly demand
  - Airport metrics
  - Zone revenue
  - Payment trends
  - Trip type summary
        |
        v
Lakeview dashboard and audit views
```

## What This Proves

- Databricks Lakeflow Declarative Pipelines with Python source files.
- Bronze/silver/gold medallion modeling.
- ADLS Gen2-backed external Delta storage.
- Auto Loader ingestion from parquet landing zones.
- Data quality enforcement using `expect_all_or_drop` and `expect_all`.
- Separate quarantine stream for data steward review.
- BI-ready gold materialized views.
- Audit views based on pipeline event logs.
- Monthly maintenance job for `OPTIMIZE`, `VACUUM`, and quarantine retention.
- Databricks Asset Bundle deployment config.

## Project Layout

```text
.
|-- databricks.yml
|-- dashboards/
|   |-- nyc_taxi_analytics.lvdash.json
|   |-- nyc_taxi_analytics_dashboard.pdf
|-- maintenance/
|   |-- table_maintenance.py
|-- sample_data/
|   |-- yellow_trips_sample.csv
|-- setup/
|   |-- 01_unity_catalog_setup.sql
|   |-- 02_smoke_validation.sql
|-- tests/
|   |-- test_project_contract.py
|-- transformations/
|   |-- _config.py
|   |-- bronze/
|   |-- silver/
|   |-- gold/
|   |-- audit/
```

## Data Landing Contract

The production pipeline expects these ADLS Gen2 containers and paths:

```text
abfss://bronze@<storage-account>.dfs.core.windows.net/trip-data-yellow/trip-data/
abfss://bronze@<storage-account>.dfs.core.windows.net/trip-data-green/
abfss://bronze@<storage-account>.dfs.core.windows.net/trip-data-fhv/
abfss://bronze@<storage-account>.dfs.core.windows.net/trip-data-fhvheavy/
abfss://bronze@<storage-account>.dfs.core.windows.net/trip_zone/
```

The pipeline writes managed external Delta tables into these Unity Catalog schemas:

```text
nyctaxi_databricks.bronze
nyctaxi_databricks.silver
nyctaxi_databricks.gold
nyctaxi_databricks.audit
```

## Setup

Prerequisites:

- Azure Databricks workspace with Unity Catalog enabled.
- ADLS Gen2 storage account with `bronze`, `silver`, and `gold` containers.
- Storage credential and external location access configured for Databricks.
- Databricks CLI installed and authenticated.
- SQL Warehouse ID if you want to deploy the dashboard.

Create catalog and schemas:

```sql
-- Edit setup/01_unity_catalog_setup.sql first.
-- Replace <storage-account-name> with your storage account.
```

Then run the SQL in [setup/01_unity_catalog_setup.sql](setup/01_unity_catalog_setup.sql).

Create bundle variable overrides:

```powershell
New-Item -ItemType Directory -Force ".databricks\bundle\dev"
Copy-Item "configs\variable-overrides.json.example" `
  ".databricks\bundle\dev\variable-overrides.json"
```

Edit the copied file:

```json
{
  "workspace_host": "https://adb-0000000000000000.0.azuredatabricks.net",
  "storage_account_name": "yourstorageaccount",
  "notification_email": "you@example.com",
  "warehouse_id": "0000000000000000"
}
```

Validate and deploy:

```powershell
databricks bundle validate --target dev
databricks bundle deploy --target dev
databricks bundle run monthly_pipeline_update --target dev
```

After a successful pipeline update, run [setup/02_smoke_validation.sql](setup/02_smoke_validation.sql) to verify that bronze, silver, gold, and quarantine tables contain rows.

## Local Checks

These checks do not require Databricks. They verify the repository contract and catch embarrassing portfolio mistakes such as broken bundle paths, invalid dashboard JSON, leaked personal values, and README encoding issues.

```powershell
python -m unittest discover -s tests
python -m compileall transformations maintenance tests
```

## Data Quality Rules

Silver taxi tables drop records that fail critical checks:

- Missing pickup or dropoff timestamp.
- Pickup or dropoff location outside the NYC TLC zone range.
- Negative or unrealistic trip duration.
- Pickup dates outside the expected project window.
- Negative or extreme fare or total amount for taxi types with fare columns.

Monitoring expectations keep rows but surface quality metrics:

- Passenger count range.
- Trip distance range.
- Zero fare tracking.

The quarantine table separately captures failed raw records for review. It is not a perfect copy of every dropped silver rule yet; if this were a production system, the next improvement would be to centralize the DQ predicates so silver drops and quarantine reasons cannot drift.

## Gold Tables

| Table | Purpose |
| --- | --- |
| `gold.daily_borough_metrics` | Daily trip count, revenue, and distance by borough and taxi type |
| `gold.hourly_demand` | Hourly demand patterns by day and borough |
| `gold.airport_metrics` | JFK, LaGuardia, and Newark pickup/dropoff metrics |
| `gold.zone_revenue` | Revenue ranking by pickup zone |
| `gold.payment_trends` | Payment method trends |
| `gold.trip_type_summary` | Street-hail vs dispatch style summary |

## Dashboard

Dashboard assets live in [dashboards](dashboards/):

- `nyc_taxi_analytics.lvdash.json` for Databricks Lakeview deployment.
- `nyc_taxi_analytics_dashboard.pdf` as a portable snapshot.

The dashboard queries the gold tables and is intended to show:

- Revenue by pickup zone.
- Daily borough trends.
- Airport demand.
- Payment method mix.
- Taxi type summary.

## Interview Talking Points

Strong points:

- You used a real cloud analytics pattern, not a single notebook.
- You separated raw, cleaned, analytics, and audit responsibilities.
- You used data quality expectations and documented retention/maintenance.
- You included deployment config and dashboard assets.

Be honest about limitations:

- This repo assumes raw TLC data is already in ADLS.
- Infrastructure provisioning is not automated here.
- The quarantine predicates should be refactored to share the exact same rule definitions as the silver tables.
- Large row-count claims should be backed with screenshots or smoke-query outputs before being used in a resume.

## Cost Control

- Use development mode and small test slices while iterating.
- Stop SQL warehouses and all-purpose clusters after dashboard work.
- Schedule maintenance monthly, not daily, unless file counts justify it.
- Keep `VACUUM RETAIN 168 HOURS` unless your organization requires longer rollback windows.
