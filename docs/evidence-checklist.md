# Evidence Checklist

Before publishing this project or putting it on a resume, capture evidence that the pipeline ran in your workspace.

## Required Screenshots

- Databricks pipeline graph with all bronze, silver, gold, and audit nodes.
- Pipeline update summary showing success.
- Data quality expectations summary.
- Query output from `setup/02_smoke_validation.sql`.
- Dashboard screenshot from `dashboards/nyc_taxi_analytics_dashboard.pdf` or the live Lakeview dashboard.
- ADLS Gen2 containers showing `bronze`, `silver`, and `gold`.

## Resume-Safe Metrics

Use only numbers you can back with screenshots or query output:

- Total raw rows ingested.
- Silver rows after quality filtering.
- Quarantine row count.
- Number of gold tables/views.
- Pipeline runtime.
- Cluster size used.

## Honest Project Framing

Good:

> Built a Databricks Lakeflow medallion pipeline on Azure for NYC Taxi data, with DQ expectations, quarantine handling, gold marts, audit views, and Lakeview dashboard assets.

Avoid unless you add infra and ingestion automation:

> Built a fully end-to-end Azure platform from infrastructure provisioning through ingestion, transformation, orchestration, and BI.

