# Sample Data

The production pipeline reads NYC TLC parquet files from ADLS Gen2. This folder intentionally keeps only tiny CSV examples for documentation and test-fixture purposes so the repository stays lightweight.

Use the official NYC TLC parquet files for real runs:

- Yellow taxi trips
- Green taxi trips
- FHV trips
- High-volume FHV trips
- Taxi zone lookup

Expected ADLS landing layout:

```text
abfss://bronze@<storage-account>.dfs.core.windows.net/trip-data-yellow/trip-data/
abfss://bronze@<storage-account>.dfs.core.windows.net/trip-data-green/
abfss://bronze@<storage-account>.dfs.core.windows.net/trip-data-fhv/
abfss://bronze@<storage-account>.dfs.core.windows.net/trip-data-fhvheavy/
abfss://bronze@<storage-account>.dfs.core.windows.net/trip_zone/
```

