-- Run once in a Databricks SQL warehouse or notebook before deploying the bundle.
-- Replace <storage-account-name> with your ADLS Gen2 account.

CREATE CATALOG IF NOT EXISTS nyctaxi_databricks;
CREATE SCHEMA IF NOT EXISTS nyctaxi_databricks.bronze;
CREATE SCHEMA IF NOT EXISTS nyctaxi_databricks.silver;
CREATE SCHEMA IF NOT EXISTS nyctaxi_databricks.gold;
CREATE SCHEMA IF NOT EXISTS nyctaxi_databricks.audit;

ALTER SCHEMA nyctaxi_databricks.bronze SET MANAGED LOCATION
  'abfss://bronze@<storage-account-name>.dfs.core.windows.net/tables';

ALTER SCHEMA nyctaxi_databricks.silver SET MANAGED LOCATION
  'abfss://silver@<storage-account-name>.dfs.core.windows.net';

ALTER SCHEMA nyctaxi_databricks.gold SET MANAGED LOCATION
  'abfss://gold@<storage-account-name>.dfs.core.windows.net';

ALTER SCHEMA nyctaxi_databricks.audit SET MANAGED LOCATION
  'abfss://silver@<storage-account-name>.dfs.core.windows.net/_audit';

