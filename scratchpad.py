--0. One-time SQL setup (run once)

-- Schemas
CREATE SCHEMA IF NOT EXISTS main.d_bronze;
CREATE SCHEMA IF NOT EXISTS main.d_silver;
CREATE SCHEMA IF NOT EXISTS main.control;

-- Status table for CDF version tracking
CREATE TABLE IF NOT EXISTS main.control.cdf_status_scd2 (
  source_table             STRING,
  last_processed_version   BIGINT,
  last_processed_timestamp TIMESTAMP,
  last_batch_id            STRING,
  versions_processed       BIGINT,
  PRIMARY KEY (source_table)
) USING DELTA;

-- Bronze (append-only) with CDF enabled
CREATE TABLE IF NOT EXISTS main.d_bronze.customer_bronze (
  customer_id       STRING,
  customer_name     STRING,
  email_address     STRING,
  record_timestamp  TIMESTAMP,
  loaded_at_utc     TIMESTAMP
)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Silver Dimension (SCD2) – history table
CREATE TABLE IF NOT EXISTS main.d_silver.dim_customer_scd2 (
  customer_sk          BIGINT GENERATED ALWAYS AS IDENTITY,
  customer_id          STRING,
  customer_name        STRING,
  email_address        STRING,
  record_timestamp     TIMESTAMP,
  bronze_loaded_at_utc TIMESTAMP,
  effective_start_utc  TIMESTAMP,
  effective_end_utc    TIMESTAMP,
  is_current           BOOLEAN
)
USING DELTA;



--Optional: clear and seed Bronze for testing:
TRUNCATE TABLE main.d_bronze.customer_bronze;
TRUNCATE TABLE main.d_silver.dim_customer_scd2;
DELETE FROM main.control.cdf_status_scd2 WHERE source_table = 'main.d_bronze.customer_bronze';

INSERT INTO main.d_bronze.customer_bronze VALUES
  ('C001','Alice','alice@example.com',  current_timestamp(), current_timestamp()),
  ('C002','Bob',  'bob@example.com',    current_timestamp(), current_timestamp()),
  ('C003','Carol','carol@example.com',  current_timestamp(), current_timestamp());



-- Notebook: CDF → SCD2 merge
Cell 1 – Parameters
dbutils.widgets.text("bronze_table",  "main.d_bronze.customer_bronze")
dbutils.widgets.text("dim_table",     "main.d_silver.dim_customer_scd2")
dbutils.widgets.text("status_table",  "main.control.cdf_status_scd2")
dbutils.widgets.text("key_columns",   "customer_id")
dbutils.widgets.text("data_columns",  "customer_name,email_address,record_timestamp")
Cell 2 – Setup
from delta.tables import DeltaTable
from pyspark.sql.functions import (
    current_timestamp, col, max as spark_max, row_number
)
from pyspark.sql.window import Window
import datetime as dt

bronze_table = dbutils.widgets.get("bronze_table")
dim_table    = dbutils.widgets.get("dim_table")
status_table = dbutils.widgets.get("status_table")

key_cols  = [c.strip() for c in dbutils.widgets.get("key_columns").split(",") if c.strip()]
data_cols = [c.strip() for c in dbutils.widgets.get("data_columns").split(",") if c.strip()]

batch_id = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")

print(f"[CONFIG] Bronze : {bronze_table}")
print(f"[CONFIG] Dim    : {dim_table}")
print(f"[CONFIG] Status : {status_table}")
print(f"[CONFIG] Keys   : {key_cols}")
print(f"[CONFIG] Data   : {data_cols}")
print(f"[CONFIG] Batch  : {batch_id}")
Cell 3 – Get last processed CDF version
source_id = bronze_table  # identifier for status table

last_version_query = f"""
    SELECT last_processed_version
    FROM {status_table}
    WHERE source_table = '{source_id}'
"""

try:
    rows = spark.sql(last_version_query).collect()
    if rows and rows[0]["last_processed_version"] is not None:
        start_version = rows[0]["last_processed_version"] + 1
    else:
        start_version = 0
except Exception:
    start_version = 0

print(f"[INFO] Starting from CDF version: {start_version}")

Cell 4 – Read CDF changes (append-only → inserts only)
cdf_query = f"""
  SELECT *, _commit_version
  FROM table_changes('{bronze_table}', {start_version})
  WHERE _change_type = 'insert'
"""

df_changes = spark.sql(cdf_query)

if df_changes.limit(1).count() == 0:
    print(f"[INFO] No new changes since version {start_version}")
    dbutils.notebook.exit("NO_CHANGES")

changes_count = df_changes.count()
max_version   = df_changes.agg(spark_max("_commit_version")).collect()[0][0]
versions_proc = max_version - start_version + 1

print(f"[INFO] New inserts           : {changes_count}")
print(f"[INFO] CDF version range     : {start_version} → {max_version}")
print(f"[INFO] CDF versions processed: {versions_proc}")

Cell 5 – Prepare “latest per key” batch snapshot
For SCD2 we need to compare latest incoming state per key to the current active dim row.
w = Window.partitionBy(*key_cols).orderBy(col("_commit_version").desc())

df_latest = (
    df_changes
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") == 1)
    .drop("rn")
)

# df_latest has one “latest” row per customer_id in this batch
print(f"[INFO] Latest-per-key rows in batch: {df_latest.count()}")
Cell 6 – Split NEW vs CHANGED vs UNCHANGED
We interpret “change” as any meaningful attribute difference vs current SCD2 active row.
# Read current dimension (only current rows)
dim_current = spark.table(dim_table).filter(col("is_current") == True)

# Join latest incoming with current dim to detect changes
join_expr = " AND ".join([f"d.{k} = s.{k}" for k in key_cols])

df_join = (
    df_latest.alias("s")
    .join(dim_current.alias("d"), on=[col(f"s.{k}") == col(f"d.{k}") for k in key_cols], how="left")
)

# Columns to compare for change
compare_cols = data_cols

# Rows with NO current dim row → brand new customers
df_new = df_join.filter(col("d.customer_id").isNull()).select("s.*")

# Rows with existing current dim row AND any changed data column
change_cond = None
for c in compare_cols:
    cond = (col(f"s.{c}") != col(f"d.{c}")) | (col(f"s.{c}").isNull() != col(f"d.{c}").isNull())
    change_cond = cond if change_cond is None else (change_cond | cond)

df_changed = df_join.filter(col("d.customer_id").isNotNull() & change_cond).select("s.*")

print(f"[INFO] New customers     : {df_new.count()}")
print(f"[INFO] Changed customers : {df_changed.count()}")

Cell 7 – Prepare rows to INSERT for SCD2
# For SCD2:

# For new keys:

# Insert a new row with is_current = true, effective_start_utc = now, effective_end_utc = null.

# For changed keys:

# End-date old row (is_current = false, effective_end_utc = now).

# Insert a new row with is_current = true, new attributes, new start date.

# First, build the new rows to insert:

                               from pyspark.sql.functions import lit

now_ts = current_timestamp()

# NEW keys → new SCD2 rows
df_new_scd2 = (
    df_new
    .withColumnRenamed("loaded_at_utc", "bronze_loaded_at_utc")
    .withColumn("effective_start_utc", now_ts)
    .withColumn("effective_end_utc",   lit(None).cast("timestamp"))
    .withColumn("is_current",          lit(True))
)

# CHANGED keys → new SCD2 rows (new version)
df_changed_scd2 = (
    df_changed
    .withColumnRenamed("loaded_at_utc", "bronze_loaded_at_utc")
    .withColumn("effective_start_utc", now_ts)
    .withColumn("effective_end_utc",   lit(None).cast("timestamp"))
    .withColumn("is_current",          lit(True))
)

df_to_insert = df_new_scd2.unionByName(df_changed_scd2)

print(f"[INFO] Total new SCD2 rows to insert: {df_to_insert.count()}")

Cell 8 – End-date existing current rows for changed keys

delta_dim = DeltaTable.forName(spark, dim_table)

if df_changed.limit(1).count() > 0:
    # Keys that changed
    changed_keys = df_changed.select(*key_cols).distinct()
    end_join = " AND ".join([f"t.{k} = s.{k}" for k in key_cols])

    (
        delta_dim.alias("t")
        .merge(
            changed_keys.alias("s"),
            end_join
        )
        .whenMatchedUpdate(
            condition="t.is_current = true",
            set={
                "is_current": "false",
                "effective_end_utc": "current_timestamp()"
            }
        )
        .execute()
    )
    print(f"[INFO] End-dated current rows for changed keys: {changed_keys.count()}")
else:
    print("[INFO] No changed keys → no end-dates applied")
Cell 9 – Insert new SCD2 rows
if df_to_insert.limit(1).count() > 0:
    (
        df_to_insert
        .select(
            *key_cols,
            *data_cols,
            "bronze_loaded_at_utc",
            "effective_start_utc",
            "effective_end_utc",
            "is_current"
        )
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(dim_table)
    )
    print(f"[INFO] Inserted {df_to_insert.count()} new SCD2 rows")
else:
    print("[INFO] No new SCD2 rows to insert")
Cell 10 – Update CDF status
status_merge = f"""
    MERGE INTO {status_table} t
    USING (
        SELECT
            '{source_id}'         AS source_table,
            {max_version}         AS last_processed_version,
            current_timestamp()   AS last_processed_timestamp,
            '{batch_id}'          AS last_batch_id,
            {versions_proc}       AS versions_processed
    ) s
    ON t.source_table = s.source_table
    WHEN MATCHED THEN UPDATE SET
        t.last_processed_version   = s.last_processed_version,
        t.last_processed_timestamp = s.last_processed_timestamp,
        t.last_batch_id            = s.last_batch_id,
        t.versions_processed       = s.versions_processed
    WHEN NOT MATCHED THEN INSERT *
"""

spark.sql(status_merge)
print(f"[INFO] Status updated: {start_version} → {max_version}")
dbutils.notebook.exit("OK")


