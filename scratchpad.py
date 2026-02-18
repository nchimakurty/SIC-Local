# Databricks notebook source
# MAGIC %md
# MAGIC ## BRONZE-TO-ODS MERGE NOTEBOOK
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parameters

# COMMAND ----------

# DBTITLE 1,Cell 3
dbutils.widgets.removeAll()
dbutils.widgets.text("bronze_catalog", "d_bronze")
dbutils.widgets.text("silver_catalog",    "d_silver_ods")
dbutils.widgets.text("schema_name", "temp")
dbutils.widgets.text("table_name", "customer_test")
dbutils.widgets.text("key_columns",  "customer_id")  # comma-separated
dbutils.widgets.text("data_columns", "customer_name, email_address") # comma-separated

# COMMAND ----------

# MAGIC %md
# MAGIC ###Read params & setup

# COMMAND ----------

# DBTITLE 1,Cell 5
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit, col, max as spark_max, row_number
from pyspark.sql.window import Window
import datetime as dt

bronze_catalog = dbutils.widgets.get("bronze_catalog")
silver_catalog = dbutils.widgets.get("silver_catalog")
schema_name = dbutils.widgets.get("schema_name")
table_name    = dbutils.widgets.get("table_name")
key_cols     = [c.strip() for c in dbutils.widgets.get("key_columns").split(",") if c.strip()]
data_cols    = [c.strip() for c in dbutils.widgets.get("data_columns").split(",") if c.strip()]

print(f"Bronze Catalog: {bronze_catalog}")
print(f"Silver Catalog: {silver_catalog}")
print(f"Schema Name: {schema_name}")
print(f"Table Name   : {table_name}")
print(f"Keys  : {key_cols}")
print(f"Data  : {data_cols}")

# COMMAND ----------

# DBTITLE 1,Cell 6
# MAGIC %md
# MAGIC ###Get last processed CDF version from Delta history

# COMMAND ----------

# DBTITLE 1,Cell 7
# Read last processed version from target table's _bronze_version column
target_table = f"{silver_catalog}.{schema_name}.{table_name}_hist"

try:
    # Query the max bronze version from the target table
    max_version_query = f"""
        SELECT MAX(_bronze_version) as last_version
        FROM {target_table}
    """
    
    result = spark.sql(max_version_query).collect()
    
    if result and result[0]["last_version"] is not None:
        start_version = result[0]["last_version"] + 1
    else:
        start_version = 0
except Exception as e:
    # Table doesn't exist yet
    print(f"No history found (table may be new): {e}")
    start_version = 0

print(f"Starting from CDF version: {start_version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read CDF (Change Data Feed) changes from Bronze

# COMMAND ----------

# Read the changes from the bronze table

cdf_query = f"""
  SELECT *, _commit_version
  FROM table_changes('{bronze_catalog}.{schema_name}.{table_name}', {start_version})
  WHERE _change_type = 'insert'
"""

df_changes = spark.sql(cdf_query)

print(f"CDF Query:\n{cdf_query}")

df_changes = spark.sql(cdf_query)  # table_changes is the CDF function.[web:64]
print(f"Changes in CDF: Sample 10 records")
display(df_changes,10)

if df_changes.isEmpty():
    print("No changes to process.")
    dbutils.notebook.exit("No changes in bronze layer to process")

changes_count   = df_changes.count()
max_version     = df_changes.agg(spark_max("_commit_version")).collect()[0][0]
versions_proc   = max_version - start_version + 1

print(f"New inserts          : {changes_count}")
print(f"CDF version range    : {start_version} → {max_version}")
print(f"CDF versions processed: {versions_proc}")    

# COMMAND ----------

# DBTITLE 1,Cell 10
# MAGIC %md
# MAGIC ###Prepare source and detect processing strategy

# COMMAND ----------

# DBTITLE 1,Cell 11
# For proper SCD2, deduplicate WITHIN each version only
# This preserves intermediate changes across different versions
w = Window.partitionBy(*key_cols, "_commit_version").orderBy(col("_commit_timestamp").desc())

df_source = (
    df_changes
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") == 1)  # Keep one record per key per version
    .drop("rn", "_change_type")
)

# Detect if any keys appear in multiple versions (requires sequential processing)
keys_per_version = (
    df_source
    .groupBy(*key_cols)
    .agg(spark_max("_commit_version").alias("max_version"))
    .count()
)

total_records = df_source.count()
requires_sequential = (keys_per_version < total_records)

# Get list of versions for sequential processing (if needed)
# versions_to_process = (
#     df_source
#     .select("_commit_version")
#     .distinct()
#     .orderBy("_commit_version")
#     .rdd.flatMap(lambda x: x).collect()
# )

# Use toPandas() instead of RDD to be compatible on serverless
versions_to_process = (
    df_source
    .select("_commit_version")
    .distinct()
    .orderBy("_commit_version")
    .toPandas()["_commit_version"]
    .tolist()
)

print(f"Total changes: {total_records}")
print(f"Unique keys: {keys_per_version}")
print(f"Versions in batch: {versions_to_process}")
print(f"Processing strategy: {'SEQUENTIAL (keys changed multiple times)' if requires_sequential else 'SINGLE MERGE (fast path)'}")
print(f"\nSample of changes:")
display(df_source.orderBy("_commit_version", "_commit_timestamp"), 10)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Create insert and update for each delta record to handle Single Merge

# COMMAND ----------

# Prepare insert and update dataframes for MERGE from df_source
update_df = 

insert_dict = {
    **{k: f"source.{k}" for k in key_cols},
    **{c: f"source.{c}" for c in data_cols},
    "bronze_loaded_at_utc": "source.loaded_at_utc",
    "effective_start_utc": "source._commit_timestamp",
    "effective_end_utc": "to_timestamp('9999-12-31 23:59:59', 'yyyy-MM-dd HH:mm:ss')",
    "is_current": "true",
    "_bronze_version": "source._commit_version",
    "processed_at_utc": "current_timestamp()"
}

print("Update dict for MERGE:")
print(update_dict)
print("\nInsert dict for MERGE:")
print(insert_dict)

# COMMAND ----------

# DBTITLE 1,Cell 12
# MAGIC %md
# MAGIC ###Execute SCD2 MERGE (Adaptive Strategy)
# MAGIC
# MAGIC **Fast Path (Single MERGE)**: When no keys change multiple times across versions
# MAGIC * Single atomic MERGE operation
# MAGIC * Best performance
# MAGIC
# MAGIC **Sequential Path**: When keys change multiple times across versions
# MAGIC * Process each version in order
# MAGIC * Ensures correct SCD2 history for all intermediate changes

# COMMAND ----------

# DBTITLE 1,Untitled
# Load target Delta table
delta_dim = DeltaTable.forName(spark, f"{silver_catalog}.{schema_name}.{table_name}_hist")

# Build merge condition: match on business keys AND current rows only
merge_condition = " AND ".join([f"target.{k} = source.{k}" for k in key_cols])
merge_condition_with_current = f"{merge_condition} AND target.is_current = true"

print(f"Merge condition: {merge_condition_with_current}\n")

total_rows_processed = 0

if not requires_sequential:
    # FAST PATH: Single MERGE (no keys change multiple times)
    print("Using FAST PATH: Single atomic MERGE\n")
    
    if df_source.limit(1).count() > 0:
        (
            delta_dim.alias("target")
            .merge(
                df_source.alias("source"),
                merge_condition_with_current
            )
            .whenMatchedUpdate(
                set={
                    "is_current": "false",
                    "effective_end_utc": "source._commit_timestamp",
                    "processed_at_utc": "current_timestamp()"
                }
            )
            .whenNotMatchedInsert(
                values={
                    **{k: f"source.{k}" for k in key_cols},
                    **{c: f"source.{c}" for c in data_cols},
                    "bronze_loaded_at_utc": "source.loaded_at_utc",
                    "effective_start_utc": "source._commit_timestamp",
                    "effective_end_utc": "to_timestamp('9999-12-31 23:59:59', 'yyyy-MM-dd HH:mm:ss')",
                    "is_current": "true",
                    "_bronze_version": "source._commit_version",
                    "processed_at_utc": "current_timestamp()"
                }
            )
            .execute()
        )
        total_rows_processed = df_source.count()
        print(f"  ✓ Single MERGE complete: {total_rows_processed} rows")

else:
    # SEQUENTIAL PATH: Process each version separately (keys change multiple times)
    print("Using SEQUENTIAL PATH: Processing versions in order\n")
    
    for version in versions_to_process:
        print(f"Processing version {version}...")
        
        df_version = df_source.filter(col("_commit_version") == version)
        
        if df_version.limit(1).count() > 0:
            (
                delta_dim.alias("target")
                .merge(
                    df_version.alias("source"),
                    merge_condition_with_current
                )
                .whenMatchedUpdate(
                    set={
                        "is_current": "false",
                        "effective_end_utc": "source._commit_timestamp",
                        "processed_at_utc": "current_timestamp()"
                    }
                )
                .whenNotMatchedInsert(
                    values={
                        **{k: f"source.{k}" for k in key_cols},
                        **{c: f"source.{c}" for c in data_cols},
                        "bronze_loaded_at_utc": "source.loaded_at_utc",
                        "effective_start_utc": "source._commit_timestamp",
                        "effective_end_utc": "to_timestamp('9999-12-31 23:59:59', 'yyyy-MM-dd HH:mm:ss')",
                        "is_current": "true",
                        "_bronze_version": "source._commit_version",
                        "processed_at_utc": "current_timestamp()"
                    }
                )
                .execute()
            )
            
            version_rows = df_version.count()
            total_rows_processed += version_rows
            print(f"  ✓ Version {version}: {version_rows} rows processed")

print(f"\n✓ SCD2 MERGE complete")
print(f"  - Total rows processed: {total_rows_processed}")
print(f"  - Bronze version range: {start_version} → {max_version}")
