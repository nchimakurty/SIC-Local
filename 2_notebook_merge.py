# ============================================================================
# DATABRICKS BRONZE-TO-ODS MERGE NOTEBOOK
# Python Implementation with Full MERGE Logic
# ============================================================================

# ==============================================================================
# CELL 1: IMPORTS, CONFIGURATION & INITIALIZATION
# ==============================================================================

from pyspark.sql.functions import (
    current_timestamp, col, when, lit, row_number, 
    coalesce, max as spark_max, count as spark_count
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import datetime as dt
import json

print("=" * 80)
print("BRONZE-TO-ODS ETL PROCESS - INITIATED")
print("=" * 80)

# Configuration Parameters (Hardcoded for MVP; parameterize later via Workflow)
CATALOG = "main"
BRONZE_SCHEMA = "d_bronze"
BRONZE_TABLE = "edp_customer_bronze"
SILVER_SCHEMA = "d_silver"
ODS_TABLE = "ods_customer"
METADATA_TABLE = "etl_batch_metadata"

# Full Qualified Names (FQN)
BRONZE_FQN = f"{CATALOG}.{BRONZE_SCHEMA}.{BRONZE_TABLE}"
ODS_FQN = f"{CATALOG}.{SILVER_SCHEMA}.{ODS_TABLE}"
METADATA_FQN = f"{CATALOG}.{SILVER_SCHEMA}.{METADATA_TABLE}"

# Batch ID (unique identifier for this run)
BATCH_ID = dt.datetime.now().strftime("%Y%m%d_%H%M%S")

# Configuration flags
HARD_DELETE_INACTIVE = False  # True: delete rows with is_active=FALSE; False: soft delete
ENABLE_INCREMENTAL = False   # True: process only new rows; False: full table
ENABLE_METADATA_LOG = True   # True: log metadata to tracking table

print(f"\n[CONFIG]")
print(f"  Catalog:              {CATALOG}")
print(f"  Bronze Source:        {BRONZE_FQN}")
print(f"  ODS Target:           {ODS_FQN}")
print(f"  Metadata Table:       {METADATA_FQN}")
print(f"  Batch ID:             {BATCH_ID}")
print(f"  Hard Delete Inactive: {HARD_DELETE_INACTIVE}")
print(f"  Incremental Mode:     {ENABLE_INCREMENTAL}")
print(f"  Enable Metadata Log:  {ENABLE_METADATA_LOG}")
print("=" * 80)

# ==============================================================================
# CELL 2: READ BRONZE DATA & PREPARE SOURCE DATAFRAME
# ==============================================================================

try:
    # Read Bronze table
    df_bronze = spark.table(BRONZE_FQN)
    
    # Check if table exists and has data
    bronze_count = df_bronze.count()
    print(f"\n[BRONZE READ]")
    print(f"  Total rows in Bronze: {bronze_count}")
    
    if bronze_count == 0:
        print("  WARNING: Bronze table is empty. Nothing to process.")
    
    # Display sample data
    print(f"\n[BRONZE SAMPLE DATA]")
    df_bronze.show(truncate=False, n=5)
    
except Exception as e:
    print(f"\n[ERROR] Failed to read Bronze table: {str(e)}")
    raise

# Prepare Source DataFrame with Audit Columns
try:
    df_source = (
        df_bronze
        .withColumn("loaded_at_utc", current_timestamp())
        .withColumn("modified_at_utc", current_timestamp())
        .withColumn("_etl_batch_id", lit(BATCH_ID))
    )
    
    source_count = df_source.count()
    print(f"\n[SOURCE PREPARATION]")
    print(f"  Rows with audit columns: {source_count}")
    
    if source_count > 0:
        print(f"\n[SOURCE SAMPLE]")
        df_source.show(truncate=False, n=3)
    
except Exception as e:
    print(f"\n[ERROR] Failed to prepare source data: {str(e)}")
    raise

# ==============================================================================
# CELL 3: PERFORM MERGE INTO ODS
# ==============================================================================

try:
    print(f"\n[MERGE OPERATION]")
    print(f"  Starting MERGE at: {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Get reference to ODS Delta table
    delta_ods = DeltaTable.forName(spark, ODS_FQN)
    
    # MERGE Logic: Match on customer_id (primary key)
    # WHEN MATCHED: Update all columns except loaded_at_utc (preserve original)
    # WHEN NOT MATCHED: Insert all columns (including loaded_at_utc set to current_timestamp)
    
    merge_result = (
        delta_ods.alias("t")
        .merge(
            df_source.alias("s"),
            "t.customer_id = s.customer_id"  # Merge condition: match on primary key
        )
        .whenMatchedUpdate(
            set={
                "customer_name": "s.customer_name",
                "email_address": "s.email_address",
                "is_active": "s.is_active",
                "business_key_hash": "s.business_key_hash",
                "modified_at_utc": "s.modified_at_utc",
                "_etl_batch_id": "s._etl_batch_id"
                # CRITICAL: Do NOT update loaded_at_utc (preserve original load time)
            }
        )
        .whenNotMatchedInsert(
            values={
                "customer_id": "s.customer_id",
                "customer_name": "s.customer_name",
                "email_address": "s.email_address",
                "is_active": "s.is_active",
                "business_key_hash": "s.business_key_hash",
                "loaded_at_utc": "s.loaded_at_utc",      # New record: set loaded_at_utc
                "modified_at_utc": "s.modified_at_utc",
                "_etl_batch_id": "s._etl_batch_id"
            }
        )
        .execute()
    )
    
    print(f"  MERGE operation completed successfully")
    print(f"  Batch ID: {BATCH_ID}")
    
except Exception as e:
    print(f"\n[ERROR] MERGE operation failed: {str(e)}")
    raise

# ==============================================================================
# CELL 4: HANDLE INACTIVE RECORDS (Soft Delete or Hard Delete)
# ==============================================================================

try:
    print(f"\n[INACTIVE RECORDS HANDLING]")
    
    if HARD_DELETE_INACTIVE:
        # Option A: Hard delete - physically remove inactive records
        print(f"  Using HARD DELETE strategy (physically removing rows)")
        rows_deleted = spark.sql(f"DELETE FROM {ODS_FQN} WHERE is_active = FALSE")
        print(f"  Deleted inactive records from ODS")
    else:
        # Option B: Soft delete - keep rows but mark is_active = FALSE
        print(f"  Using SOFT DELETE strategy (keeping rows with is_active = FALSE)")
        print(f"  Recommendation: Always query ODS with WHERE is_active = TRUE")
    
except Exception as e:
    print(f"\n[WARNING] Inactive record handling encountered issue: {str(e)}")

# ==============================================================================
# CELL 5: VALIDATION, METRICS & LOGGING
# ==============================================================================

try:
    print(f"\n[VALIDATION & METRICS]")
    print(f"  {'=' * 60}")
    
    # Get ODS statistics
    ods_stats = spark.sql(f"""
        SELECT 
          COUNT(*) as total_rows,
          SUM(CASE WHEN is_active = TRUE THEN 1 ELSE 0 END) as active_rows,
          SUM(CASE WHEN is_active = FALSE THEN 1 ELSE 0 END) as inactive_rows,
          MIN(loaded_at_utc) as earliest_load_time,
          MAX(modified_at_utc) as latest_modify_time
        FROM {ODS_FQN}
    """).collect()[0]
    
    ods_total = ods_stats['total_rows']
    ods_active = ods_stats['active_rows'] or 0
    ods_inactive = ods_stats['inactive_rows'] or 0
    
    print(f"  ODS Total Records:    {ods_total}")
    print(f"  ODS Active Records:   {ods_active}")
    print(f"  ODS Inactive Records: {ods_inactive}")
    print(f"  Earliest Load Time:   {ods_stats['earliest_load_time']}")
    print(f"  Latest Modify Time:   {ods_stats['latest_modify_time']}")
    
    # Show recent modifications
    print(f"\n[RECENT ODS CHANGES]")
    spark.sql(f"""
        SELECT 
          customer_id,
          customer_name,
          is_active,
          loaded_at_utc,
          modified_at_utc
        FROM {ODS_FQN}
        WHERE modified_at_utc >= (CURRENT_TIMESTAMP() - INTERVAL 1 HOUR)
        ORDER BY modified_at_utc DESC
        LIMIT 10
    """).show(truncate=False)
    
    # Check for data quality issues
    print(f"\n[DATA QUALITY CHECKS]")
    null_check = spark.sql(f"""
        SELECT 
          SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customer_id,
          SUM(CASE WHEN is_active IS NULL THEN 1 ELSE 0 END) as null_is_active
        FROM {ODS_FQN}
    """).collect()[0]
    
    print(f"  NULL customer_id: {null_check['null_customer_id']}")
    print(f"  NULL is_active:   {null_check['null_is_active']}")
    
except Exception as e:
    print(f"\n[WARNING] Validation encountered issue: {str(e)}")

# ==============================================================================
# CELL 6: SUMMARY & COMPLETION
# ==============================================================================

print(f"\n{'=' * 80}")
print(f"BRONZE-TO-ODS ETL PROCESS - COMPLETED")
print(f"{'=' * 80}")
print(f"Batch ID:        {BATCH_ID}")
print(f"Completion Time: {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
print(f"Status:          SUCCESS")
print(f"{'=' * 80}\n")