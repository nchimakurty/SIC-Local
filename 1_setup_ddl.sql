-- ============================================================================
-- DATABRICKS BRONZE-TO-ODS IMPLEMENTATION
-- DDL Setup Scripts (Unity Catalog)
-- ============================================================================

-- ============================================================================
-- 1. CREATE BRONZE TABLE (Raw Append-Only Source)
-- ============================================================================

CREATE TABLE IF NOT EXISTS main.d_bronze.edp_customer_bronze (
  customer_id        STRING NOT NULL,
  customer_name      STRING,
  email_address      STRING,
  is_active          BOOLEAN,
  business_key_hash  STRING,
  record_timestamp   TIMESTAMP,
  _extracted_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Bronze: Raw append-only customer data from simulator'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.appendOnly' = 'true'
);

-- Verify Bronze table creation
DESCRIBE TABLE EXTENDED main.d_bronze.edp_customer_bronze;

-- View Bronze table properties
SHOW TBLPROPERTIES main.d_bronze.edp_customer_bronze;

-- ============================================================================
-- 2. CREATE ODS TABLE (Active Records with Audit Fields)
-- ============================================================================

CREATE TABLE IF NOT EXISTS main.d_silver.ods_customer (
  customer_id        STRING NOT NULL,
  customer_name      STRING,
  email_address      STRING,
  is_active          BOOLEAN,
  business_key_hash  STRING,
  loaded_at_utc      TIMESTAMP,
  modified_at_utc    TIMESTAMP,
  _etl_batch_id      STRING
)
USING DELTA
COMMENT 'ODS: Active customer records with load/modify audit fields'
TBLPROPERTIES (
  'delta.appendOnly' = 'false'
);

-- Add primary key constraint (informational in Delta)
ALTER TABLE main.d_silver.ods_customer
ADD CONSTRAINT pk_ods_customer PRIMARY KEY(customer_id);

-- Verify ODS table creation
DESCRIBE TABLE EXTENDED main.d_silver.ods_customer;

-- ============================================================================
-- 3. CREATE METADATA TABLE (For Tracking Watermarks & Batch Info)
-- ============================================================================

CREATE TABLE IF NOT EXISTS main.d_silver.etl_batch_metadata (
  batch_id           STRING,
  source_table       STRING,
  target_table       STRING,
  batch_start_time   TIMESTAMP,
  batch_end_time     TIMESTAMP,
  rows_processed     BIGINT,
  rows_inserted      BIGINT,
  rows_updated       BIGINT,
  status             STRING,
  error_message      STRING,
  last_processed_utc TIMESTAMP
)
USING DELTA
COMMENT 'ETL metadata: Tracks batch executions and watermarks';

-- Verify metadata table
DESCRIBE TABLE EXTENDED main.d_silver.etl_batch_metadata;

-- ============================================================================
-- 4. TEST DATA INSERT (For MVP Testing)
-- ============================================================================

-- Insert initial test records into Bronze
INSERT INTO main.d_bronze.edp_customer_bronze 
  (customer_id, customer_name, email_address, is_active, business_key_hash, record_timestamp)
VALUES
  ('C001', 'Alice Johnson', 'alice@example.com', true, 'hash_c001', CURRENT_TIMESTAMP()),
  ('C002', 'Bob Smith', 'bob@example.com', true, 'hash_c002', CURRENT_TIMESTAMP()),
  ('C003', 'Carol White', 'carol@example.com', true, 'hash_c003', CURRENT_TIMESTAMP());

-- Verify Bronze data
SELECT COUNT(*) as bronze_row_count FROM main.d_bronze.edp_customer_bronze;
SELECT * FROM main.d_bronze.edp_customer_bronze ORDER BY customer_id;