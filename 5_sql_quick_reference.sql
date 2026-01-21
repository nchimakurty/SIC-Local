-- ============================================================================
-- QUICK REFERENCE: Essential SQL Commands for Bronze-to-ODS Pipeline
-- ============================================================================

-- ============================================================================
-- SECTION 1: CHECK TABLE STATES
-- ============================================================================

-- Check Bronze row count
SELECT COUNT(*) as bronze_row_count FROM main.d_bronze.edp_customer_bronze;

-- Check ODS active records
SELECT COUNT(*) as ods_active_count 
FROM main.d_silver.ods_customer 
WHERE is_active = TRUE;

-- View ODS active records (standard query pattern - ALWAYS use this!)
SELECT 
  customer_id, 
  customer_name, 
  email_address,
  is_active,
  loaded_at_utc, 
  modified_at_utc
FROM main.d_silver.ods_customer 
WHERE is_active = TRUE
ORDER BY customer_id;

-- ============================================================================
-- SECTION 2: AUDIT TIMESTAMP VERIFICATION
-- ============================================================================

-- View records that have been updated (loaded_at != modified_at)
SELECT 
  customer_id, 
  customer_name, 
  loaded_at_utc, 
  modified_at_utc,
  DATEDIFF(SECOND, loaded_at_utc, modified_at_utc) as seconds_since_load
FROM main.d_silver.ods_customer
WHERE loaded_at_utc < modified_at_utc
ORDER BY modified_at_utc DESC
LIMIT 10;

-- View records never updated (loaded_at == modified_at)
SELECT 
  customer_id, 
  customer_name, 
  loaded_at_utc, 
  modified_at_utc
FROM main.d_silver.ods_customer
WHERE loaded_at_utc = modified_at_utc
ORDER BY customer_id;

-- ============================================================================
-- SECTION 3: TEST DATA INSERTS
-- ============================================================================

-- Insert new test record
INSERT INTO main.d_bronze.edp_customer_bronze 
  (customer_id, customer_name, email_address, is_active, business_key_hash, record_timestamp)
VALUES 
  ('TESTME', 'Test Customer', 'test@example.com', true, 'test_hash', CURRENT_TIMESTAMP());

-- Insert update + new record (upsert test)
INSERT INTO main.d_bronze.edp_customer_bronze 
  (customer_id, customer_name, email_address, is_active, business_key_hash, record_timestamp)
VALUES
  ('C001', 'Alice Updated', 'alice.updated@example.com', true, 'hash_c001', CURRENT_TIMESTAMP()),
  ('C005', 'Eve Smith', 'eve@example.com', true, 'hash_c005', CURRENT_TIMESTAMP());

-- Insert soft delete (is_active = FALSE)
INSERT INTO main.d_bronze.edp_customer_bronze 
  (customer_id, customer_name, email_address, is_active, business_key_hash, record_timestamp)
VALUES 
  ('C002', 'Bob Smith', 'bob@example.com', FALSE, 'hash_c002', CURRENT_TIMESTAMP());

-- ============================================================================
-- SECTION 4: DATA QUALITY CHECKS
-- ============================================================================

-- NULL value check
SELECT 
  SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customer_id,
  SUM(CASE WHEN is_active IS NULL THEN 1 ELSE 0 END) as null_is_active,
  SUM(CASE WHEN loaded_at_utc IS NULL THEN 1 ELSE 0 END) as null_loaded_at_utc,
  SUM(CASE WHEN modified_at_utc IS NULL THEN 1 ELSE 0 END) as null_modified_at_utc
FROM main.d_silver.ods_customer;

-- Timestamp integrity check
SELECT 
  SUM(CASE WHEN loaded_at_utc > modified_at_utc THEN 1 ELSE 0 END) as invalid_timestamp_order,
  COUNT(DISTINCT customer_id) as distinct_customers,
  COUNT(*) as total_records
FROM main.d_silver.ods_customer;

-- Duplicate check
SELECT 
  customer_id,
  COUNT(*) as count
FROM main.d_silver.ods_customer
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY count DESC;

-- ============================================================================
-- SECTION 5: VIEW INACTIVE RECORDS
-- ============================================================================

-- View all inactive records (soft deleted)
SELECT 
  customer_id, 
  customer_name, 
  is_active,
  loaded_at_utc, 
  modified_at_utc
FROM main.d_silver.ods_customer
WHERE is_active = FALSE
ORDER BY modified_at_utc DESC;

-- Count of active vs inactive
SELECT 
  is_active,
  COUNT(*) as count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percent
FROM main.d_silver.ods_customer
GROUP BY is_active;

-- ============================================================================
-- SECTION 6: METADATA & BATCH TRACKING
-- ============================================================================

-- View recent batch executions
SELECT 
  batch_id,
  batch_start_time,
  batch_end_time,
  rows_processed,
  status
FROM main.d_silver.etl_batch_metadata
ORDER BY batch_start_time DESC
LIMIT 10;

-- Get last processed timestamp
SELECT MAX(last_processed_utc) as last_process_time 
FROM main.d_silver.etl_batch_metadata;

-- ============================================================================
-- SECTION 7: TABLE PROPERTIES
-- ============================================================================

-- Verify CDF enabled on Bronze
SHOW TBLPROPERTIES main.d_bronze.edp_customer_bronze;

-- View table schemas
DESCRIBE TABLE EXTENDED main.d_bronze.edp_customer_bronze;
DESCRIBE TABLE EXTENDED main.d_silver.ods_customer;

-- View Delta history
DESCRIBE HISTORY main.d_silver.ods_customer LIMIT 5;

-- ============================================================================
-- SECTION 8: CLEANUP (CAUTION!)
-- ============================================================================

-- Clear test data from Bronze
DELETE FROM main.d_bronze.edp_customer_bronze 
WHERE customer_id LIKE 'TEST%';

-- Truncate ODS (reset to empty)
-- TRUNCATE TABLE main.d_silver.ods_customer;

-- Drop all tables (DESTRUCTIVE!)
-- DROP TABLE IF EXISTS main.d_silver.ods_customer;
-- DROP TABLE IF EXISTS main.d_bronze.edp_customer_bronze;
-- DROP TABLE IF EXISTS main.d_silver.etl_batch_metadata;