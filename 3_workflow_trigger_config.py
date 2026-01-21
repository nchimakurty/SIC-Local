#!/usr/bin/env python3
# ============================================================================
# DATABRICKS WORKFLOW CONFIGURATION WITH TABLE TRIGGERS
# Complete JSON Config - Ready for Jobs API 2.1
# ============================================================================

import json

# ============================================================================
# COMPLETE WORKFLOW WITH TABLE TRIGGER (RECOMMENDED FOR MVP)
# ============================================================================

WORKFLOW_WITH_TABLE_TRIGGER = {
    "name": "bronze_to_ods_etl",
    "description": "Bronze-to-ODS ETL Pipeline with Table Trigger",
    "max_concurrent_runs": 1,
    "timeout_seconds": 3600,
    
    "tasks": [
        {
            "task_key": "merge_bronze_to_ods",
            "description": "MERGE Bronze data into ODS with upsert logic",
            "notebook_task": {
                "notebook_path": "/Workspace/Users/[YOUR_USER_ID]/bronze_to_ods_merge",
                "source": "WORKSPACE",
                "base_parameters": {
                    "catalog": "main",
                    "bronze_schema": "d_bronze",
                    "bronze_table": "edp_customer_bronze",
                    "ods_schema": "d_silver",
                    "ods_table": "ods_customer"
                }
            },
            "existing_cluster_id": "[CLUSTER_ID_HERE]",
            "timeout_seconds": 1800,
            "max_retries": 1,
            "min_retry_interval_millis": 60000
        }
    ],
    
    # ========================================================================
    # TABLE TRIGGER CONFIGURATION (Event-Driven Execution)
    # ========================================================================
    "trigger": {
        "trigger_type": "TABLE_UPDATE",
        "table_update_trigger_config": {
            "table_names": [
                "main.d_bronze.edp_customer_bronze"
            ],
            "min_time_between_triggers_seconds": 60,
            "wait_after_last_change_seconds": 30
        }
    },
    
    "tags": {
        "environment": "development",
        "pipeline": "bronze_to_ods",
        "team": "data_engineering"
    }
}

# ============================================================================
# ALTERNATIVE: SCHEDULED TRIGGER (Fixed Cron Schedule)
# ============================================================================

WORKFLOW_WITH_SCHEDULED_TRIGGER = {
    "name": "bronze_to_ods_etl",
    "description": "Bronze-to-ODS ETL Pipeline with Scheduled Trigger",
    "max_concurrent_runs": 1,
    "timeout_seconds": 3600,
    
    "tasks": [
        {
            "task_key": "merge_bronze_to_ods",
            "description": "MERGE Bronze data into ODS with upsert logic",
            "notebook_task": {
                "notebook_path": "/Workspace/Users/[YOUR_USER_ID]/bronze_to_ods_merge",
                "source": "WORKSPACE"
            },
            "existing_cluster_id": "[CLUSTER_ID_HERE]",
            "timeout_seconds": 1800
        }
    ],
    
    # ========================================================================
    # SCHEDULED TRIGGER (Alternative to Table Triggers)
    # ========================================================================
    "trigger": {
        "trigger_type": "PERIODIC",
        "periodic_trigger_config": {
            "interval": 5,
            "unit": "MINUTES"
        }
    }
}

# ============================================================================
# HOW TO CREATE WORKFLOW - STEP BY STEP UI INSTRUCTIONS
# ============================================================================

UI_SETUP_INSTRUCTIONS = """
MANUAL WORKFLOW SETUP (Recommended for MVP)
═══════════════════════════════════════════════════════════════════════════

STEP 1: Create Workflow Job
─────────────────────────────────────────────────────────────────────────
1. In Databricks, click Workflows (left sidebar)
2. Click "Create job"
3. Fill in:
   - Job name: bronze_to_ods_etl
   - Description: Bronze-to-ODS ETL Pipeline with Table Trigger

STEP 2: Add Notebook Task
─────────────────────────────────────────────────────────────────────────
1. Task name: merge_bronze_to_ods
2. Task type: Notebook
3. Notebook path: Select "bronze_to_ods_merge" (your notebook)
4. Cluster: Select existing cluster
5. Leave parameters blank (hardcoded in notebook)

STEP 3: Add Table Trigger
─────────────────────────────────────────────────────────────────────────
1. Click "Schedules & Triggers"
2. Click "Add trigger"
3. Trigger type: "Table update"
4. Table name: main.d_bronze.edp_customer_bronze
5. Click "Advanced"
6. Set:
   - Min time between triggers: 60 seconds
   - Wait after last change: 30 seconds
7. Click "Save"

STEP 4: Configure Notifications (Optional)
─────────────────────────────────────────────────────────────────────────
1. Click "Notifications"
2. Add email for failures (optional)
3. Add email for success (optional)

STEP 5: Save & Test
─────────────────────────────────────────────────────────────────────────
1. Click "Save"
2. Click "Run now"
3. Watch the Runs tab
4. Job should complete in 30-60 seconds

STEP 6: Test the Trigger
─────────────────────────────────────────────────────────────────────────
1. Go to SQL editor
2. Insert test data:
   INSERT INTO main.d_bronze.edp_customer_bronze 
   VALUES ('TESTME', 'Test User', 'test@example.com', true, 'hash', CURRENT_TIMESTAMP());
3. Wait 30-60 seconds
4. Check Workflow Runs tab - job should have auto-triggered!
"""

# ============================================================================
# API SETUP: Create Workflow via Databricks Jobs API 2.1
# ============================================================================

API_SETUP_INSTRUCTIONS = """
API CREATION (Advanced - for Infrastructure as Code)
═══════════════════════════════════════════════════════════════════════════

PREREQUISITES:
- Databricks workspace URL
- Personal Access Token (PAT)
- Python SDK installed: pip install databricks-sdk

OPTION A: Using Python SDK
─────────────────────────────────────────────────────────────────────────

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import CreateJob
import os

# Initialize client
w = WorkspaceClient(
    host=os.environ.get("DATABRICKS_HOST"),
    token=os.environ.get("DATABRICKS_TOKEN")
)

# Create job with table trigger
job = CreateJob(
    name="bronze_to_ods_etl",
    max_concurrent_runs=1,
    tasks=[{
        "task_key": "merge_bronze_to_ods",
        "notebook_task": {
            "notebook_path": "/path/to/bronze_to_ods_merge",
            "source": "WORKSPACE"
        },
        "existing_cluster_id": "your-cluster-id"
    }],
    trigger={
        "trigger_type": "TABLE_UPDATE",
        "table_update_trigger_config": {
            "table_names": ["main.d_bronze.edp_customer_bronze"],
            "min_time_between_triggers_seconds": 60,
            "wait_after_last_change_seconds": 30
        }
    }
)

response = w.jobs.create(job)
print(f"Job created with ID: {response.job_id}")


OPTION B: Using curl
─────────────────────────────────────────────────────────────────────────

DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
DATABRICKS_TOKEN="your-personal-access-token"

curl -X POST \\
  $DATABRICKS_HOST/api/2.1/jobs/create \\
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \\
  -H "Content-Type: application/json" \\
  -d '{
    "name": "bronze_to_ods_etl",
    "max_concurrent_runs": 1,
    "tasks": [{
      "task_key": "merge_bronze_to_ods",
      "notebook_task": {
        "notebook_path": "/path/to/bronze_to_ods_merge",
        "source": "WORKSPACE"
      },
      "existing_cluster_id": "your-cluster-id"
    }],
    "trigger": {
      "trigger_type": "TABLE_UPDATE",
      "table_update_trigger_config": {
        "table_names": ["main.d_bronze.edp_customer_bronze"],
        "min_time_between_triggers_seconds": 60,
        "wait_after_last_change_seconds": 30
      }
    }
  }'
"""

# ============================================================================
# TABLE TRIGGER CONFIGURATION DETAILS
# ============================================================================

TRIGGER_EXPLANATION = """
TABLE TRIGGER CONFIGURATION
═══════════════════════════════════════════════════════════════════════════

trigger_type: TABLE_UPDATE
│
├─ Monitors: main.d_bronze.edp_customer_bronze
│
├─ min_time_between_triggers_seconds: 60
│  └─ Minimum 60 seconds between job executions
│     (prevents job hammering if multiple updates occur)
│
└─ wait_after_last_change_seconds: 30
   └─ Wait 30 seconds after last table update before triggering
      (batches multiple updates within 30-second window)

TIMING EXAMPLE:
─────────────────────────────────────────────────────────────────────────

Update at 10:00:00
Update at 10:00:15 (within 30s window, not separate trigger)
Job starts at 10:00:45 (30s after last update)
Next job can start at 10:01:45 (min 60s between triggers)

Update at 10:02:00
Job starts at 10:02:30 (30s after update)

BENEFITS:
- Automatic execution (no cron needed)
- Event-driven (responds to data arrival)
- Configurable batching (prevents overhead)
- Scales naturally with data arrival rate
"""

# ============================================================================
# OUTPUT: Print all configurations
# ============================================================================

if __name__ == "__main__":
    print("=" * 80)
    print("DATABRICKS WORKFLOW & TABLE TRIGGER CONFIGURATION")
    print("=" * 80)
    
    print("\n\n[1] UI SETUP INSTRUCTIONS")
    print(UI_SETUP_INSTRUCTIONS)
    
    print("\n\n[2] API SETUP INSTRUCTIONS")
    print(API_SETUP_INSTRUCTIONS)
    
    print("\n\n[3] TABLE TRIGGER CONFIGURATION DETAILS")
    print(TRIGGER_EXPLANATION)
    
    print("\n\n[4] WORKFLOW JSON (TABLE TRIGGER)")
    print(json.dumps(WORKFLOW_WITH_TABLE_TRIGGER, indent=2))
    
    print("\n\n[5] WORKFLOW JSON (SCHEDULED TRIGGER - ALTERNATIVE)")
    print(json.dumps(WORKFLOW_WITH_SCHEDULED_TRIGGER, indent=2))
    
    print("\n" + "=" * 80)
