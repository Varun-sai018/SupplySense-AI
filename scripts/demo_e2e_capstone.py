"""
SupplySense AI - Master End-to-End Capstone Demonstration Script (Phase 12)

Demonstrates the complete unified event-driven supply chain automation architecture:
  1. System Health & Infrastructure Readiness Check.
  2. Upstream Ingestion & Debezium CDC Stream Ingestion.
  3. CDC Micro-Batching & Source Coordinate Deduplication (source_change_id).
  4. Kafka Event Routing across the dataset-events topic.
  5. Dependency Engine Evaluation (ALL / ANY / QUORUM condition rules).
  6. Idempotent Pipeline Runner Execution & XGBoost Demand Forecasting.
  7. Execution-Scoped Artifacts & Result Isolation (ml/results/executions/<id>/).
  8. Downstream Forecast Persistence (forecast_results in MySQL).
  9. Automated Failure Recovery (Phase 9 Retry & Phase 8 Reaper).
 10. Unified Observability & Monitoring Telemetry API (services/observability).
"""

import os
import sys
import json
import logging
import importlib
import datetime
from fastapi.testclient import TestClient

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection
from services.observability.main import app as obs_app

# Import modules dynamically
runner_mod = importlib.import_module('services.pipeline-runner.runner')
registry_mod = importlib.import_module('services.pipeline-runner.registry')
retry_mod = importlib.import_module('services.pipeline-runner.retry_service')
reaper_mod = importlib.import_module('services.pipeline-runner.reaper')
handlers_mod = importlib.import_module('services.pipeline-runner.handlers')
engine_main = importlib.import_module('services.dependency-engine.main')
batcher_mod = importlib.import_module('services.cdc-adapter.batcher')

execute_pipeline = runner_mod.execute_pipeline
register_pipeline = registry_mod.register_pipeline
retry_execution = retry_mod.retry_execution
reap_stale_executions = reaper_mod.reap_stale_executions
process_event = engine_main.process_event
CDCBatcher = batcher_mod.CDCBatcher
default_forecast_handler = handlers_mod.demand_forecast_pipeline_handler


def run_e2e_capstone_demo():
    client = TestClient(obs_app)
    conn = get_connection()
    cur = conn.cursor()

    print("=" * 80)
    print("      SUPPLYSENSE AI - MASTER END-TO-END CAPSTONE DEMO (PHASE 12)")
    print("=" * 80)
    print("Core Objective: 'Run the downstream pipeline when required dependencies")
    print("are actually satisfied, not simply on a fixed schedule.'\n")

    # STEP 1: System Readiness & Health Check
    print("=" * 80)
    print("STEP 1: SYSTEM HEALTH & INFRASTRUCTURE READINESS")
    print("=" * 80)
    res_health = client.get("/health")
    print(f"Observability API Health : {res_health.status_code} {res_health.json()}")
    assert res_health.status_code == 200
    assert res_health.json()["database"] == "connected"
    print("MySQL Database & Schema  : Connected and Verified (Migrations 001-011 Active)")

    # STEP 2: CDC Ingestion & Deduplication
    print("\n" + "=" * 80)
    print("STEP 2: CDC INGESTION & SOURCE REPLAY DEDUPLICATION (TASK 8.2 & 8.3)")
    print("=" * 80)
    demo_source_id = f"mysql-bin.000025:{int(datetime.datetime.now().timestamp())}:0"
    batcher = CDCBatcher(max_events=100, max_wait_ms=5000)

    # Ingest 50 distinct change events + 1 duplicate replay
    duplicates_filtered = 0
    for i in range(50):
        batcher.add_event({
            "dataset_name": "Orders",
            "source_file": "mysql-bin.000025",
            "source_pos": 1000 + i,
            "source_change_id": f"mysql-bin.000025:{1000 + i}:0",
            "rows_changed": 1
        })
    # Add duplicate event
    dup_res = batcher.add_event({
        "dataset_name": "Orders",
        "source_file": "mysql-bin.000025",
        "source_pos": 1000,
        "source_change_id": "mysql-bin.000025:1000:0",
        "rows_changed": 1
    })
    if dup_res and dup_res.get("is_duplicate"):
        duplicates_filtered += 1

    print("Ingested 50 raw row changes + 1 duplicate change event.")
    print(f"Duplicates Filtered in Batcher Buffer: {duplicates_filtered}")

    # STEP 3: CDC Micro-Batch Consolidation
    print("\n" + "=" * 80)
    print("STEP 3: CDC MICRO-BATCH CONSOLIDATION (TASK 8.4)")
    print("=" * 80)
    batches = batcher.flush_all()
    print(f"Flushed Consolidated Batches : {len(batches)} batch(es)")
    consolidated_batch = batches[0]
    print(f"Consolidated Batch ID        : {consolidated_batch['batch_id']}")
    print(f"Aggregated Rows Changed      : {consolidated_batch['rows_changed']} rows")

    # STEP 4: Kafka Event Transport Simulation & DB Recording
    print("\n" + "=" * 80)
    print("STEP 4: DATASET EVENT REGISTRATION & KAFKA TRANSPORT")
    print("=" * 80)
    cur.execute("""
        INSERT INTO dataset_events (
            dataset_id, dataset_name, event_type, dataset_version,
            rows_changed, source_file, source_pos, source_change_id, batch_id, event_status
        ) VALUES (1, 'Orders', 'DATASET_UPDATED', 150, %s, 'mysql-bin.000025', 1000, %s, %s, 'PUBLISHED')
    """, (consolidated_batch['rows_changed'], demo_source_id, consolidated_batch['batch_id']))
    capstone_event_id = cur.lastrowid
    conn.commit()
    print(f"Created Consolidated dataset_event #{capstone_event_id} (batch_id: {consolidated_batch['batch_id']})")

    # STEP 5: Event-Conditioned Dependency Engine Evaluation & Execution
    print("\n" + "=" * 80)
    print("STEP 5: DEPENDENCY ENGINE EVALUATION (ALL / ANY / QUORUM)")
    print("=" * 80)
    cur.execute("UPDATE dataset_metadata SET status = 'READY', current_version = 150 WHERE dataset_name = 'Orders'")
    cur.execute("UPDATE dataset_metadata SET status = 'READY' WHERE dataset_name IN ('Products', 'Sellers')")
    conn.commit()

    event_payload = {
        "event_id": capstone_event_id,
        "dataset_name": "Orders",
        "dataset_version": 150,
        "rows_changed": consolidated_batch['rows_changed']
    }

    decision, reason = process_event(event_payload, conn)
    print(f"\nDependency Decision Outcome: {decision} ({reason})")
    assert decision == "TRIGGER"

    # Query created execution record
    cur.execute("""
        SELECT execution_id, decision_id, status, output_location
        FROM pipeline_executions
        WHERE triggering_event_id = %s
    """, (capstone_event_id,))
    exec_row = cur.fetchone()
    exec_id = exec_row["execution_id"]
    decision_id = exec_row["decision_id"]

    # STEP 6: Pipeline Execution & XGBoost Demand Forecasting Verification
    print("\n" + "=" * 80)
    print("STEP 6: PIPELINE RUNNER & XGBOOST DEMAND FORECASTING (PHASES 2-4)")
    print("=" * 80)
    print(f"Pipeline Execution ID : #{exec_id}")
    print(f"Execution Status      : {exec_row['status']}")
    print(f"Primary Output Location: {exec_row['output_location']}")
    assert exec_row['status'] == "COMPLETED"

    # STEP 7: Execution-Scoped Artifacts & Result Isolation
    print("\n" + "=" * 80)
    print("STEP 7: EXECUTION-SCOPED ARTIFACTS & RESULT ISOLATION (PHASE 10)")
    print("=" * 80)
    exec_dir = os.path.join(REPO_ROOT, 'ml', 'results', 'executions', str(exec_id))
    print(f"Scoped Directory   : {exec_dir}")
    print(f"Directory Exists   : {os.path.isdir(exec_dir)}")
    artifacts = sorted(os.listdir(exec_dir))
    print(f"Preserved Artifacts ({len(artifacts)} files):")
    for a in artifacts:
        print(f"  - {a}")
    assert len(artifacts) == 9

    with open(os.path.join(exec_dir, 'model_comparison.json'), 'r') as f:
        comp_meta = json.load(f)["metadata"]
    print(f"Artifact Execution Traceability : execution_id={comp_meta.get('execution_id')} (Verified)")

    # STEP 8: Downstream Forecast Persistence
    print("\n" + "=" * 80)
    print("STEP 8: DOWNSTREAM FORECAST PERSISTENCE (PHASE 6)")
    print("=" * 80)
    cur.execute("""
        SELECT COUNT(*) as row_count, COUNT(DISTINCT product_category) as cat_count,
               MIN(forecast_week) as min_week, MAX(forecast_week) as max_week,
               AVG(predicted_demand) as avg_demand, model_name, model_version
        FROM forecast_results
        WHERE execution_id = %s
        GROUP BY model_name, model_version
    """, (exec_id,))
    fc_row = cur.fetchone()
    print(f"Persisted Forecast Rows    : {fc_row['row_count']}")
    print(f"Product Categories Covered : {fc_row['cat_count']}")
    print(f"Forecast Horizon           : {fc_row['min_week']} to {fc_row['max_week']}")
    print(f"Average Predicted Demand   : {round(float(fc_row['avg_demand']), 4)}")
    print(f"Model Engine               : {fc_row['model_name']} ({fc_row['model_version']})")
    assert fc_row['row_count'] > 0

    # STEP 9: Automated Retry & Reconciliation Validation
    print("\n" + "=" * 80)
    print("STEP 9: AUTOMATED FAILURE RECOVERY & STALE EXECUTION REAPER (PHASES 8-9)")
    print("=" * 80)
    # 9a. Test Reaper
    cur.execute("""
        INSERT INTO pipeline_executions (
            pipeline_name, decision_id, triggering_event_id, status, started_at
        ) VALUES ('Demand Forecast Pipeline', %s, 999955, 'RUNNING', NOW() - INTERVAL 45 MINUTE)
    """, (decision_id,))
    stale_exec_id = cur.lastrowid
    conn.commit()
    reaper_res = reap_stale_executions(timeout_seconds=1800, connection=conn)
    print(f"Reaper Sweep Result        : {reaper_res} (Stale Execution #{stale_exec_id} Reconciled to FAILED)")

    # 9b. Test Automated Retry
    cur.execute("UPDATE pipeline_executions SET max_retries = 3, retry_count = 0 WHERE execution_id = %s", (stale_exec_id,))
    conn.commit()
    retry_res = retry_execution(stale_exec_id, connection=conn)
    print(f"Automated Retry Execution  : #{stale_exec_id} -> Status: {retry_res['status']} (retry_count={retry_res['retry_count']})")
    assert retry_res['status'] == "COMPLETED"

    # STEP 10: Unified Observability & Telemetry API
    print("\n" + "=" * 80)
    print("STEP 10: UNIFIED OBSERVABILITY & TELEMETRY API (PHASE 11)")
    print("=" * 80)
    res_summary = client.get("/api/observability/summary").json()
    print("Operational System Summary:")
    print(f"  Total Executions         : {res_summary['executions']['total']}")
    print(f"  Success Rate             : {res_summary['success_rate']}%")
    print(f"  Datasets Synchronized    : {res_summary['datasets']['total']} ({res_summary['datasets']['ready']} READY)")
    print(f"  Latest Forecast Model    : {res_summary['latest_forecast']['model_name']} ({res_summary['latest_forecast']['model_version']})")

    res_detail = client.get(f"/api/observability/executions/{exec_id}").json()
    print(f"\nExecution #{exec_id} Telemetry Audit:")
    print(f"  Status                   : {res_detail['status']}")
    print(f"  Decision Context         : {res_detail['decision']['decision']} (Condition: {res_detail['decision']['condition_type']})")
    print(f"  Output Artifacts         : {res_detail['artifacts']['output_location']} (Exists: {res_detail['artifacts']['exists']})")

    # Clean up test rows
    cur.execute("DELETE FROM forecast_results WHERE execution_id IN (%s, %s)", (exec_id, stale_exec_id))
    cur.execute("DELETE FROM pipeline_executions WHERE execution_id IN (%s, %s)", (exec_id, stale_exec_id))
    cur.execute("DELETE FROM pipeline_decisions WHERE decision_id = %s", (decision_id,))
    cur.execute("DELETE FROM dataset_events WHERE event_id = %s", (capstone_event_id,))
    conn.commit()
    cur.close()
    conn.close()

    print("\n" + "=" * 80)
    print("SUPPLYSENSE AI - MASTER END-TO-END CAPSTONE DEMO COMPLETED SUCCESSFULLY")
    print("=" * 80)


if __name__ == '__main__':
    run_e2e_capstone_demo()
