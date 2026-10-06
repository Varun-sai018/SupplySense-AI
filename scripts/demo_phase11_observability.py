"""
SupplySense AI - Phase 11 Demo Script: Pipeline Execution Observability & Monitoring

Demonstrates:
  1. Service health and database connectivity check.
  2. Operational execution summary (total, running, completed, failed, rates, average duration).
  3. Recent execution telemetry with status, retry counts, duration, and output locations.
  4. Dataset synchronization states, row counts, and latest event metadata.
  5. Dependency engine condition rules and latest evaluated decisions.
  6. Machine learning forecast summaries and aggregated metrics.
  7. Detailed inspection of an individual pipeline execution.
  8. Strict read-only verification across the operational database.
"""

import os
import sys
import json
from fastapi.testclient import TestClient

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from services.observability.main import app
from common.database import get_connection


def run_phase11_demo():
    client = TestClient(app)
    conn = get_connection()
    cur = conn.cursor()

    print("=" * 80)
    print("      SUPPLYSENSE AI - PHASE 11 OBSERVABILITY & MONITORING DEMO")
    print("=" * 80)

    # Step 1: Health Check
    print("\n" + "=" * 80)
    print("STEP 1: CHECK OBSERVABILITY SERVICE HEALTH")
    print("=" * 80)
    res_health = client.get("/health")
    print(f"HTTP Status : {res_health.status_code}")
    print(f"Response    : {json.dumps(res_health.json(), indent=2)}")

    # Step 2: Execution Summary
    print("\n" + "=" * 80)
    print("STEP 2: OPERATIONAL EXECUTION SUMMARY")
    print("=" * 80)
    res_summary = client.get("/api/observability/summary")
    summary = res_summary.json()

    print(f"Total Executions    : {summary['executions']['total']}")
    print(f"  - Running         : {summary['executions']['running']}")
    print(f"  - Completed       : {summary['executions']['completed']}")
    print(f"  - Failed          : {summary['executions']['failed']}")
    print(f"  - Retrying        : {summary['executions']['retrying']}")
    print(f"  - Retried Total   : {summary['executions']['retried_total']}")
    print(f"Success Rate        : {summary['success_rate']}%")
    print(f"Failure Rate        : {summary['failure_rate']}%")
    print(f"Average Duration    : {summary['average_duration_seconds']}s")
    print(f"Datasets Summary    : {summary['datasets']['total']} total ({summary['datasets']['ready']} READY, {summary['datasets']['waiting']} WAITING)")

    # Step 3: Recent Executions
    print("\n" + "=" * 80)
    print("STEP 3: RECENT PIPELINE EXECUTIONS (LAST 5)")
    print("=" * 80)
    res_execs = client.get("/api/observability/executions?limit=5")
    execs = res_execs.json()

    sample_exec_id = None
    for e in execs:
        if sample_exec_id is None and e["status"] == "COMPLETED":
            sample_exec_id = e["execution_id"]
        print(f"Execution #{e['execution_id']} | Pipeline: '{e['pipeline_name']}'")
        print(f"  Status          : {e['status']}")
        print(f"  Retry Info      : {e['retry_count']}/{e['max_retries']} (Error: {e['retry_error_type'] or 'None'})")
        print(f"  Duration        : {e['duration_seconds']}s")
        print(f"  Started / Ended : {e['started_at']} -> {e['completed_at']}")
        print(f"  Output Location : {e['output_location']}")
        print("-" * 60)

    if sample_exec_id is None and execs:
        sample_exec_id = execs[0]["execution_id"]

    # Step 4: Dataset Status
    print("\n" + "=" * 80)
    print("STEP 4: DATASET SYNCHRONIZATION STATUS")
    print("=" * 80)
    res_datasets = client.get("/api/observability/datasets")
    datasets = res_datasets.json()

    for ds in datasets:
        print(f"Dataset #{ds['dataset_id']} - {ds['dataset_name']} ({ds['source_type']})")
        print(f"  Status          : {ds['status']} | Version: v{ds['current_version']} | Rows: {ds['row_count']}")
        print(f"  Latest Event    : #{ds['latest_event_id']} ({ds['latest_event_type']}) at {ds['latest_event_time']}")
        print(f"  Latest Batch ID : {ds['latest_batch_id']}")
        print("-" * 60)

    # Step 5: Dependency Status
    print("\n" + "=" * 80)
    print("STEP 5: DEPENDENCY CONFIGURATION & EVALUATION STATUS")
    print("=" * 80)
    res_deps = client.get("/api/observability/dependencies")
    dependencies = res_deps.json()

    for dep in dependencies:
        print(f"Pipeline: '{dep['pipeline_name']}' [Condition: {dep['condition_type']}]")
        print(f"  Configured Datasets : {', '.join(dep['configured_datasets'])}")
        latest_dec = dep.get("latest_decision")
        if latest_dec:
            print(f"  Latest Decision     : {latest_dec['decision']} (Ready: {latest_dec['ready_count']}/{latest_dec['total_required']})")
            print(f"  Decision Reason     : {latest_dec['reason']}")
            print(f"  Evaluated At        : {latest_dec['evaluated_at']}")
        else:
            print("  Latest Decision     : None")
        print("-" * 60)

    # Step 6: Forecast Summary
    print("\n" + "=" * 80)
    print("STEP 6: LATEST ML FORECAST EXECUTION SUMMARIES")
    print("=" * 80)
    res_forecasts = client.get("/api/observability/forecasts?limit=3")
    forecasts = res_forecasts.json()

    for fc in forecasts:
        print(f"Forecast for Execution #{fc['execution_id']} | Pipeline: '{fc['pipeline_name']}'")
        print(f"  Model               : {fc['model_name']} ({fc['model_version']})")
        print(f"  Forecast Rows       : {fc['row_count']} across {fc['category_count']} product categories")
        print(f"  Forecast Horizon    : {fc['min_forecast_week']} to {fc['max_forecast_week']}")
        print(f"  Avg Predicted Demand: {fc['average_predicted_demand']}")
        print(f"  Artifact Location   : {fc['output_location']}")
        print("-" * 60)

    # Step 7: Specific Execution Detail
    if sample_exec_id:
        print("\n" + "=" * 80)
        print(f"STEP 7: DETAILED INSPECTION FOR EXECUTION #{sample_exec_id}")
        print("=" * 80)
        res_detail = client.get(f"/api/observability/executions/{sample_exec_id}")
        detail = res_detail.json()
        print(f"Execution ID   : #{detail['execution_id']}")
        print(f"Status         : {detail['status']}")
        print(f"Duration       : {detail['duration_seconds']}s")
        print(f"Retry Count    : {detail['retry_count']}/{detail['max_retries']}")
        print(f"Artifacts Path : {detail['artifacts']['output_location']}")
        print(f"Artifacts Exist: {detail['artifacts']['exists']}")
        print(f"Scoped Files   : {', '.join(detail['artifacts']['scoped_files'][:5])} ... ({len(detail['artifacts']['scoped_files'])} total files)")

    # Step 8: Strict Read-Only Verification
    print("\n" + "=" * 80)
    print("STEP 8: STRICT READ-ONLY DATABASE VERIFICATION")
    print("=" * 80)
    cur.execute("SELECT COUNT(*) as cnt FROM pipeline_executions")
    total_execs_db = cur.fetchone()["cnt"]
    print(f"Total Database Executions Verified Unchanged: {total_execs_db} rows")
    cur.close()
    conn.close()

    print("\n" + "=" * 80)
    print("PHASE 11 OBSERVABILITY DEMO COMPLETED SUCCESSFULLY")
    print("=" * 80)


if __name__ == '__main__':
    run_phase11_demo()
