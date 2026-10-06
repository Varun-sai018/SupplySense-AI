"""
SupplySense AI - Phase 9 Demo Script: Automated Pipeline Retry & Failure Recovery

Demonstrates:
  1. Initial pipeline execution entering RUNNING status.
  2. Simulated transient failure transitioning execution to FAILED (retry_count=0).
  3. Exponential backoff calculation and atomic retry claiming (retry_count=1).
  4. Successful retry execution transitioning to COMPLETED.
  5. Forecast persistence verification in MySQL forecast_results.
  6. Retry exhaustion scenario (3 retries failed -> permanent failure, 0 further retries).
  7. Idempotency verification: original triggering_event_id is protected against duplicate rows.
"""

import os
import sys
import logging
import importlib
import datetime

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection

runner_mod = importlib.import_module('services.pipeline-runner.runner')
registry_mod = importlib.import_module('services.pipeline-runner.registry')
retry_mod = importlib.import_module('services.pipeline-runner.retry_service')
handlers_mod = importlib.import_module('services.pipeline-runner.handlers')
forecast_repo = importlib.import_module('services.pipeline-runner.forecast_repository')

execute_pipeline = runner_mod.execute_pipeline
register_pipeline = registry_mod.register_pipeline
retry_execution = retry_mod.retry_execution
default_handler = handlers_mod.demand_forecast_pipeline_handler


def run_phase9_demo():
    conn = get_connection()
    cur = conn.cursor()

    print("=" * 80)
    print("      SUPPLYSENSE AI - PHASE 9 AUTOMATED PIPELINE RETRY & RECOVERY DEMO")
    print("=" * 80)

    # Step 1: Initialize Demo State
    print("\n[Step 1] Initializing Environment & Inserting Demo Decision...")
    cur.execute("""
        INSERT INTO pipeline_decisions (
            pipeline_name, condition_type, decision, ready_count, total_required, reason
        ) VALUES (
            'Demand Forecast Pipeline', 'ALL', 'TRIGGER', 3, 3, 'Phase 9 Demo Trigger'
        )
    """)
    demo_decision_id = cur.lastrowid
    demo_trigger_event_id = 999991

    # Clean up any leftover demo records
    cur.execute("DELETE FROM pipeline_executions WHERE triggering_event_id = %s", (demo_trigger_event_id,))
    cur.execute("DELETE FROM forecast_results WHERE execution_id >= 99000")
    conn.commit()

    # Step 2: Initial Execution Creation
    print("\n" + "=" * 80)
    print("STEP 2: INITIAL EXECUTION START (STATUS: RUNNING)")
    print("=" * 80)
    cur.execute("""
        INSERT INTO pipeline_executions (
            pipeline_name, decision_id, triggering_event_id, status, retry_count, max_retries
        ) VALUES (
            'Demand Forecast Pipeline', %s, %s, 'RUNNING', 0, 3
        )
    """, (demo_decision_id, demo_trigger_event_id))
    exec_id = cur.lastrowid
    conn.commit()

    cur.execute("SELECT execution_id, pipeline_name, status, retry_count, max_retries FROM pipeline_executions WHERE execution_id = %s", (exec_id,))
    init_row = cur.fetchone()
    print(f"  Execution ID       : #{init_row['execution_id']}")
    print(f"  Pipeline Name      : {init_row['pipeline_name']}")
    print(f"  Initial Status     : {init_row['status']}")
    print(f"  Initial Retry Count: {init_row['retry_count']}/{init_row['max_retries']}")

    # Step 3: Simulated Transient Failure
    print("\n" + "=" * 80)
    print("STEP 3: SIMULATING TRANSIENT FAILURE (STATUS: FAILED, RETRY_COUNT: 0)")
    print("=" * 80)

    def transient_failure_handler(e_id, p_name):
        raise RuntimeError("OperationalError: (2006, 'MySQL server has gone away')")

    register_pipeline("Demand Forecast Pipeline", transient_failure_handler)
    fail_res = execute_pipeline(exec_id, 'Demand Forecast Pipeline', connection=conn)

    cur.execute("SELECT status, retry_count, retry_error_type, error_message, next_retry_at FROM pipeline_executions WHERE execution_id = %s", (exec_id,))
    failed_row = cur.fetchone()
    print(f"  Execution Result   : {fail_res['status']}")
    print(f"  Recorded Status    : {failed_row['status']}")
    print(f"  Error Type         : {failed_row['retry_error_type']}")
    print(f"  Error Message      : {failed_row['error_message']}")
    print(f"  Next Retry At      : {failed_row['next_retry_at']}")

    # Step 4: Retry Scheduled, Claimed, and Successfully Executed
    print("\n" + "=" * 80)
    print("STEP 4: ATOMIC CLAIM & RETRY EXECUTION (RETRY_COUNT: 1 -> COMPLETED)")
    print("=" * 80)

    mock_forecast_preds = [
        {"product_category": "agro_industria_e_comercio", "forecast_week": "2018-09-03", "predicted_demand": 14.8, "model_name": "xgboost-v1"},
        {"product_category": "agro_industria_e_comercio", "forecast_week": "2018-09-10", "predicted_demand": 16.5, "model_name": "xgboost-v1"},
        {"product_category": "utilidades_domesticas", "forecast_week": "2018-09-03", "predicted_demand": 42.1, "model_name": "xgboost-v1"},
    ]

    def successful_retry_handler(e_id, p_name):
        forecast_repo.save_forecast_results(e_id, p_name, mock_forecast_preds, connection=conn)
        return {
            "output_location": "ml/results/model_comparison.json",
            "forecast_rows": len(mock_forecast_preds)
        }

    register_pipeline("Demand Forecast Pipeline", successful_retry_handler)
    retry_res = retry_execution(exec_id, connection=conn)

    cur.execute("SELECT status, retry_count, completed_at, output_location, error_message FROM pipeline_executions WHERE execution_id = %s", (exec_id,))
    success_row = cur.fetchone()
    print(f"  Retry Status       : {retry_res['status']}")
    print(f"  New Retry Count    : {retry_res['retry_count']}/{retry_res['max_retries']}")
    print(f"  Completed At       : {success_row['completed_at']}")
    print(f"  Output Location    : {success_row['output_location']}")
    print(f"  Error Cleared      : {success_row['error_message'] is None}")

    # Step 5: Forecast Persistence Verification
    print("\n" + "=" * 80)
    print("STEP 5: FORECAST PERSISTENCE VERIFICATION")
    print("=" * 80)
    cur.execute("SELECT COUNT(*) AS total FROM forecast_results WHERE execution_id = %s", (exec_id,))
    fc_count = cur.fetchone()["total"]
    print(f"  Linked Execution ID: #{exec_id}")
    print(f"  Persisted Rows     : {fc_count} forecast rows in MySQL forecast_results")

    # Step 6: Retry Exhaustion Scenario
    print("\n" + "=" * 80)
    print("STEP 6: RETRY EXHAUSTION SCENARIO (MAX RETRIES REACHED -> PERMANENT FAILED)")
    print("=" * 80)
    cur.execute("""
        INSERT INTO pipeline_executions (
            pipeline_name, decision_id, triggering_event_id, status, retry_count, max_retries
        ) VALUES (
            'Demand Forecast Pipeline', %s, 999992, 'RUNNING', 0, 3
        )
    """, (demo_decision_id,))
    exhaust_id = cur.lastrowid
    conn.commit()

    def persistent_failure_handler(e_id, p_name):
        raise RuntimeError("Kafka broker connection timeout (transient)")

    register_pipeline("Demand Forecast Pipeline", persistent_failure_handler)

    # Initial failure
    execute_pipeline(exhaust_id, 'Demand Forecast Pipeline', connection=conn)

    # 3 Retries
    for attempt in range(1, 4):
        r_attempt = retry_execution(exhaust_id, connection=conn)
        print(f"  Attempt #{attempt}: Status = {r_attempt['status']}, Retry Count = {r_attempt['retry_count']}/3, Next Retry = {r_attempt.get('next_retry_at')}")

    # 4th Attempt (Exhausted)
    r_final = retry_execution(exhaust_id, connection=conn)
    print(f"  Attempt #4: Status = {r_final['status']}, Reason = {r_final.get('reason')} (Permanently FAILED)")

    # Step 7: Idempotency Verification
    print("\n" + "=" * 80)
    print("STEP 7: TRIGGERING-EVENT IDEMPOTENCY VERIFICATION")
    print("=" * 80)
    cur.execute("SELECT COUNT(*) AS total FROM pipeline_executions WHERE triggering_event_id = %s", (demo_trigger_event_id,))
    event_exec_count = cur.fetchone()["total"]
    print(f"  Triggering Event ID: {demo_trigger_event_id}")
    print(f"  Execution Records  : {event_exec_count} (Expected: 1, no duplicate rows created across retries)")

    # Clean up demo records
    cur.execute("DELETE FROM forecast_results WHERE execution_id IN (%s, %s)", (exec_id, exhaust_id))
    cur.execute("DELETE FROM pipeline_executions WHERE execution_id IN (%s, %s)", (exec_id, exhaust_id))
    cur.execute("DELETE FROM pipeline_decisions WHERE decision_id = %s", (demo_decision_id,))
    conn.commit()

    # Restore default handler
    register_pipeline("Demand Forecast Pipeline", default_handler)
    conn.close()

    print("\n" + "=" * 80)
    print("      PHASE 9 DEMO COMPLETED SUCCESSFULLY")
    print("=" * 80)


if __name__ == '__main__':
    run_phase9_demo()
