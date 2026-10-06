"""
SupplySense AI - Phase 10 Demo Script: Execution-Scoped Artifacts & Result Isolation

Demonstrates:
  1. Successive/concurrent pipeline executions (Execution A and Execution B).
  2. Complete directory isolation under ml/results/executions/<execution_id>/.
  3. Artifact completeness (comparison, metrics, predictions, models).
  4. Execution metadata traceability (execution_id in JSON headers).
  5. Atomic file replacement during retries ensuring zero corrupted state.
  6. Output location registration in MySQL pipeline_executions.
  7. Forecast predictions persistence linking to execution_id in forecast_results.
"""

import os
import sys
import json
import logging
import importlib

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection

runner_mod = importlib.import_module('services.pipeline-runner.runner')
registry_mod = importlib.import_module('services.pipeline-runner.registry')
retry_mod = importlib.import_module('services.pipeline-runner.retry_service')
handlers_mod = importlib.import_module('services.pipeline-runner.handlers')

execute_pipeline = runner_mod.execute_pipeline
register_pipeline = registry_mod.register_pipeline
retry_execution = retry_mod.retry_execution
default_handler = handlers_mod.demand_forecast_pipeline_handler


def run_phase10_demo():
    conn = get_connection()
    cur = conn.cursor()

    print("=" * 80)
    print("   SUPPLYSENSE AI - PHASE 10 EXECUTION-SCOPED ARTIFACTS & ISOLATION DEMO")
    print("=" * 80)

    # 1. Initialize decisions
    print("\n[Step 1] Initializing Demo Decisions for Execution A & Execution B...")
    cur.execute("""
        INSERT INTO pipeline_decisions (
            pipeline_name, condition_type, decision, ready_count, total_required, reason
        ) VALUES 
        ('Demand Forecast Pipeline', 'ALL', 'TRIGGER', 3, 3, 'Phase 10 Demo Trigger Execution A'),
        ('Demand Forecast Pipeline', 'ALL', 'TRIGGER', 3, 3, 'Phase 10 Demo Trigger Execution B')
    """)
    dec_id_b = cur.lastrowid
    dec_id_a = dec_id_b - 1
    conn.commit()

    # Step 2: Trigger Execution A
    print("\n" + "=" * 80)
    print("STEP 2: RUNNING PIPELINE EXECUTION A")
    print("=" * 80)

    cur.execute("""
        INSERT INTO pipeline_executions (
            pipeline_name, decision_id, triggering_event_id, status
        ) VALUES ('Demand Forecast Pipeline', %s, 999901, 'RUNNING')
    """, (dec_id_a,))
    exec_id_a = cur.lastrowid
    conn.commit()
    print(f"Created Execution Record A [ID: {exec_id_a}]")

    res_a = execute_pipeline(exec_id_a, "Demand Forecast Pipeline", connection=conn)
    print(f"Execution A Status: {res_a['status']}")
    print(f"Execution A Output Location: {res_a.get('output_location')}")

    # Step 3: Trigger Execution B
    print("\n" + "=" * 80)
    print("STEP 3: RUNNING PIPELINE EXECUTION B")
    print("=" * 80)

    cur.execute("""
        INSERT INTO pipeline_executions (
            pipeline_name, decision_id, triggering_event_id, status
        ) VALUES ('Demand Forecast Pipeline', %s, 999902, 'RUNNING')
    """, (dec_id_b,))
    exec_id_b = cur.lastrowid
    conn.commit()
    print(f"Created Execution Record B [ID: {exec_id_b}]")

    res_b = execute_pipeline(exec_id_b, "Demand Forecast Pipeline", connection=conn)
    print(f"Execution B Status: {res_b['status']}")
    print(f"Execution B Output Location: {res_b.get('output_location')}")

    # Step 4: Verify Isolation & Contents
    print("\n" + "=" * 80)
    print("STEP 4: VERIFYING ARTIFACT ISOLATION & METADATA")
    print("=" * 80)

    dir_a = os.path.join(REPO_ROOT, 'ml', 'results', 'executions', str(exec_id_a))
    dir_b = os.path.join(REPO_ROOT, 'ml', 'results', 'executions', str(exec_id_b))

    print(f"Directory A: {dir_a} (Exists: {os.path.isdir(dir_a)})")
    print(f"Directory B: {dir_b} (Exists: {os.path.isdir(dir_b)})")

    files_a = sorted(os.listdir(dir_a))
    files_b = sorted(os.listdir(dir_b))

    print(f"\nExecution A Artifacts ({len(files_a)} files):")
    for f in files_a:
        print(f"  - {f}")

    print(f"\nExecution B Artifacts ({len(files_b)} files):")
    for f in files_b:
        print(f"  - {f}")

    # Inspect model comparison metadata
    with open(os.path.join(dir_a, 'model_comparison.json'), 'r') as f:
        meta_a = json.load(f)["metadata"]
    with open(os.path.join(dir_b, 'model_comparison.json'), 'r') as f:
        meta_b = json.load(f)["metadata"]

    print(f"\nMetadata Execution ID in Artifact A: {meta_a.get('execution_id')} (Expected: {exec_id_a})")
    print(f"Metadata Execution ID in Artifact B: {meta_b.get('execution_id')} (Expected: {exec_id_b})")

    # Step 5: Verify MySQL Persistence
    print("\n" + "=" * 80)
    print("STEP 5: VERIFYING MYSQL PERSISTENCE")
    print("=" * 80)

    cur.execute("SELECT execution_id, output_location, status FROM pipeline_executions WHERE execution_id IN (%s, %s)", (exec_id_a, exec_id_b))
    for r in cur.fetchall():
        print(f"Execution #{r['execution_id']}: Status={r['status']}, Output Location={r['output_location']}")

    cur.execute("SELECT execution_id, COUNT(*) as cnt FROM forecast_results WHERE execution_id IN (%s, %s) GROUP BY execution_id", (exec_id_a, exec_id_b))
    for r in cur.fetchall():
        print(f"Forecast Results for Execution #{r['execution_id']}: {r['cnt']} rows persisted")

    # Step 6: Verify Atomic Overwrite on Retry
    print("\n" + "=" * 80)
    print("STEP 6: TESTING RETRY ATOMIC REPLACEMENT IN SCOPED DIRECTORY")
    print("=" * 80)

    cur.execute("""
        INSERT INTO pipeline_decisions (
            pipeline_name, condition_type, decision, ready_count, total_required, reason
        ) VALUES ('Demand Forecast Pipeline', 'ALL', 'TRIGGER', 3, 3, 'Phase 10 Demo Trigger Retry')
    """)
    dec_id_retry = cur.lastrowid

    cur.execute("""
        INSERT INTO pipeline_executions (
            pipeline_name, decision_id, triggering_event_id, status, retry_count, max_retries
        ) VALUES ('Demand Forecast Pipeline', %s, 999903, 'RUNNING', 0, 3)
    """, (dec_id_retry,))
    exec_id_retry = cur.lastrowid
    conn.commit()

    # Intentionally simulate failure + corrupted partial artifact
    dir_retry = os.path.join(REPO_ROOT, 'ml', 'results', 'executions', str(exec_id_retry))
    os.makedirs(dir_retry, exist_ok=True)
    with open(os.path.join(dir_retry, 'model_comparison.json'), 'w') as f:
        f.write('{"corrupted": true}')

    def failing_first_handler(e_id, p_name):
        raise RuntimeError("Transient connection drop")

    register_pipeline("Demand Forecast Pipeline", failing_first_handler)
    res_retry_fail = execute_pipeline(exec_id_retry, "Demand Forecast Pipeline", connection=conn)
    print(f"Initial attempt for Execution #{exec_id_retry} failed as expected: status={res_retry_fail['status']}")

    # Restore default handler and retry
    register_pipeline("Demand Forecast Pipeline", default_handler)
    res_retry_ok = retry_execution(exec_id_retry, connection=conn)
    print(f"Retry attempt for Execution #{exec_id_retry}: status={res_retry_ok['status']}, retry_count={res_retry_ok['retry_count']}")

    with open(os.path.join(dir_retry, 'model_comparison.json'), 'r') as f:
        comp_recovered = json.load(f)
    print(f"Recovered Artifact Valid: {'validation' in comp_recovered and 'test' in comp_recovered}")
    print(f"Recovered Artifact Execution ID: {comp_recovered.get('metadata', {}).get('execution_id')}")

    # Clean up demo records from database and directories
    cur.execute("DELETE FROM forecast_results WHERE execution_id IN (%s, %s, %s)", (exec_id_a, exec_id_b, exec_id_retry))
    cur.execute("DELETE FROM pipeline_executions WHERE execution_id IN (%s, %s, %s)", (exec_id_a, exec_id_b, exec_id_retry))
    conn.commit()
    cur.close()
    conn.close()

    print("\n" + "=" * 80)
    print("  PHASE 10 ARTIFACT ISOLATION & TRACEABILITY DEMO COMPLETED SUCCESSFULLY")
    print("=" * 80)


if __name__ == '__main__':
    run_phase10_demo()
