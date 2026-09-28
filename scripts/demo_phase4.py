"""
SupplySense AI - Phase 4 Demo Script

Demonstrates:
  - SCENARIO A (BLOCK): Orders READY (1/3) -> Decision: BLOCK, no pipeline executed.
  - SCENARIO B (TRIGGER): Products READY (2/3 -> BLOCK), Sellers READY (3/3 -> TRIGGER) -> Pipeline executed to COMPLETED.
  - Idempotency check: Replaying the triggering event skips execution.
  - SQL verification: Queries database tables to display execution records and artifact paths.
"""

import os
import sys
import importlib
import json

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection

event_gen = importlib.import_module('services.event-generator.main')
dep_engine = importlib.import_module('services.dependency-engine.main')


def run_demo():
    conn = get_connection()
    cur = conn.cursor()

    print("=" * 80)
    print("      SUPPLYSENSE AI - PHASE 4 PIPELINE EXECUTION DEMO")
    print("=" * 80)

    # 1. Reset initial states for Olist dependencies
    print("\n[Step 0] Initializing Dependency Configuration (Condition: ALL)...")
    cur.execute("UPDATE dataset_metadata SET status = 'WAITING' WHERE dataset_name IN ('Orders', 'Products', 'Sellers')")
    cur.execute("UPDATE pipeline_dependencies SET condition_type = 'ALL', required_count = NULL WHERE pipeline_name = 'Demand Forecast Pipeline'")
    conn.commit()

    # 2. SCENARIO A: BLOCK Demo
    print("\n" + "=" * 80)
    print("SCENARIO A: BLOCK DEMO (1/3 Datasets READY under ALL condition)")
    print("=" * 80)
    print("Generating update event for 'Orders' dataset...")
    event_gen.generate_event('Orders', 25)

    cur.execute("SELECT * FROM dataset_events WHERE dataset_name = 'Orders' ORDER BY event_id DESC LIMIT 1")
    event_orders = cur.fetchone()

    print(f"\nEvaluating Event #{event_orders['event_id']} ({event_orders['dataset_name']})...")
    decision_a, reason_a = dep_engine.process_event(event_orders, conn)

    print(f"\n[Verification A] Decision: {decision_a} | Reason: {reason_a}")
    cur.execute("SELECT COUNT(*) as count FROM pipeline_executions WHERE triggering_event_id = %s", (event_orders['event_id'],))
    count_a = cur.fetchone()['count']
    print(f"[Verification A] Pipeline executions created: {count_a} (Expected: 0)")
    assert count_a == 0, f"Expected 0 executions on BLOCK, found {count_a}"

    # 3. SCENARIO B: TRIGGER Demo
    print("\n" + "=" * 80)
    print("SCENARIO B: TRIGGER DEMO (Progressing to 3/3 Datasets READY under ALL condition)")
    print("=" * 80)
    
    # Step B1: Products updated (2/3 -> BLOCK)
    print("Generating update event for 'Products' dataset...")
    event_gen.generate_event('Products', 10)
    cur.execute("SELECT * FROM dataset_events WHERE dataset_name = 'Products' ORDER BY event_id DESC LIMIT 1")
    event_products = cur.fetchone()
    decision_b1, reason_b1 = dep_engine.process_event(event_products, conn)
    print(f"[Verification B1] Decision: {decision_b1} | Reason: {reason_b1}")

    # Step B2: Sellers updated (3/3 -> TRIGGER)
    print("\nGenerating update event for 'Sellers' dataset...")
    event_gen.generate_event('Sellers', 5)
    cur.execute("SELECT * FROM dataset_events WHERE dataset_name = 'Sellers' ORDER BY event_id DESC LIMIT 1")
    event_sellers = cur.fetchone()
    decision_b2, reason_b2 = dep_engine.process_event(event_sellers, conn)
    print(f"[Verification B2] Decision: {decision_b2} | Reason: {reason_b2}")

    # Step B3: SQL Verification
    print("\n" + "=" * 80)
    print("SQL EXECUTION RECORD VERIFICATION")
    print("=" * 80)
    cur.execute("""
        SELECT execution_id, pipeline_name, decision_id, triggering_event_id, status, started_at, completed_at, output_location, error_message
        FROM pipeline_executions
        WHERE triggering_event_id = %s
    """, (event_sellers['event_id'],))
    record = cur.fetchone()

    for k, v in record.items():
        print(f"  {k:<22}: {v}")

    assert record['status'] == 'COMPLETED', f"Expected status COMPLETED, got {record['status']}"
    assert record['completed_at'] is not None, "Expected completed_at timestamp"
    assert record['output_location'] is not None, "Expected output_location"

    # Step B4: Verify generated ML artifacts
    print("\n" + "=" * 80)
    print("GENERATED ML BASELINE ARTIFACTS VERIFICATION")
    print("=" * 80)
    metrics_path = os.path.join(REPO_ROOT, record['output_location'])
    print(f"Reading output metrics from: {metrics_path}")
    if os.path.isfile(metrics_path):
        with open(metrics_path, 'r') as f:
            metrics_data = json.load(f)
        print(json.dumps(metrics_data, indent=2))
    else:
        print(f"WARNING: Output file {metrics_path} does not exist!")

    # 4. Idempotency Replay Test
    print("\n" + "=" * 80)
    print("IDEMPOTENCY & DUPLICATE EVENT REPLAY VERIFICATION")
    print("=" * 80)
    print(f"Replaying already-triggered Event #{event_sellers['event_id']} ({event_sellers['dataset_name']})...")
    decision_dup, reason_dup = dep_engine.process_event(event_sellers, conn)
    print(f"Replay Result: Decision = {decision_dup} | Reason = {reason_dup}")

    cur.execute("SELECT COUNT(*) as count FROM pipeline_executions WHERE triggering_event_id = %s", (event_sellers['event_id'],))
    final_count = cur.fetchone()['count']
    print(f"Total execution records for Event #{event_sellers['event_id']}: {final_count} (Expected: 1)")
    assert final_count == 1, f"Expected exactly 1 execution record, found {final_count}"

    # 5. Dependency Cycle Reset Verification
    print("\n" + "=" * 80)
    print("DEPENDENCY CYCLE RESET VERIFICATION")
    print("=" * 80)
    cur.execute("SELECT dataset_name, status, current_version FROM dataset_metadata WHERE dataset_name IN ('Orders', 'Products', 'Sellers')")
    for row in cur.fetchall():
        print(f"  Dataset: {row['dataset_name']:<10} | Status: {row['status']:<10} | Version: {row['current_version']}")

    conn.close()
    print("\n" + "=" * 80)
    print("      PHASE 4 DEMO COMPLETED SUCCESSFULLY")
    print("=" * 80)


if __name__ == '__main__':
    run_demo()
