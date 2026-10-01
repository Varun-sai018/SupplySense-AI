"""
SupplySense AI - Phase 7 Demo Script: Debezium Change Data Capture (CDC) Pipeline

Demonstrates:
  1. Inspecting Debezium Connect REST API and MySQL CDC connector status.
  2. Ingesting change events from MySQL tables via Debezium CDC.
  3. CDC Adapter normalizing raw binlog events into SupplySense dataset events.
  4. Updating `dataset_metadata` and routing events to Kafka `dataset-events`.
  5. Dependency Engine evaluating the dependency rule (ALL condition -> TRIGGER).
  6. Automatic execution of the downstream XGBoost Demand Forecast Pipeline.
  7. Persisting and querying generated forecasts in MySQL `forecast_results`.
"""

import os
import sys
import importlib
import time
import json

# Ensure repository root is on sys.path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from common.database import get_connection

cdc_adapter = importlib.import_module('services.cdc-adapter.adapter')
register_conn = importlib.import_module('services.cdc-adapter.register_connector')
dep_engine = importlib.import_module('services.dependency-engine.main')
forecast_repo = importlib.import_module('services.pipeline-runner.forecast_repository')


def wait_for_pipeline_execution(triggering_event_id, conn, timeout=300, poll_interval=2):
    """
    Polls pipeline_executions table until the execution transitions to COMPLETED or FAILED,
    or until the timeout expires.
    """
    start_time = time.time()
    cur = conn.cursor()
    print(f"Waiting for pipeline execution (triggering_event_id={triggering_event_id}, timeout={timeout}s)...")

    last_status = None
    while time.time() - start_time < timeout:
        conn.commit()  # Refresh transaction snapshot for REPEATABLE READ
        cur.execute("""
            SELECT execution_id, pipeline_name, status, started_at, completed_at, output_location, error_message
            FROM pipeline_executions
            WHERE triggering_event_id = %s
            ORDER BY execution_id DESC
            LIMIT 1
        """, (triggering_event_id,))
        exec_record = cur.fetchone()

        if exec_record:
            status = exec_record.get('status')
            elapsed = int(time.time() - start_time)
            if status != last_status:
                print(f"  [{elapsed}s] Execution #{exec_record.get('execution_id')}: status={status}")
                last_status = status

            if status == 'COMPLETED':
                return exec_record
            elif status == 'FAILED':
                err = exec_record.get('error_message') or 'Unknown error'
                raise RuntimeError(f"Pipeline execution #{exec_record.get('execution_id')} FAILED: {err}")
        else:
            elapsed = int(time.time() - start_time)
            print(f"  [{elapsed}s] Waiting for execution record...")

        time.sleep(poll_interval)

    conn.commit()
    cur.execute("""
        SELECT execution_id, pipeline_name, status, started_at, completed_at, output_location, error_message
        FROM pipeline_executions
        WHERE triggering_event_id = %s
        ORDER BY execution_id DESC
        LIMIT 1
    """, (triggering_event_id,))
    final_record = cur.fetchone()
    exec_id = final_record.get('execution_id') if final_record else 'N/A'
    current_status = final_record.get('status') if final_record else 'NOT_FOUND'
    raise TimeoutError(
        f"Timed out after {timeout}s waiting for pipeline execution to complete. "
        f"Last known execution_id={exec_id}, status={current_status}"
    )


def run_phase7_demo():
    conn = get_connection()
    cur = conn.cursor()

    print("=" * 80)
    print("      SUPPLYSENSE AI - PHASE 7 DEBEZIUM CDC PIPELINE DEMO")
    print("=" * 80)

    # 1. Verify Debezium Connector Infrastructure
    print("\n[Step 1] Verifying Debezium MySQL CDC Connector Status...")
    try:
        register_conn.register_or_update_connector()
        is_running = register_conn.wait_for_connector_running(timeout=15)
        status = register_conn.get_connector_status()
        conn_state = status.get('connector', {}).get('state')
        tasks = status.get('tasks', [])
        print(f"  Connector Name : {status.get('name')}")
        print(f"  Connector State: {conn_state}")
        print(f"  Active Tasks   : {len(tasks)} ({tasks[0].get('state') if tasks else 'None'})")
        assert conn_state == "RUNNING", "Connector is not running"
    except Exception as e:
        print(f"  WARNING: Debezium connector check encountered: {e}")

    # 2. Reset Dependency Configuration
    print("\n[Step 2] Initializing Pipeline Dependencies (Condition: ALL)...")
    cur.execute("UPDATE dataset_metadata SET status = 'WAITING' WHERE dataset_name IN ('Orders', 'Products', 'Sellers')")
    cur.execute("UPDATE pipeline_dependencies SET condition_type = 'ALL', required_count = NULL WHERE pipeline_name = 'Demand Forecast Pipeline'")
    conn.commit()

    # 3. Simulate CDC Events for Orders, Products, Sellers
    print("\n" + "=" * 80)
    print("STEP 3: SIMULATING CDC EVENTS & ADAPTER NORMALIZATION")
    print("=" * 80)

    # 3a. Orders CDC Event
    print("\n3a. Ingesting CDC event for 'olist_orders'...")
    orders_cdc = cdc_adapter.normalize_cdc_record({
        "before": None,
        "after": {"order_id": f"demo_ord_{int(time.time())}", "order_status": "invoiced"},
        "source": {"table": "olist_orders", "db": "supplysense"},
        "op": "c",
        "ts_ms": int(time.time() * 1000)
    })
    res_orders = cdc_adapter.process_cdc_event(orders_cdc, connection=conn)
    print(f"    CDC Event #{res_orders['event_id']} (Dataset: {res_orders['dataset_name']}, v{res_orders['dataset_version']}) -> PUBLISHED")

    cur.execute("SELECT * FROM dataset_events WHERE event_id = %s", (res_orders["event_id"],))
    dec_1, reason_1 = dep_engine.process_event(cur.fetchone(), conn)
    print(f"    Dependency Decision: {dec_1} | Reason: {reason_1}")

    # 3b. Products CDC Event
    print("\n3b. Ingesting CDC event for 'olist_products'...")
    products_cdc = cdc_adapter.normalize_cdc_record({
        "before": None,
        "after": {"product_id": f"demo_prod_{int(time.time())}", "product_category_name": "cama_mesa_banho"},
        "source": {"table": "olist_products", "db": "supplysense"},
        "op": "c",
        "ts_ms": int(time.time() * 1000)
    })
    res_products = cdc_adapter.process_cdc_event(products_cdc, connection=conn)
    print(f"    CDC Event #{res_products['event_id']} (Dataset: {res_products['dataset_name']}, v{res_products['dataset_version']}) -> PUBLISHED")

    cur.execute("SELECT * FROM dataset_events WHERE event_id = %s", (res_products["event_id"],))
    dec_2, reason_2 = dep_engine.process_event(cur.fetchone(), conn)
    print(f"    Dependency Decision: {dec_2} | Reason: {reason_2}")

    # 3c. Sellers CDC Event -> TRIGGER
    print("\n3c. Ingesting CDC event for 'olist_sellers' (Satisfies ALL -> TRIGGER)...")
    sellers_cdc = cdc_adapter.normalize_cdc_record({
        "before": None,
        "after": {"seller_id": f"demo_sell_{int(time.time())}", "seller_city": "sao paulo"},
        "source": {"table": "olist_sellers", "db": "supplysense"},
        "op": "c",
        "ts_ms": int(time.time() * 1000)
    })
    res_sellers = cdc_adapter.process_cdc_event(sellers_cdc, connection=conn)
    print(f"    CDC Event #{res_sellers['event_id']} (Dataset: {res_sellers['dataset_name']}, v{res_sellers['dataset_version']}) -> PUBLISHED")

    cur.execute("SELECT * FROM dataset_events WHERE event_id = %s", (res_sellers["event_id"],))
    dec_3, reason_3 = dep_engine.process_event(cur.fetchone(), conn)
    print(f"    Dependency Decision: {dec_3} | Reason: {reason_3}")

    # 4. Verify Pipeline Execution Record (Poll until COMPLETED)
    print("\n" + "=" * 80)
    print("STEP 4: VERIFYING DOWNSTREAM PIPELINE EXECUTION LIFECYCLE")
    print("=" * 80)
    exec_record = wait_for_pipeline_execution(res_sellers['event_id'], conn, timeout=300, poll_interval=2)

    print("\nExecution Lifecycle Details:")
    for k, v in exec_record.items():
        print(f"  {k:<22}: {v}")

    execution_id = exec_record['execution_id']

    # 5. Query Forecast Results
    print("\n" + "=" * 80)
    print(f"STEP 5: QUERYING PERSISTED FORECASTS IN MYSQL (Execution ID #{execution_id})")
    print("=" * 80)
    summary = forecast_repo.get_forecast_summary_by_execution(execution_id, connection=conn)
    print("Forecast Aggregate Summary:")
    print(f"  Total Predictions      : {summary.get('total_forecasts')}")
    print(f"  Distinct Categories    : {summary.get('distinct_categories')}")
    print(f"  Forecast Horizon       : {summary.get('min_week')} to {summary.get('max_week')}")
    print(f"  Average Demand         : {float(summary.get('avg_predicted_demand', 0)):.4f}")
    print(f"  Model Metadata         : {summary.get('model_name')} ({summary.get('model_version')})")

    # Sample Category
    print("\nSample Forecasts ('cama_mesa_banho'):")
    sample_rows = forecast_repo.get_forecasts_by_category("cama_mesa_banho", limit=3, connection=conn)
    for r in sample_rows:
        print(f"  Week: {r['forecast_week']} | Category: {r['product_category']:<18} | Predicted: {float(r['predicted_demand']):.4f} | Model: {r['model_name']} {r['model_version']}")

    conn.close()
    print("\n" + "=" * 80)
    print("      PHASE 7 DEMO COMPLETED SUCCESSFULLY")
    print("=" * 80)


if __name__ == '__main__':
    run_phase7_demo()
